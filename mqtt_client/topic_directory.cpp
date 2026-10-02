#include "topic_directory.hpp"
#include "topic_filter.hpp"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <exception>
#include <stdexcept>
#include <utility>

namespace mqttx {
namespace {

/// 当前线程正在执行的 handler；用于识别“在回调里摘除自己”的场景，避免自等死锁。
thread_local const void* t_active_slot = nullptr;

/// 等到计数器归零。
/// 计数器只可能被本线程之外的操作减少（见 remove() 的说明），因此这里不会自等。
template <typename Counter>
void wait_until_zero(const Counter& counter) {
    int n = counter.load(std::memory_order_acquire);
    while (n != 0) {
        counter.wait(n, std::memory_order_acquire);
        n = counter.load(std::memory_order_acquire);
    }
}

}  // namespace

/// 每个 handler 调用一份的保活凭据（定义见头文件中的前置声明）。
///
/// - 持有 SlotPtr：即便登记项已被摘除，凭据析构时访问的 Slot 依然有效；
/// - 持有调用方给的 KeepAlive（通常是 paho 消息）：让 MessageView 的借用视图
///   在异步任务里继续有效；
/// - 构造即 +1、析构即 -1（并唤醒等待中的 remove()）：计数与凭据本身同寿，
///   因此 handler 把视图捕获到线程池后，remove() 会一直等到任务释放它为止。
struct TopicDirectory::Lease {
    SlotPtr   slot;
    KeepAlive source;

    Lease(SlotPtr s, KeepAlive src) : slot{std::move(s)}, source{std::move(src)} {
        slot->async_refs.fetch_add(1, std::memory_order_acq_rel);
    }
    ~Lease() {
        if (slot->async_refs.fetch_sub(1, std::memory_order_acq_rel) == 1)
            slot->async_refs.notify_all();
    }

    Lease(const Lease&)            = delete;
    Lease& operator=(const Lease&) = delete;
};

// ---------------------------------------------------------------------------
// add
// ---------------------------------------------------------------------------

TopicDirectory::InstallResult
TopicDirectory::add(std::string_view topic, void* obj, InvokeFn fn, int qos) {
    if (topic.empty()) throw std::invalid_argument("mqttx::TopicDirectory: topic 不能为空");
    if (fn == nullptr) throw std::invalid_argument("mqttx::TopicDirectory: handler 不能为空");

    auto slot    = std::make_shared<Slot>();
    slot->slot   = next_slot_.fetch_add(1, std::memory_order_relaxed);
    slot->obj.store(obj, std::memory_order_relaxed);
    slot->fn = fn;

    // 先取出 id：下面的 insert_or_assign 会移动 slot，不能依赖实参求值顺序
    const std::uint64_t id = slot->slot;

    InstallResult res;
    res.slot = id;

    const std::unique_lock lk{mtx_};
    auto [it, inserted] = dir_.try_emplace(std::string{topic});
    Entry& entry        = it->second;

    if (inserted) {
        entry.qos     = qos;
        res.subscribe = true;  // 新 topic：必须 SUBSCRIBE
        if (is_wildcard_filter(it->first)) wild_filters_.push_back(it->first);
    }
    else if (qos > entry.qos) {
        entry.qos     = qos;   // QoS 提升：需要按更高 QoS 重新 SUBSCRIBE
        res.subscribe = true;
    }

    entry.handlers.push_back(slot);
    res.qos = entry.qos;

    by_slot_.insert_or_assign(id, Record{it->first, std::move(slot)});
    return res;
}

// ---------------------------------------------------------------------------
// remove
// ---------------------------------------------------------------------------

TopicDirectory::RemoveResult TopicDirectory::remove(std::uint64_t slot) {
    RemoveResult res;
    SlotPtr      target;

    {
        const std::unique_lock lk{mtx_};
        auto rec = by_slot_.find(slot);
        if (rec == by_slot_.end()) return res;  // 未知或已摘除：幂等

        target    = std::move(rec->second.slot);
        res.topic = std::move(rec->second.topic);
        by_slot_.erase(rec);

        // 先失效：同一批 dispatch 中尚未执行的该 handler 不会再被调用
        target->alive.store(false, std::memory_order_release);

        auto it = dir_.find(res.topic);
        if (it != dir_.end()) {
            auto& handlers = it->second.handlers;
            std::erase_if(handlers, [&](const SlotPtr& p) { return p.get() == target.get(); });
            res.removed = true;
            if (handlers.empty()) {
                dir_.erase(it);
                res.unsubscribe = true;  // 该 topic 已无 handler：UNSUBSCRIBE
                std::erase(wild_filters_, res.topic);
            }
        }
    }

    // 等待“在飞调用”和“外部保活凭据”全部释放，之后 handler 的目标对象即可安全析构。
    //
    // 两段等待的顺序不能颠倒：
    // async_refs 的自增发生在 dispatch 仍持有该 slot 的 inflight 期间，因此只要
    // inflight 归零，本 slot 的凭据就已全部创建完毕，随后观察 async_refs 不会漏。
    // 另外，登记项已在本函数开头（持锁）被摘除，新的派发批次不可能再包含它。
    //
    // 若当前线程正跑在该 handler 内（回调里析构自己），不能自等。
    if (target != nullptr && t_active_slot != target.get()) {
        wait_until_zero(target->inflight);   // 同步调用帧
        wait_until_zero(target->async_refs); // 异步任务持有的消息/凭据副本
    }
    return res;
}

// ---------------------------------------------------------------------------
// rebind
// ---------------------------------------------------------------------------

bool TopicDirectory::rebind(std::uint64_t slot, void* obj) noexcept {
    try {
        const std::shared_lock lk{mtx_};
        auto                 it = by_slot_.find(slot);
        if (it == by_slot_.end()) return false;
        it->second.slot->obj.store(obj, std::memory_order_release);
        return true;
    }
    catch (...) {
        return false;  // noexcept：理论上不会走到这里
    }
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

std::size_t TopicDirectory::dispatch(std::string_view topic, const MessageView& msg) const 
{
    std::vector<SlotPtr> batch;

    {
        const std::shared_lock lk{mtx_};

        // 把某个 filter 下的 handler 收进本批；alive 检查与计数自增都在锁内完成，
        // 因此 remove() 只要看到 inflight == 0 就一定能安全返回。
        const auto collect = [&batch](const Entry& entry) {
            batch.reserve(batch.size() + entry.handlers.size());
            for (const auto& h : entry.handlers) {
                if (!h->alive.load(std::memory_order_acquire)) continue;
                h->inflight.fetch_add(1, std::memory_order_acq_rel);
                batch.push_back(h);
            }
        };

        // 1) 精确命中（最常见路径，无额外分配）
        if (auto it = dir_.find(topic); it != dir_.end()) collect(it->second);

        // 2) 含通配符的 filter（'+' / '#'）——按 MQTT 规则逐个匹配
        for (const auto& filter : wild_filters_) {
            if (filter == topic) continue;  // 已在精确路径处理
            if (!topic_matches_filter(topic, filter)) continue;
            if (auto it = dir_.find(filter); it != dir_.end()) collect(it->second);
        }

        if (batch.empty()) return 0;
    }

    std::size_t invoked = 0;
    std::size_t next    = 0;  // 下一个尚未移交给逐项 Guard 的元素

    // 兜底：任何异常退出（例如凭据分配失败）都要归还本批剩余的 inflight 计数，
    // 否则这些 slot 的 remove() 会永远等不到 0。
    struct BatchGuard {
        const std::vector<SlotPtr>& batch;
        std::size_t&                next;
        ~BatchGuard() {
            for (std::size_t i = next; i < batch.size(); ++i) {
                Slot& s = *batch[i];
                if (s.inflight.fetch_sub(1, std::memory_order_acq_rel) == 1)
                    s.inflight.notify_all();
            }
        }
    } batch_guard{batch, next};

    for (std::size_t i = 0; i < batch.size(); ++i) {
        const SlotPtr& h = batch[i];

        // RAII：无论 handler 是否抛异常，都要恢复 t_active_slot 并归还在飞计数
        struct Guard {
            Slot*       slot;
            const void* prev;
            ~Guard() {
                t_active_slot = prev;
                if (slot->inflight.fetch_sub(1, std::memory_order_acq_rel) == 1)
                    slot->inflight.notify_all();
            }
        } guard{h.get(), t_active_slot};
        next = i + 1;  // 本元素的计数已由 guard 负责

        if (!h->alive.load(std::memory_order_acquire)) continue;  // 同批内已被摘除

        void* obj = h->obj.load(std::memory_order_acquire);
        if (obj == nullptr || h->fn == nullptr) continue;

        // 为本次调用准备带保活凭据的视图副本：
        //   - 同步 handler 用完即释放；
        //   - handler 若把副本投递到线程池，凭据会随副本存活，remove() 会一直等到
        //     它释放为止（消息数据也因此保持有效）。
        MessageView call = msg;
        const auto  lease = std::make_shared<Lease>(h, msg.keep_alive);  // 构造即计数 +1
        call.keep_alive   = KeepAlive{std::shared_ptr<const void>{lease}};

        t_active_slot = h.get();
        ++invoked;
        try {
            h->fn(obj, call);  // 不持有目录锁执行用户代码
        }
        catch (const std::exception& e) {
            spdlog::error("[mqttx] topic={} 的 handler 抛出异常: {}", topic, e.what());
        }
        catch (...) {
            spdlog::error("[mqttx] topic={} 的 handler 抛出未知异常", topic);
        }
    }
    return invoked;
}

// ---------------------------------------------------------------------------
// 查询
// ---------------------------------------------------------------------------

std::vector<TopicDirectory::Subscription> TopicDirectory::subscriptions() const {
    const std::shared_lock lk{mtx_};
    std::vector<Subscription> out;
    out.reserve(dir_.size());
    for (const auto& [topic, entry] : dir_) out.push_back(Subscription{topic, entry.qos});
    return out;
}

std::size_t TopicDirectory::topic_count() const {
    const std::shared_lock lk{mtx_};
    return dir_.size();
}

std::size_t TopicDirectory::handler_count() const {
    const std::shared_lock lk{mtx_};
    std::size_t            n = 0;
    for (const auto& [topic, entry] : dir_) n += entry.handlers.size();
    return n;
}

bool TopicDirectory::empty() const {
    const std::shared_lock lk{mtx_};
    return dir_.empty();
}

bool TopicDirectory::contains(std::string_view topic) const {
    const std::shared_lock lk{mtx_};
    return dir_.find(topic) != dir_.end();
}

}  // namespace mqttx

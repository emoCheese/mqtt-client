// ---------------------------------------------------------------------------
// TopicDirectory 单元测试：多播、引用计数、QoS、生命周期安全、并发
// ---------------------------------------------------------------------------

#include "mini_test.hpp"
#include "topic_directory.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace mqttx;

namespace {

MessageView make_view(std::string_view topic, std::string_view payload, int qos = 1) {
    return MessageView{.topic = topic, .payload = payload, .props = nullptr, .qos = qos, .retain = false, .keep_alive = {}};
}

struct Sink {
    std::atomic<int> count{0};
    std::string      last_topic;
    std::string      last_payload;
};

void sink_fn(void* obj, const MessageView& msg) {
    auto* s = static_cast<Sink*>(obj);
    s->count.fetch_add(1, std::memory_order_relaxed);
    s->last_topic.assign(msg.topic);
    s->last_payload.assign(msg.payload);
}

void throwing_fn(void*, const MessageView&) { throw std::runtime_error("handler boom"); }

/// 模拟“在回调里析构自己”：handler 内部摘除自己的 slot
struct SelfRemover {
    TopicDirectory* dir{nullptr};
    std::uint64_t   slot{0};
    int             calls{0};
};

void self_remove_fn(void* obj, const MessageView&) {
    auto* s = static_cast<SelfRemover*>(obj);
    ++s->calls;
    s->dir->remove(s->slot);
}

}  // namespace

// ---------------------------------------------------------------------------

TEST(directory_add_new_topic_requests_subscribe) {
    TopicDirectory dir;
    Sink           a;

    const auto res = dir.add("robot/1/cmd", &a, sink_fn, 1);

    CHECK(res.slot != 0);
    CHECK(res.subscribe);
    CHECK_EQ(res.qos, 1);
    CHECK_EQ(dir.topic_count(), std::size_t{1});
    CHECK_EQ(dir.handler_count(), std::size_t{1});
    CHECK(dir.contains("robot/1/cmd"));
    CHECK_FALSE(dir.contains("robot/2/cmd"));
}

TEST(directory_add_empty_topic_or_null_handler_throws) {
    TopicDirectory dir;
    Sink           a;

    CHECK_THROWS_AS(dir.add("", &a, sink_fn, 1), std::invalid_argument);
    CHECK_THROWS_AS(dir.add("t", &a, nullptr, 1), std::invalid_argument);
}

TEST(directory_dispatch_reaches_handler) {
    TopicDirectory dir;
    Sink           a;

    const auto res = dir.add("robot/1/cmd", &a, sink_fn, 1);
    const auto n   = dir.dispatch("robot/1/cmd", make_view("robot/1/cmd", "hello"));

    CHECK_EQ(n, std::size_t{1});
    CHECK_EQ(a.count.load(), 1);
    CHECK_EQ(a.last_payload, std::string{"hello"});
    CHECK_EQ(a.last_topic, std::string{"robot/1/cmd"});
    CHECK(res.slot != 0);
}

TEST(directory_dispatch_unknown_topic_is_noop) {
    TopicDirectory dir;
    CHECK_EQ(dir.dispatch("nobody", make_view("nobody", "x")), std::size_t{0});
}

TEST(directory_multicast_same_topic) {
    TopicDirectory dir;
    Sink           a;
    Sink           b;

    const auto sa = dir.add("evt", &a, sink_fn, 1);
    const auto sb = dir.add("evt", &b, sink_fn, 1);

    CHECK_FALSE(sb.subscribe);  // 已有订阅者：无需重复 SUBSCRIBE
    CHECK_EQ(dir.topic_count(), std::size_t{1});
    CHECK_EQ(dir.handler_count(), std::size_t{2});

    CHECK_EQ(dir.dispatch("evt", make_view("evt", "m1")), std::size_t{2});
    CHECK_EQ(a.count.load(), 1);
    CHECK_EQ(b.count.load(), 1);

    // 摘除第一个：topic 仍在，另一个照常收消息
    const auto ra = dir.remove(sa.slot);
    CHECK(ra.removed);
    CHECK_FALSE(ra.unsubscribe);
    CHECK_EQ(ra.topic, std::string{"evt"});
    CHECK_EQ(dir.dispatch("evt", make_view("evt", "m2")), std::size_t{1});
    CHECK_EQ(a.count.load(), 1);
    CHECK_EQ(b.count.load(), 2);

    // 摘除最后一个：需要 UNSUBSCRIBE
    const auto rb = dir.remove(sb.slot);
    CHECK(rb.removed);
    CHECK(rb.unsubscribe);
    CHECK(dir.empty());
    CHECK_EQ(dir.dispatch("evt", make_view("evt", "m3")), std::size_t{0});
    CHECK_EQ(b.count.load(), 2);
}

TEST(directory_remove_unknown_slot_is_noop) {
    TopicDirectory dir;
    const auto     r = dir.remove(12345);
    CHECK_FALSE(r.removed);
    CHECK_FALSE(r.unsubscribe);

    Sink       a;
    const auto s = dir.add("t", &a, sink_fn, 1);
    dir.remove(s.slot);
    CHECK_FALSE(dir.remove(s.slot).removed);  // 幂等
}

TEST(directory_qos_is_max_of_handlers) {
    TopicDirectory dir;
    Sink           a;
    Sink           b;

    CHECK_EQ(dir.add("t", &a, sink_fn, 0).qos, 0);
    const auto hi = dir.add("t", &b, sink_fn, 2);
    CHECK_EQ(hi.qos, 2);
    CHECK(hi.subscribe);  // QoS 提升 → 需要按更高 QoS 重新 SUBSCRIBE

    Sink       c;
    const auto lo = dir.add("t", &c, sink_fn, 1);
    CHECK_EQ(lo.qos, 2);
    CHECK_FALSE(lo.subscribe);  // QoS 未提升 → 不必重订阅

    const auto subs = dir.subscriptions();
    CHECK_EQ(subs.size(), std::size_t{1});
    CHECK_EQ(subs.at(0).topic, std::string{"t"});
    CHECK_EQ(subs.at(0).qos, 2);
}

TEST(directory_rebind_updates_target) {
    TopicDirectory dir;
    Sink           a;
    Sink           b;

    const auto s = dir.add("t", &a, sink_fn, 1);
    CHECK(dir.rebind(s.slot, &b));
    CHECK_EQ(dir.dispatch("t", make_view("t", "x")), std::size_t{1});
    CHECK_EQ(a.count.load(), 0);
    CHECK_EQ(b.count.load(), 1);

    CHECK_FALSE(dir.rebind(9999, &a));  // 未知 slot
}

TEST(directory_handler_exception_is_isolated) {
    TopicDirectory dir;
    Sink           good;
    int            dummy = 0;

    dir.add("t", &dummy, throwing_fn, 1);
    dir.add("t", &good, sink_fn, 1);

    // 抛异常的 handler 不影响同批其它 handler，也不让异常逃出 dispatch
    CHECK_EQ(dir.dispatch("t", make_view("t", "x")), std::size_t{2});
    CHECK_EQ(good.count.load(), 1);
}

TEST(directory_self_removal_inside_handler_does_not_deadlock) {
    TopicDirectory dir;
    SelfRemover    remover;

    remover.dir          = &dir;
    const auto res       = dir.add("t", &remover, self_remove_fn, 1);
    remover.slot         = res.slot;

    CHECK_EQ(dir.dispatch("t", make_view("t", "x")), std::size_t{1});
    CHECK_EQ(remover.calls, 1);
    CHECK(dir.empty());  // 回调内已把自己摘掉

    // 后续消息不会再进入
    CHECK_EQ(dir.dispatch("t", make_view("t", "y")), std::size_t{0});
    CHECK_EQ(remover.calls, 1);
}

TEST(directory_remove_waits_for_inflight_dispatch) {
    TopicDirectory dir;
    Sink           a;

    std::atomic<bool> in_handler{false};
    std::atomic<bool> release{false};
    std::atomic<int>  finished{0};

    struct Slow {
        std::atomic<bool>* in_handler;
        std::atomic<bool>* release;
        std::atomic<int>*  finished;
    } slow{&in_handler, &release, &finished};

    const auto slow_fn = [](void* obj, const MessageView&) {
        auto* s = static_cast<Slow*>(obj);
        s->in_handler->store(true);
        while (!s->release->load()) std::this_thread::yield();
        s->finished->fetch_add(1);
    };

    const auto slot = dir.add("t", &slow, +slow_fn, 1);

    std::thread dispatching([&] { dir.dispatch("t", make_view("t", "x")); });
    while (!in_handler.load()) std::this_thread::yield();

    // remove 必须等在飞回调结束才返回（否则目标对象可能被析构）
    std::thread remover([&] { dir.remove(slot.slot); });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    CHECK_EQ(finished.load(), 0);

    release.store(true);
    remover.join();
    CHECK_EQ(finished.load(), 1);
    dispatching.join();

    CHECK_EQ(a.count.load(), 0);  // 从未被调用
    CHECK(dir.empty());
}

TEST(directory_routes_by_topic_filter) {
    TopicDirectory dir;
    Sink           exact;
    Sink           plus;
    Sink           hash;
    Sink           other;

    dir.add("robot/1/cmd", &exact, sink_fn, 1);
    dir.add("robot/+/cmd", &plus, sink_fn, 1);
    dir.add("robot/#", &hash, sink_fn, 1);
    dir.add("robot/2/cmd", &other, sink_fn, 1);

    // 具体 topic 命中：精确 + 'robot/+/cmd' + 'robot/#'
    CHECK_EQ(dir.dispatch("robot/1/cmd", make_view("robot/1/cmd", "go")), std::size_t{3});
    CHECK_EQ(exact.count.load(), 1);
    CHECK_EQ(plus.count.load(), 1);
    CHECK_EQ(hash.count.load(), 1);
    CHECK_EQ(other.count.load(), 0);

    // 只命中通配：'robot/+/cmd' 与 'robot/#'
    CHECK_EQ(dir.dispatch("robot/9/cmd", make_view("robot/9/cmd", "go")), std::size_t{2});
    CHECK_EQ(plus.count.load(), 2);
    CHECK_EQ(hash.count.load(), 2);
    CHECK_EQ(other.count.load(), 0);

    // 只命中 'robot/#'（'+' 只匹配一级，这里有三层）
    CHECK_EQ(dir.dispatch("robot/9/deep/cmd", make_view("robot/9/deep/cmd", "go")), std::size_t{1});
    CHECK_EQ(hash.count.load(), 3);

    // 不命中任何 filter
    CHECK_EQ(dir.dispatch("sensor/1", make_view("sensor/1", "x")), std::size_t{0});

    // 同一 filter 上的多播 + 摘除后不再命中
    const auto extra = dir.add("robot/#", &hash, sink_fn, 1);
    CHECK_EQ(dir.handler_count(), std::size_t{5});
    CHECK_EQ(dir.dispatch("robot/9/deep/cmd", make_view("robot/9/deep/cmd", "go")), std::size_t{2});
    CHECK_EQ(hash.count.load(), 5);

    dir.remove(extra.slot);
    CHECK_EQ(dir.dispatch("robot/9/deep/cmd", make_view("robot/9/deep/cmd", "go")), std::size_t{1});
    CHECK_EQ(hash.count.load(), 6);
}

TEST(directory_remove_waits_for_async_view_copy) {
    // 模拟“handler 把消息投递到线程池”：调用帧立刻返回，但异步侧仍持有 MessageView 副本
    struct AsyncSink {
        MessageView       view;  // 异步任务持有的副本（含保活凭据）
        std::atomic<bool> started{false};
    } async;

    const auto post_to_pool = [](void* obj, const MessageView& m) {
        auto* s = static_cast<AsyncSink*>(obj);
        s->view = m;  // 按值捕获：凭据随副本一起存活
        s->started.store(true, std::memory_order_release);
    };

    TopicDirectory dir;
    const auto     slot = dir.add("t/async", &async, +post_to_pool, 1);

    CHECK_EQ(dir.dispatch("t/async", make_view("t/async", "payload")), std::size_t{1});
    CHECK(async.started.load());
    CHECK(async.view.keep_alive.valid());  // 副本拿到了有效凭据
    CHECK_EQ(async.view.payload, std::string_view{"payload"});

    // 调用帧已返回，但异步副本还在飞：remove 必须继续等待
    std::atomic<bool> removed{false};
    std::thread       remover([&] {
        dir.remove(slot.slot);
        removed.store(true, std::memory_order_release);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    CHECK_FALSE(removed.load());  // 仍在等凭据释放

    async.view = {};  // 异步任务完成，释放副本
    remover.join();
    CHECK(removed.load());
    CHECK(dir.empty());
}

TEST(directory_async_view_keeps_message_data_alive) {
    // 凭据除了保护登记项，还持有底层消息：异步侧读到的数据必须仍然有效
    struct AsyncSink {
        MessageView view;
    } async;

    const auto keep = [](void* obj, const MessageView& m) { static_cast<AsyncSink*>(obj)->view = m; };

    TopicDirectory dir;
    const auto     slot = dir.add("t/keep", &async, +keep, 1);

    // 模拟 paho 消息：数据由共享对象持有，视图只是借用
    auto                   source = std::make_shared<const std::string>("binary-payload");
    const MessageView      msg{.topic      = "t/keep",
                               .payload    = std::string_view{*source},
                               .props      = nullptr,
                               .qos        = 1,
                               .retain     = false,
                               .keep_alive = KeepAlive{std::shared_ptr<const void>{source}}};
    CHECK_EQ(dir.dispatch("t/keep", msg), std::size_t{1});

    source.reset();  // 调用方释放：只剩异步副本的凭据持有它

    CHECK_EQ(async.view.payload, std::string_view{"binary-payload"});  // 数据仍有效
    CHECK(async.view.keep_alive.valid());

    async.view = {};
    dir.remove(slot.slot);
}

TEST(directory_concurrent_add_remove_dispatch) {
    TopicDirectory dir;
    Sink           resident;
    Sink           churn;

    const auto keep = dir.add("t/a", &resident, sink_fn, 1);
    CHECK(keep.slot != 0);

    std::atomic<bool> stop{false};
    std::atomic<long> dispatched{0};

    std::thread consumer([&] {
        while (!stop.load(std::memory_order_relaxed))
            dispatched.fetch_add(static_cast<long>(dir.dispatch("t/a", make_view("t/a", "x"))),
                                 std::memory_order_relaxed);
    });

    std::thread churner([&] {
        for (int i = 0; i < 300; ++i) {
            const auto s = dir.add("t/a", &churn, sink_fn, 1);
            std::this_thread::yield();
            dir.remove(s.slot);
        }
    });

    churner.join();
    stop.store(true, std::memory_order_relaxed);
    consumer.join();

    CHECK(dispatched.load() > 0);
    CHECK(dir.contains("t/a"));
    CHECK_EQ(dir.handler_count(), std::size_t{1});  // 只剩常驻 handler
}

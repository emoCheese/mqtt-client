#pragma once

// ---------------------------------------------------------------------------
// TopicDirectory —— mqtt_client 的“topic 目录”
//
// 该库以 topic 为核心：所有能力都围绕 “一个 topic filter → 若干 handler” 展开。
// 目录负责：
//   1. 注册 / 摘除 handler，并按 topic 引用计数推导 SUBSCRIBE / UNSUBSCRIBE；
//   2. 同一 topic 支持多个 handler（多播），QoS 取所有 handler 的最大值；
//   3. 派发按 MQTT filter 规则匹配（支持 '+' / '#'），具体 topic 会命中
//      所有与之匹配的 filter；
//   4. 回调期间的生命周期安全：
//        - dispatch 不持有目录锁调用用户代码，避免用户回调内再注册造成死锁；
//        - handler 登记项用 shared_ptr 持有，摘除时先置 alive=false（同批后续
//          handler 不再进入），再等待“在飞调用”返回，保证目标对象析构后不会
//          再被触碰；
//        - 每次 handler 调用都附带一份“保活凭据”（MessageView::keep_alive）：
//          handler 即便把消息投递到线程池、调用帧立即返回，凭据仍然有效；
//          remove() 会继续等待这些凭据全部释放，因此“异步还在飞”时目标对象
//          不会被销毁，消息数据也保持有效；
//        - 若在 handler 内部摘除自己（自析构场景），不会自等死锁。
// ---------------------------------------------------------------------------

#include "message.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace mqttx {

/// 透明哈希：允许用 string_view / const char* 直接查询 string 键，避免临时构造
struct SvHash {
    using is_transparent = void;

    [[nodiscard]] std::size_t operator()(std::string_view s) const noexcept {
        return std::hash<std::string_view>{}(s);
    }
};

/// 透明相等：与 SvHash 配套
struct SvEqual {
    using is_transparent = void;

    [[nodiscard]] bool operator()(std::string_view a, std::string_view b) const noexcept {
        return a == b;
    }
};

/// 以 topic 为核心的回调目录（线程安全）
class TopicDirectory {
public:
    /// handler 的统一调用形式（编译期生成的 thunk，零额外分配）
    using InvokeFn = void (*)(void* obj, const MessageView& msg);

    /// add() 的结果：调用方可据此决定是否发送 SUBSCRIBE
    struct InstallResult {
        std::uint64_t slot{0};       ///< 本次注册的句柄，交给 remove()
        bool          subscribe{false};  ///< true = 需要（重新）SUBSCRIBE
        int           qos{0};        ///< 该 topic 当前生效的 QoS
    };

    /// remove() 的结果：调用方可据此决定是否发送 UNSUBSCRIBE
    struct RemoveResult {
        bool        removed{false};      ///< 是否真的摘除了一个登记项
        bool        unsubscribe{false};  ///< true = 该 topic 已无 handler，需 UNSUBSCRIBE
        std::string topic;               ///< 被摘除登记项所属的 topic
    };

    /// 供 subscribe_all() 使用的快照项
    struct Subscription {
        std::string topic;
        int         qos{0};
    };

    TopicDirectory()                                   = default;
    TopicDirectory(const TopicDirectory&)              = delete;
    TopicDirectory& operator=(const TopicDirectory&)   = delete;
    TopicDirectory(TopicDirectory&&)                   = delete;
    TopicDirectory& operator=(TopicDirectory&&)        = delete;
    ~TopicDirectory()                                  = default;

    /// 注册一个 handler。topic 为 topic filter（可含 '+' / '#'），
    /// qos 为已解析好的有效 QoS（不接受负值）。
    /// @throw std::invalid_argument topic 为空或 fn 为空
    InstallResult add(std::string_view topic, void* obj, InvokeFn fn, int qos);

    /// 摘除 slot 对应的 handler；必要时阻塞，直到
    ///   1) 该 handler 的在飞调用返回，且
    ///   2) handler 派生出去的异步任务不再持有保活凭据（MessageView::keep_alive）。
    /// @note 幂等：未知 slot 返回 removed=false。
    RemoveResult remove(std::uint64_t slot);

    /// handler 对象被移动后更新其指针；成功返回 true。
    bool rebind(std::uint64_t slot, void* obj) noexcept;

    /// 把消息派发给所有匹配 topic 的 handler，返回实际调用的 handler 数。
    ///
    /// 每个 handler 收到的是**带保活凭据**的 MessageView 副本：
    ///   - 同步 handler 直接使用即可；
    ///   - 异步 handler（线程池等）按值捕获该视图即可 —— 捕获期间消息数据仍然
    ///     有效，且 remove()（Topic::close()/析构）会等待这些捕获释放后才回收
    ///     目标对象，不会出现“调用已返回、异步还在飞”的悬垂。
    ///
    /// handler 抛出的异常会被就地吞掉并记录，不影响同批其它 handler。
    std::size_t dispatch(std::string_view topic, const MessageView& msg) const;

    /// 当前需要订阅的 (topic filter, qos) 快照
    [[nodiscard]] std::vector<Subscription> subscriptions() const;

    [[nodiscard]] std::size_t topic_count() const;
    [[nodiscard]] std::size_t handler_count() const;
    [[nodiscard]] bool        empty() const;

    /// 某个 topic filter 是否已注册（精确匹配，不做通配符展开）
    [[nodiscard]] bool contains(std::string_view topic) const;

private:
    /// 一次注册（一个 Topic 对象）对应的登记项
    struct Slot {
        std::uint64_t      slot{0};
        std::atomic<void*> obj{nullptr};
        InvokeFn           fn{nullptr};
        std::atomic<bool>  alive{true};
        std::atomic<int>   inflight{0};    ///< 正在执行该 handler 调用的线程数
        std::atomic<int>   async_refs{0};  ///< 外部仍持有的保活凭据份数
    };
    using SlotPtr = std::shared_ptr<Slot>;

    /// 随 MessageView 交给 handler 的保活凭据（定义在 .cpp）
    struct Lease;

    struct Entry {
        int                  qos{0};
        std::vector<SlotPtr> handlers;
    };

    struct Record {
        std::string topic;
        SlotPtr     slot;
    };

    using DirMap  = std::unordered_map<std::string, Entry, SvHash, SvEqual>;
    using SlotMap = std::unordered_map<std::uint64_t, Record>;

    mutable std::shared_mutex mtx_;
    DirMap                    dir_;           ///< topic filter → Entry
    std::vector<std::string>  wild_filters_;  ///< 含 '+' / '#' 的 filter（派发时需要扫描）
    SlotMap                   by_slot_;
    std::atomic<std::uint64_t> next_slot_{1};
};

}  // namespace mqttx

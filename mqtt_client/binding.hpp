#pragma once

// ---------------------------------------------------------------------------
// Binding / Subscription —— 绑定描述与激活凭据分离
//
//   auto b = mqttx::Binding{"robot/+/cmd", mqttx::on<&Svc::on_cmd>(svc), 1};
//   auto s = b.open(cli);     // 注册；s 析构即退订
//
// Binding 是纯值：只描述 (filter, handler, qos)，构造期完成校验，不产生副作用，
// 可在 Session 构造期安全创建，也可复用（同一 handler 绑定多个 filter）。
//
// Subscription 是激活凭据：只移动，析构/close() 退订。它不持有 Handler，
// 也不参与目录里的目标指针；move 只是 slot 搬运，不存在 rebind。
// ---------------------------------------------------------------------------

#include "handler.hpp"
#include "mqtt_client.hpp"

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace mqttx {

class Subscription;

class Binding {
public:
    /// @throw std::invalid_argument filter 为空 / handler 无效 / qos 越界
    Binding(std::string filter, Handler handler, int qos = -1);

    /// 注册到客户端；返回 RAII 凭据。可多次调用，每次产生独立注册。
    [[nodiscard]] Subscription open(MqttClient& cli) const;

    [[nodiscard]] const std::string& filter() const noexcept { return filter_; }
    [[nodiscard]] int                qos() const noexcept { return qos_; }

    /// 只在未激活时修改；已激活请用 Subscription::set_qos（目录级更新，不重订）
    void set_qos(int qos);

private:
    std::string filter_;
    Handler     handler_;
    int         qos_{-1};
};

class Subscription {
public:
    Subscription(Subscription&& o) noexcept
        : cli_{std::exchange(o.cli_, nullptr)}
        , slot_{std::exchange(o.slot_, 0)}
        , filter_{std::move(o.filter_)}
        , qos_{o.qos_} {}

    Subscription& operator=(Subscription&& o) noexcept {
        if (this != &o) {
            close();
            cli_    = std::exchange(o.cli_, nullptr);
            slot_   = std::exchange(o.slot_, 0);
            filter_ = std::move(o.filter_);
            qos_    = o.qos_;
        }
        return *this;
    }

    Subscription(const Subscription&)            = delete;
    Subscription& operator=(const Subscription&) = delete;

    ~Subscription() { close(); }

    /// 幂等；最后一个绑定者会让目录发送 UNSUBSCRIBE。
    void close() noexcept {
        if (slot_ == 0 || cli_ == nullptr) return;
        cli_->uninstall(slot_);
        slot_ = 0;
    }

    [[nodiscard]] bool active() const noexcept { return slot_ != 0; }
    [[nodiscard]] std::string_view filter() const noexcept { return filter_; }

    /// 请求的 QoS（-1 表示使用客户端默认值）
    [[nodiscard]] int qos() const noexcept { return qos_; }

    /// 目录中该 handler 实际生效的 QoS；未知/已关闭返回 -1。
    [[nodiscard]] int effective_qos() const { return active() ? cli_->effective_qos(slot_) : -1; }

    /// 目录级更新 QoS：slot 不变，仅按新的最大 QoS 重新 SUBSCRIBE。
    /// @throw std::invalid_argument qos 越界；std::logic_error 未激活
    /// @return false 表示 slot 已失效（并发 close）
    bool set_qos(int qos) {
        if (qos < -1 || qos > 2) throw std::invalid_argument("mqttx::Subscription: qos 必须在 [-1, 2]");
        if (!active()) throw std::logic_error("mqttx::Subscription: 未激活");
        if (qos == qos_) return true;
        if (!cli_->set_qos(slot_, qos)) return false;
        qos_ = qos;
        return true;
    }

private:
    friend class Binding;

    Subscription(MqttClient& cli, std::string filter, int qos, std::uint64_t slot)
        : cli_{&cli}, slot_{slot}, filter_{std::move(filter)}, qos_{qos} {}

    MqttClient*   cli_{nullptr};
    std::uint64_t slot_{0};
    std::string   filter_;
    int           qos_{-1};
};

// ---------------------------------------------------------------------------
// Binding 实现
// ---------------------------------------------------------------------------

inline Binding::Binding(std::string filter, Handler handler, int qos) {
    if (filter.empty()) throw std::invalid_argument("mqttx::Binding: topic filter 不能为空");
    if (!handler.valid()) throw std::invalid_argument("mqttx::Binding: handler 不能为空");
    if (qos < -1 || qos > 2) throw std::invalid_argument("mqttx::Binding: qos 必须在 [-1, 2]");

    filter_  = std::move(filter);
    handler_ = std::move(handler);
    qos_     = qos;
}

inline Subscription Binding::open(MqttClient& cli) const {
    const std::uint64_t slot = cli.install(filter_, handler_, qos_);
    if (slot == 0) throw std::runtime_error("mqttx::Binding: 注册失败");
    return Subscription{cli, filter_, qos_, slot};
}

inline void Binding::set_qos(int qos) {
    if (qos < -1 || qos > 2) throw std::invalid_argument("mqttx::Binding: qos 必须在 [-1, 2]");
    qos_ = qos;
}

// ---------------------------------------------------------------------------
// 工厂：统一成员函数 / 可调用对象 / 已擦除 Handler
// ---------------------------------------------------------------------------

template <auto M, typename Svc>
    requires std::is_member_function_pointer_v<decltype(M)> &&
             std::is_invocable_v<decltype(M), Svc*, const MessageView&>
[[nodiscard]] Binding make_binding(std::string filter, Svc& svc, int qos = -1) {
    return Binding{std::move(filter), Handler::member<Svc, M>(svc), qos};
}

template <typename F>
    requires MessageHandler<F> && (!std::is_same_v<std::remove_cvref_t<F>, Handler>)
[[nodiscard]] Binding make_binding(std::string filter, F&& fn, int qos = -1) {
    return Binding{std::move(filter), Handler::callable(std::forward<F>(fn)), qos};
}

[[nodiscard]] inline Binding make_binding(std::string filter, Handler handler, int qos = -1) {
    return Binding{std::move(filter), std::move(handler), qos};
}

template <auto M, typename Svc>
    requires std::is_member_function_pointer_v<decltype(M)> &&
             std::is_invocable_v<decltype(M), Svc*, const MessageView&>
[[nodiscard]] Subscription subscribe(MqttClient& cli, std::string filter, Svc& svc, int qos = -1) {
    return make_binding<M>(std::move(filter), svc, qos).open(cli);
}

template <typename F>
    requires MessageHandler<F> && (!std::is_same_v<std::remove_cvref_t<F>, Handler>)
[[nodiscard]] Subscription subscribe(MqttClient& cli, std::string filter, F&& fn, int qos = -1) {
    return make_binding(std::move(filter), std::forward<F>(fn), qos).open(cli);
}

[[nodiscard]] inline Subscription
subscribe(MqttClient& cli, std::string filter, Handler handler, int qos = -1) {
    return Binding{std::move(filter), std::move(handler), qos}.open(cli);
}

// ---------------------------------------------------------------------------
// SubscriptionGroup —— 批量/事务式订阅作用域
// ---------------------------------------------------------------------------

class SubscriptionGroup {
public:
    SubscriptionGroup() = default;
    SubscriptionGroup(const SubscriptionGroup&)            = delete;
    SubscriptionGroup& operator=(const SubscriptionGroup&) = delete;

    SubscriptionGroup(SubscriptionGroup&& o) noexcept
        : bindings_{std::move(o.bindings_)}, active_{std::move(o.active_)} {
        o.close_all();
    }

    SubscriptionGroup& operator=(SubscriptionGroup&& o) noexcept {
        if (this != &o) {
            close_all();
            bindings_ = std::move(o.bindings_);
            active_   = std::move(o.active_);
            o.close_all();
        }
        return *this;
    }

    ~SubscriptionGroup() { close_all(); }

    /// 只能在 open_all() 之前添加，避免“已打开但新绑定未生效”的歧义。
    /// @throw std::logic_error 已经打开
    void add(Binding binding) {
        if (!active_.empty())
            throw std::logic_error("mqttx::SubscriptionGroup: open_all 之后不能再 add");
        bindings_.push_back(std::move(binding));
    }

    /// 打开全部绑定；任一失败则关闭已打开的部分（强保证）。
    void open_all(MqttClient& cli) {
        if (!active_.empty()) return;

        std::vector<Subscription> opened;
        opened.reserve(bindings_.size());
        try {
            for (const auto& binding : bindings_) opened.push_back(binding.open(cli));
        }
        catch (...) {
            opened.clear();  // Subscription 析构即退订
            throw;
        }
        active_ = std::move(opened);
    }

    void close_all() noexcept { active_.clear(); }

    [[nodiscard]] bool        opened() const noexcept { return !active_.empty(); }
    [[nodiscard]] std::size_t size() const noexcept { return bindings_.size(); }

private:
    std::vector<Binding>      bindings_;
    std::vector<Subscription> active_;
};

}  // namespace mqttx

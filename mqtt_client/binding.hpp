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

#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

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

    /// 只在未激活时修改；已激活请用 Subscription::set_qos（后续阶段提供）
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
    [[nodiscard]] int qos() const noexcept { return qos_; }

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

}  // namespace mqttx

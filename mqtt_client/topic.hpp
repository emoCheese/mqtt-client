#pragma once

// ---------------------------------------------------------------------------
// Topic<Svc, M> —— 绑定 + 激活的兼容门面
//
//   mqttx::Topic<Session, &Session::on_cmd> t{cli, "robot/+/cmd", session, 1};
//   t.open();                       // 显式注册（构造期不把 this 交出去）
//
//   // 新代码更推荐直接使用 Binding / Subscription：
//   auto binding = mqttx::Binding{"robot/+/cmd", mqttx::on<&Session::on_cmd>(session), 1};
//   auto sub     = binding.open(cli);
//
// 设计要点：
//   1. Topic 只是 Binding + std::optional<Subscription> 的薄门面；
//   2. 构造只保存描述，不注册；open() 才产生注册；析构自动退订；
//   3. 目录持有 Handler 本身，move 只搬运 Subscription，不再需要 rebind；
//   4. 成员函数由模板参数 M 在编译期确定；可调用对象模式没有 service()。
// ---------------------------------------------------------------------------

#include "binding.hpp"
#include "handler.hpp"
#include "message.hpp"
#include "mqtt_client.hpp"

#include <cstddef>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

namespace mqttx {

template <typename Svc, auto M = nullptr>
class Topic {
public:
    /// 绑定到 svc 的成员函数（const / 非 const 均可，M 在编译期已知）
    Topic(MqttClient& cli, std::string topic, Svc& svc, int qos = -1)
        requires std::is_member_function_pointer_v<decltype(M)> &&
                 std::is_invocable_v<decltype(M), Svc*, const MessageView&>
        : cli_{&cli}
        , binding_{std::move(topic), Handler::member<Svc, M>(svc), qos}
        , svc_{&svc} {}

    /// 拥有型：绑定到可调用对象（lambda / 函数对象 / std::function）
    template <typename F>
        requires std::is_null_pointer_v<decltype(M)> &&
                 std::is_constructible_v<Svc, F> &&
                 std::is_invocable_v<Svc&, const MessageView&>
    Topic(MqttClient& cli, std::string topic, F&& fn, int qos = -1)
        : cli_{&cli}
        , binding_{std::move(topic), Handler::callable(std::forward<F>(fn)), qos}
        , svc_{nullptr} {}

    ~Topic() = default;

    Topic(const Topic&)            = delete;
    Topic& operator=(const Topic&) = delete;

    Topic(Topic&& o) noexcept
        : cli_{std::exchange(o.cli_, nullptr)}
        , binding_{std::move(o.binding_)}
        , sub_{std::move(o.sub_)}
        , svc_{std::exchange(o.svc_, nullptr)} {
        o.sub_.reset();  // optional move 会保留 moved-from 值，显式解除
    }

    Topic& operator=(Topic&& o) noexcept {
        if (this != &o) {
            close();
            cli_     = std::exchange(o.cli_, nullptr);
            binding_ = std::move(o.binding_);
            sub_     = std::move(o.sub_);
            svc_     = std::exchange(o.svc_, nullptr);
            o.sub_.reset();
        }
        return *this;
    }

    // ---------- 激活 / 卸载（幂等）----------

    /// 注册到客户端目录；成功返回 true。
    [[nodiscard]] bool open() {
        if (opened()) return true;  // 已激活
        if (cli_ == nullptr) return false;
        sub_.emplace(binding_.open(*cli_));
        return true;
    }

    /// 从目录摘除（最后一个绑定者会触发 UNSUBSCRIBE）
    void close() noexcept { sub_.reset(); }

    [[nodiscard]] bool opened() const noexcept { return sub_ && sub_->active(); }

    /// 修改 QoS：已激活时重新注册（触发重新 SUBSCRIBE）。
    /// 失败时恢复到旧的 QoS 与激活状态。
    void set_qos(int qos) {
        if (binding_.qos() == qos) return;
        const int  old_qos  = binding_.qos();
        const bool was_open = opened();
        if (was_open) close();
        binding_.set_qos(qos);
        if (was_open) {
            try {
                if (!open()) {
                    binding_.set_qos(old_qos);
                    (void)open();
                }
            }
            catch (...) {
                binding_.set_qos(old_qos);
                (void)open();
                throw;
            }
        }
    }

    // ---------- 业务接口 ----------

    /// 向本 topic 发布（qos < 0 使用客户端默认 QoS）
    /// @throw std::logic_error Topic 已被 move 走
    mqtt::delivery_token_ptr
    publish(std::string payload, int qos = -1, bool retain = false) {
        return require_client().publish(binding_.filter(), std::move(payload), qos, retain);
    }

    /// 二进制载荷
    mqtt::delivery_token_ptr
    publish_binary(const void* data, std::size_t len, int qos = -1, bool retain = false) {
        return require_client().publish_binary(binding_.filter(), data, len, qos, retain);
    }

    [[nodiscard]] const std::string& name() const noexcept { return binding_.filter(); }
    [[nodiscard]] int                qos() const noexcept { return binding_.qos(); }
    [[nodiscard]] MqttClient&        client() const { return require_client(); }

    /// 仅成员函数模式可用；可调用对象模式没有可返回的 Svc 对象。
    [[nodiscard]] Svc& service() requires (!std::is_null_pointer_v<decltype(M)>) {
        return *require_service();
    }
    [[nodiscard]] const Svc& service() const requires (!std::is_null_pointer_v<decltype(M)>) {
        return *require_service();
    }

private:
    [[nodiscard]] MqttClient& require_client() const {
        if (cli_ == nullptr) throw std::logic_error("mqttx::Topic: 未绑定客户端（对象已被移动）");
        return *cli_;
    }

    [[nodiscard]] Svc* require_service() const {
        if (svc_ == nullptr) throw std::logic_error("mqttx::Topic: 未绑定服务对象（对象已被移动）");
        return svc_;
    }

    MqttClient*               cli_{nullptr};
    Binding                   binding_;
    std::optional<Subscription> sub_;
    Svc*                      svc_{nullptr};  ///< 仅成员函数模式；可调用对象模式为 nullptr
};

using TopicFn = Topic<std::function<void(const MessageView&)>, nullptr>;

// ---------- 工厂函数 ----------

/// 成员函数模式：make_topic<&Session::on_cmd>(cli, "robot/+/cmd", session, 1)
template <auto M, typename Svc>
    requires std::is_member_function_pointer_v<decltype(M)> &&
             std::is_invocable_v<decltype(M), Svc*, const MessageView&>
[[nodiscard]] auto make_topic(MqttClient& cli, std::string topic, Svc& svc, int qos = -1) {
    return Topic<Svc, M>{cli, std::move(topic), svc, qos};
}

/// callable 模式：make_topic(cli, "a/b", [](const MessageView& m){...}, 1)
template <typename F>
    requires std::is_invocable_v<F&, const MessageView&>
[[nodiscard]] TopicFn make_topic(MqttClient& cli, std::string topic, F&& fn, int qos = -1) {
    return TopicFn{cli, std::move(topic), std::forward<F>(fn), qos};
}

}  // namespace mqttx

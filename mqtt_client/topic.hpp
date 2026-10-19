#pragma once

// ---------------------------------------------------------------------------
// Topic<Svc, M> —— 以 topic 为核心的绑定句柄
//
//   mqttx::Topic<Session, &Session::on_cmd> t{cli, "robot/+/cmd", session, 1};
//   t.open();                       // 显式注册（构造期不把 this 交出去）
//
//   // 或用工厂：auto t = mqttx::make_topic<&Session::on_cmd>(cli, ...);
//
// 设计要点：
//   1. 构造只保存参数，不注册；必须显式 open()，析构自动 close()；
//   2. 必须在 Topic 完全构造之后再 open()（例如 Session 构造体末尾统一 open）；
//   3. topic 是 MQTT topic filter，可含 '+' / '#'：到达的消息按 filter 规则路由；
//   4. move 之后自动 rebind，保证目录里的回调指针始终指向存活对象；
//   5. 一个 filter 可以被多个 Topic 绑定（多播），引用计数归零才退订；
//   6. 成员函数由模板参数 M 在编译期确定，没有运行时分支或间接调用。
//
// 支持三种处理器形式：
//   - 非 const 成员函数：Topic<Svc, &Svc::on_msg>{cli, topic, svc, qos}
//   - const 成员函数    ：Topic<Svc, &Svc::on_msg>{cli, topic, svc, qos}
//   - 可调用对象        ：make_topic(cli, topic, [](const MessageView& m){...}, qos)
// ---------------------------------------------------------------------------

#include "message.hpp"
#include "mqtt_client.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
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
        : cli_{&cli}, topic_{std::move(topic)}, svc_{&svc}, qos_{qos} {}

    /// 拥有型：绑定到可调用对象（lambda / 函数对象 / std::function）
    template <typename F>
        requires std::is_null_pointer_v<decltype(M)> &&
                 std::is_constructible_v<Svc, F> &&
                 std::is_invocable_v<Svc&, const MessageView&>
    Topic(MqttClient& cli, std::string topic, F&& fn, int qos = -1)
        : cli_{&cli}
        , topic_{std::move(topic)}
        , svc_{nullptr}
        , owned_{std::make_unique<Svc>(std::forward<F>(fn))}
        , qos_{qos} {
        svc_ = owned_.get();
    }

    ~Topic() { close(); }

    Topic(const Topic&)            = delete;
    Topic& operator=(const Topic&) = delete;

    Topic(Topic&& o) noexcept
        : cli_{std::exchange(o.cli_, nullptr)}
        , topic_{std::move(o.topic_)}
        , svc_{o.svc_}
        , owned_{std::move(o.owned_)}
        , qos_{o.qos_}
        , slot_{std::exchange(o.slot_, 0)} {
        if (owned_) svc_ = owned_.get();
        // 目录里仍存着旧对象指针：必须重新绑定到新对象
        if (slot_ != 0 && cli_ != nullptr) cli_->rebind(slot_, this);
    }

    Topic& operator=(Topic&& o) noexcept {
        if (this != &o) {
            close();
            cli_     = std::exchange(o.cli_, nullptr);
            topic_   = std::move(o.topic_);
            svc_     = o.svc_;
            owned_   = std::move(o.owned_);
            qos_     = o.qos_;
            slot_    = std::exchange(o.slot_, 0);
            if (owned_) svc_ = owned_.get();
            if (slot_ != 0 && cli_ != nullptr) cli_->rebind(slot_, this);
        }
        return *this;
    }

    // ---------- 激活 / 卸载（幂等）----------

    /// 注册到客户端目录；成功返回 true。
    bool open() {
        if (slot_ != 0) return true;                        // 已激活
        if (cli_ == nullptr || svc_ == nullptr) return false;
        slot_ = cli_->install(topic_, this, &Topic::invoke, qos_);
        return slot_ != 0;
    }

    /// 从目录摘除（最后一个绑定者会触发 UNSUBSCRIBE）
    void close() noexcept {
        if (slot_ == 0 || cli_ == nullptr) return;
        cli_->uninstall(slot_);
        slot_ = 0;
    }

    [[nodiscard]] bool opened() const noexcept { return slot_ != 0; }

    /// 修改 QoS：已激活时重新注册（触发重新 SUBSCRIBE）
    void set_qos(int qos) {
        if (qos_ == qos) return;
        const bool was_open = opened();
        if (was_open) close();
        qos_ = qos;
        if (was_open) open();
    }

    // ---------- 业务接口 ----------

    /// 向本 topic 发布（qos < 0 使用客户端默认 QoS）
    /// @throw std::logic_error Topic 已被 move 走
    mqtt::delivery_token_ptr
    publish(std::string payload, int qos = -1, bool retain = false) {
        return require_client().publish(topic_, std::move(payload), qos, retain);
    }

    /// 二进制载荷
    mqtt::delivery_token_ptr
    publish_binary(const void* data, std::size_t len, int qos = -1, bool retain = false) {
        return require_client().publish_binary(topic_, data, len, qos, retain);
    }

    [[nodiscard]] const std::string& name() const noexcept { return topic_; }
    [[nodiscard]] int                qos() const noexcept { return qos_; }
    [[nodiscard]] Svc&               service() noexcept { return *svc_; }
    [[nodiscard]] const Svc&         service() const noexcept { return *svc_; }
    [[nodiscard]] MqttClient&        client() const { return require_client(); }

private:
    [[nodiscard]] MqttClient& require_client() const {
        if (cli_ == nullptr) throw std::logic_error("mqttx::Topic: 未绑定客户端（对象已被移动）");
        return *cli_;
    }

    static void invoke(void* self, const MessageView& msg) {
        auto* t = static_cast<Topic*>(self);
        if constexpr (std::is_member_function_pointer_v<decltype(M)>) {
            // const 成员函数判定：能否用 const Svc* 调用 M
            if constexpr (std::is_invocable_v<decltype(M), const Svc*, const MessageView&>) {
                (static_cast<const Svc*>(t->svc_)->*M)(msg);   // 直接调用，可内联
            } else {
                (t->svc_->*M)(msg);                            // 直接调用，可内联
            }
        } else {
            if constexpr (std::is_invocable_v<Svc&, const MessageView&>) {
                if (t->svc_ != nullptr) (*t->svc_)(msg);       // Svc 编译期已知，可内联
            }
        }
    }

    MqttClient* cli_{nullptr};
    std::string topic_;
    Svc*        svc_{nullptr};
    std::unique_ptr<Svc> owned_;          ///< 可调用对象模式下自持有
    int         qos_{-1};
    std::uint64_t slot_{0};
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

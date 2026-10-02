#pragma once

// ---------------------------------------------------------------------------
// Topic<Svc> —— 以 topic 为核心的绑定句柄
//
//   mqttx::Topic<Session> t{cli, "robot/+/cmd", session, &Session::on_cmd, 1};
//   t.open();                       // 显式注册（构造期不把 this 交出去）
//
// 设计要点：
//   1. 构造只保存参数，不注册；必须显式 open()，析构自动 close()；
//   2. 必须在 Topic 完全构造之后再 open()（例如 Session 构造体末尾统一 open）；
//   3. topic 是 MQTT topic filter，可含 '+' / '#'：到达的消息按 filter 规则路由；
//   4. move 之后自动 rebind，保证目录里的回调指针始终指向存活对象；
//   5. 一个 filter 可以被多个 Topic 绑定（多播），引用计数归零才退订。
//
// 支持三种处理器形式：
//   - 非 const 成员函数：Topic<Svc>{cli, topic, svc, &Svc::on_msg, qos}
//   - const 成员函数    ：Topic<Svc>{cli, topic, svc, &Svc::on_msg, qos}
//   - 可调用对象        ：make_topic(cli, topic, [](const MessageView& m){...}, qos)
// ---------------------------------------------------------------------------

#include "message.hpp"
#include "mqtt_client.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

namespace mqttx {

template <typename Svc>
class Topic {
public:
    using Method      = void (Svc::*)(const MessageView&);
    using ConstMethod = void (Svc::*)(const MessageView&) const;

    /// 绑定到 svc 的非 const 成员函数
    Topic(MqttClient& cli, std::string topic, Svc& svc, Method method, int qos = -1)
        : cli_{&cli}, topic_{std::move(topic)}, svc_{&svc}, method_{require(method)}, qos_{qos} {
    }

    /// 绑定到 svc 的 const 成员函数
    Topic(MqttClient& cli, std::string topic, Svc& svc, ConstMethod method, int qos = -1)
        : cli_{&cli}, topic_{std::move(topic)}, svc_{&svc}, cmethod_{require(method)}, qos_{qos} {
    }

    /// 拥有型：绑定到可调用对象（lambda / 函数对象 / std::function）
    template <typename F>
        requires(!std::is_same_v<std::remove_cvref_t<F>, Svc>) &&
                std::is_constructible_v<Svc, F> && std::is_invocable_v<Svc&, const MessageView&>
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
        , method_{o.method_}
        , cmethod_{o.cmethod_}
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
            method_  = o.method_;
            cmethod_ = o.cmethod_;
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
    template <typename M>
    static M require(M method) {
        if (method == nullptr) throw std::invalid_argument("mqttx::Topic: 处理器不能为空");
        return method;
    }

    [[nodiscard]] MqttClient& require_client() const {
        if (cli_ == nullptr) throw std::logic_error("mqttx::Topic: 未绑定客户端（对象已被移动）");
        return *cli_;
    }

    static void invoke(void* self, const MessageView& msg) {
        auto* t = static_cast<Topic*>(self);
        if (t->method_ != nullptr) {
            (t->svc_->*t->method_)(msg);
            return;
        }
        if (t->cmethod_ != nullptr) {
            (static_cast<const Svc*>(t->svc_)->*t->cmethod_)(msg);
            return;
        }
        // 拥有型可调用对象；Svc 不可调用时该分支在编译期被丢弃
        if constexpr (std::is_invocable_v<Svc&, const MessageView&>) {
            if (t->svc_ != nullptr) (*t->svc_)(msg);
        }
    }

    MqttClient* cli_{nullptr};
    std::string topic_;
    Svc*        svc_{nullptr};
    std::unique_ptr<Svc> owned_;          ///< 可调用对象模式下自持有
    Method      method_{nullptr};
    ConstMethod cmethod_{nullptr};
    int         qos_{-1};
    std::uint64_t slot_{0};
};

/// 以可调用对象（lambda / std::function）为处理器的 Topic
using TopicFn = Topic<std::function<void(const MessageView&)>>;

/// 便捷工厂：auto t = mqttx::make_topic(cli, "a/b", [](const MessageView& m){ ... });
template <typename F>
    requires std::is_invocable_v<F&, const MessageView&>
[[nodiscard]] TopicFn make_topic(MqttClient& cli, std::string topic, F&& fn, int qos = -1) {
    return TopicFn{cli, std::move(topic), std::forward<F>(fn), qos};
}

}  // namespace mqttx

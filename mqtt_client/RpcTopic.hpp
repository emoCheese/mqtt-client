#pragma once
#include "mqtt_client.hpp"
#include <string>
#include <utility>



namespace mqttx {

template<typename Svc>
class RpcTopic
{
public:
    using Method      = void (Svc::*)(const MessageView&);
    using ConstMethod = void (Svc::*)(const MessageView&) const;

    /// 绑定到 svc 的非 const 成员函数
    RpcTopic(MqttClient& cli, std::string request_topic, std::string response_topic, Svc& svc, Method method, int qos = -1)
        : cli_{&cli}
        , request_topic_{std::move(request_topic)}
        , response_topic_(std::move(response_topic))
        , svc_{&svc}, method_{require(method)}
        , qos_{qos} 
    {

    }

    /// 绑定到 svc 的 const 成员函数
    RpcTopic(MqttClient& cli, std::string request_topic, std::string response_topic, Svc& svc, ConstMethod method, int qos = -1)
        : cli_{&cli}
        , request_topic_{std::move(request_topic)}
        , response_topic_(std::move(response_topic))
        , svc_{&svc}
        , cmethod_{require(method)}
        , qos_{qos} 
    {    

    }

    /// 拥有型：绑定到可调用对象（lambda / 函数对象 / std::function）
    template <typename F> requires(!std::is_same_v<std::remove_cvref_t<F>, Svc>) && std::is_constructible_v<Svc, F> && std::is_invocable_v<Svc&, const MessageView&>
    RpcTopic(MqttClient& cli, std::string request_topic, std::string response_topic, F&& fn, int qos = -1)
        : cli_{&cli}
        , request_topic_{std::move(request_topic)}
        , response_topic_(std::move(response_topic))
        , svc_{nullptr}
        , owned_{std::make_unique<Svc>(std::forward<F>(fn))}
        , qos_{qos} 
    {
        svc_ = owned_.get();
    }

    ~RpcTopic() { close(); }

    RpcTopic(const RpcTopic&)            = delete;
    RpcTopic& operator=(const RpcTopic&) = delete;

    RpcTopic(RpcTopic&& o) noexcept
        : cli_{std::exchange(o.cli_, nullptr)}
        , request_topic_{std::move(o.request_topic_)}
        , response_topic_(std::move(o.response_topic_))
        , svc_{o.svc_}
        , owned_{std::move(o.owned_)}
        , method_{o.method_}
        , cmethod_{o.cmethod_}
        , qos_{o.qos_}
        , slot_{std::exchange(o.slot_, 0)} 
    {
        if (owned_) svc_ = owned_.get();
        // 目录里仍存着旧对象指针：必须重新绑定到新对象
        if (slot_ != 0 && cli_ != nullptr) cli_->rebind(slot_, this);
    }

    RpcTopic& operator=(RpcTopic&& o) noexcept 
    {
        if (this != &o) {
            close();
            cli_     = std::exchange(o.cli_, nullptr);
            request_topic_   = std::move(o.request_topic_);
            response_topic_  = std::move(o.response_topic_);
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
        slot_ = cli_->install(response_topic_, this, &RpcTopic::invoke, qos_);
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
    /// @throw std::logic_error RpcTopic 已被 move 走
    mqtt::delivery_token_ptr
    request(std::string payload, int qos = -1, bool retain = false) {
        return require_client().publish(request_topic_, std::move(payload), qos, retain);
    }

    /// 二进制载荷
    mqtt::delivery_token_ptr
    request_binary(const void* data, std::size_t len, int qos = -1, bool retain = false) {
        return require_client().publish_binary(request_topic_, data, len, qos, retain);
    }

    [[nodiscard]] std::string_view   request_topic() const noexcept { return request_topic_; }
    [[nodiscard]] std::string_view   response_topic() const noexcept { return response_topic_; }
    [[nodiscard]] int                qos() const noexcept { return qos_; }
    [[nodiscard]] Svc&               service() noexcept { return *svc_; }
    [[nodiscard]] const Svc&         service() const noexcept { return *svc_; }
    [[nodiscard]] MqttClient&        client() const { return require_client(); }

private:
    template <typename M>
    static M require(M method) {
        if (method == nullptr) throw std::invalid_argument("mqttx::RpcTopic: 处理器不能为空");
        return method;
    }

    [[nodiscard]] MqttClient& require_client() const {
        if (cli_ == nullptr) throw std::logic_error("mqttx::RpcTopic: 未绑定客户端（对象已被移动）");
        return *cli_;
    }

    static void invoke(void* self, const MessageView& msg) {
        auto* t = static_cast<RpcTopic*>(self);
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
    std::string request_topic_;
    std::string response_topic_;
    Svc*        svc_{nullptr};
    std::unique_ptr<Svc> owned_;          ///< 可调用对象模式下自持有
    Method      method_{nullptr};
    ConstMethod cmethod_{nullptr};
    int         qos_{-1};
    std::uint64_t slot_{0};
};


}



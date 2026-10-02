#pragma once

#include "imqtt_client.h"
#include "message.hpp"
#include "topic_directory.hpp"

#include <mqtt/async_client.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <vector>

namespace mqttx {

/// MQTT 连接参数
struct MqttConfig {
    std::string broker_url;              ///< 例如 "tcp://127.0.0.1:1883"
    std::string client_id;               ///< 为空时自动生成（mqttx-xxxx）
    std::string username;
    std::string password;
    int         qos{1};                  ///< 默认 QoS（install/publish 传 -1 时使用）
    bool        clean_session{false};    ///< v5 的 clean_start
    int         keep_alive_sec{30};
    std::chrono::milliseconds reconnect_min{1000};
    std::chrono::milliseconds reconnect_max{30000};
    std::chrono::milliseconds poll_timeout{100};    ///< 消费线程单次等待上限
    std::chrono::milliseconds connect_timeout{5000};///< start() 等待连接的上限
};

/// 以 topic 为核心的 MQTT 客户端：
///   - 内部维护 TopicDirectory（topic → 若干 handler）；
///   - 连接建立/重连后自动补齐订阅，摘除最后一个 handler 时自动退订；
///   - 消费线程把消息按 topic 派发给全部 handler。
class MqttClient : public IMqttClient {
public:
    using InvokeFn = TopicDirectory::InvokeFn;

    /// @throw std::invalid_argument broker_url 为空
    explicit MqttClient(MqttConfig cfg);
    ~MqttClient() override;

    MqttClient(const MqttClient&)            = delete;
    MqttClient& operator=(const MqttClient&) = delete;
    MqttClient(MqttClient&&)                 = delete;
    MqttClient& operator=(MqttClient&&)      = delete;

    // ---- 生命周期 ----------------------------------------------------------

    /// 连接 broker 并启动消费线程。已在运行则直接返回。
    /// @throw mqtt::exception / std::runtime_error 连接失败（状态已回滚）
    void start() override;

    /// 停止消费线程并断开连接（幂等）。注意：不要在 handler 内部析构 MqttClient。
    void stop() noexcept override;

    [[nodiscard]] bool is_running() const noexcept override;

    /// 底层 paho 是否处于已连接状态
    [[nodiscard]] bool is_connected() const noexcept;

    [[nodiscard]] const MqttConfig& config() const noexcept { return cfg_; }

    // ---- topic 目录（通常由 Topic<> 间接调用）-------------------------------

    /// 注册 handler 并（必要时）立即订阅；返回 slot。qos < 0 时使用 cfg.qos。
    /// @throw std::invalid_argument topic 为空
    std::uint64_t install(const std::string& topic, void* obj, InvokeFn fn, int qos = -1) override;

    /// 摘除 slot：引用计数归零时退订。可能短暂阻塞到在飞回调结束。
    void uninstall(std::uint64_t slot) noexcept override;

    /// Topic 对象 move 之后更新目录中的指针
    void rebind(std::uint64_t slot, void* obj) noexcept override;

    /// 当前需要订阅的 (topic, qos) 快照
    [[nodiscard]] std::vector<TopicDirectory::Subscription> subscriptions() const;

    [[nodiscard]] std::size_t topic_count() const;
    [[nodiscard]] std::size_t handler_count() const;

    // ---- 发布 --------------------------------------------------------------

    /// 发布文本载荷；qos < 0 时使用 cfg.qos。
    /// @throw mqtt::exception 未连接等底层错误
    mqtt::delivery_token_ptr publish(const std::string& topic,
                                     std::string      payload,
                                     int              qos    = -1,
                                     bool             retain = false);

    /// 发布二进制载荷
    mqtt::delivery_token_ptr publish_binary(const std::string& topic,
                                            const void*        data,
                                            std::size_t        len,
                                            int                qos    = -1,
                                            bool               retain = false);

    // ---- 本地派发（消费线程内部使用；也可用于离线注入/单元测试）--------------

    /// 把消息派发给该 topic 的全部 handler，返回实际调用的 handler 数。
    std::size_t dispatch(std::string_view topic, const MessageView& msg);

private:
    void consume_loop(std::stop_token st);

    /// 连接（含重连）成功后的订阅补齐：
    ///   - session_present == false → 会话是新的，清空订阅记录并全量订阅；
    ///   - session_present == true  → 只补订阅本进程内新增的 topic
    ///     （首次连接时无从得知 broker 侧集合，仍然全量订阅，避免漏订阅）。
    void on_connected(bool session_present);

    /// 按目录补齐订阅（幂等）
    void subscribe_all();

    MqttConfig                          cfg_;
    std::unique_ptr<mqtt::async_client> inner_;
    std::jthread                        consumer_;
    std::atomic<bool>                   running_{false};
    TopicDirectory                      dir_;

    /// 当前会话内 broker 侧已存在的订阅（用于重连时只补差异）
    std::mutex                                                     session_mtx_;
    std::unordered_set<std::string, SvHash, SvEqual>               session_subs_;
    bool                                                           connected_once_{false};
};

}  // namespace mqttx

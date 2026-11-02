#pragma once

// ---------------------------------------------------------------------------
// IMqttClient —— 订阅侧最小契约
//
// 该库以 topic 为核心，接口只描述“按 topic 注册 / 摘除回调”这件事，
// 不泄漏任何具体传输实现（paho / 测试替身）的细节。
// ---------------------------------------------------------------------------

#include "handler.hpp"

#include <cstdint>
#include <string>

namespace mqttx {

class IMqttClient {
public:
    IMqttClient()                            = default;
    IMqttClient(const IMqttClient&)          = delete;
    IMqttClient& operator=(const IMqttClient&) = delete;
    virtual ~IMqttClient()                   = default;

    /// 建立连接并开始消费（失败时抛异常并回滚状态）
    virtual void start() = 0;

    /// 断开连接并停止消费（幂等，不抛异常）
    virtual void stop() noexcept = 0;

    [[nodiscard]] virtual bool is_running() const noexcept = 0;

    /// 注册 topic 的 handler，返回 slot（同一 topic 可注册多个）；qos < 0 用默认值
    virtual std::uint64_t install(const std::string& topic, Handler handler, int qos = -1) = 0;

    /// 摘除 slot（幂等，不抛异常）
    virtual void uninstall(std::uint64_t slot) noexcept = 0;
};

}  // namespace mqttx

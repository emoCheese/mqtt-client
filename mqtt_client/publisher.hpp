#pragma once

// ---------------------------------------------------------------------------
// Publisher —— 发布侧的值对象，与绑定/订阅生命周期无关
//
//   mqttx::Publisher pub{cli, "robot/1/cmd", 2};
//   pub.publish(payload);
//
// 构造期即校验 topic 是具体 topic（不含 '+' / '#'），从入口杜绝把 filter
// 当作 topic name 发布这类协议违规。
// ---------------------------------------------------------------------------

#include "mqtt_client.hpp"
#include "topic_filter.hpp"

#include <cstddef>
#include <stdexcept>
#include <string>
#include <utility>

namespace mqttx {

class Publisher {
public:
    /// @throw std::invalid_argument topic 为空 / 含通配符 / qos 越界
    Publisher(MqttClient& cli, std::string topic, int qos = -1, bool retain = false)
        : cli_{&cli}, topic_{std::move(topic)}, qos_{qos}, retain_{retain} {
        if (topic_.empty()) throw std::invalid_argument("mqttx::Publisher: topic 不能为空");
        if (is_wildcard_filter(topic_))
            throw std::invalid_argument("mqttx::Publisher: 不能向含通配符的 filter 发布");
        if (qos_ < -1 || qos_ > 2)
            throw std::invalid_argument("mqttx::Publisher: qos 必须在 [-1, 2]");
    }

    /// qos < 0 使用客户端默认 QoS
    mqtt::delivery_token_ptr publish(std::string payload) const {
        return cli_->publish(topic_, std::move(payload), qos_, retain_);
    }

    mqtt::delivery_token_ptr publish_binary(const void* data, std::size_t len) const {
        return cli_->publish_binary(topic_, data, len, qos_, retain_);
    }

    [[nodiscard]] const std::string& topic() const noexcept { return topic_; }
    [[nodiscard]] int                qos() const noexcept { return qos_; }
    [[nodiscard]] bool               retain() const noexcept { return retain_; }

private:
    MqttClient* cli_;
    std::string topic_;
    int         qos_{-1};
    bool        retain_{false};
};

}  // namespace mqttx

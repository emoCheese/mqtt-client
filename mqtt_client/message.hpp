#pragma once

#include <mqtt/properties.h>

#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace mqttx {

/// 保活凭据（引用计数句柄，内部类型擦除）。
///
/// 库在派发消息时会为**每个 handler 调用**生成一份凭据并放进 MessageView。
/// 只要还有一份 MessageView（或 deep_copy() 得到的 Owned）存活：
///   - topic / payload / props 指向的数据仍然有效（凭据持有底层消息）；
///   - 该 handler 的登记项不会被摘除回收 —— Topic::close() / 析构会等待，
///     这样异步任务里的业务对象不会在“还在飞”的时候被销毁。
///
/// 典型用法（把消息投递到线程池）：
///     pool.post([m] { use(m.topic, m.payload); });   // 按值捕获即可
class KeepAlive {
public:
    KeepAlive() noexcept = default;

    explicit KeepAlive(std::shared_ptr<const void> token) noexcept : token_{std::move(token)} {}

    /// 是否持有有效凭据
    [[nodiscard]] bool valid() const noexcept { return static_cast<bool>(token_); }

    /// 提前释放凭据（等价于不再保活）
    void reset() noexcept { token_.reset(); }

    /// 当前凭据的引用数（1 表示只有自己；调试用）
    [[nodiscard]] long use_count() const noexcept { return token_.use_count(); }

private:
    std::shared_ptr<const void> token_;
};

/// 消息的借用视图。
///
/// - 同步 handler：直接使用即可，无需任何额外操作；
/// - 异步 handler（线程池 / 其它线程）：**按值捕获整个 MessageView**（或先
///   deep_copy()），凭据会随副本一起存活，消息数据与 handler 登记项在副本
///   析构前一直有效。
struct MessageView {
    std::string_view        topic;          ///< 消息到达的 topic
    std::string_view        payload;        ///< 原始字节（可能是二进制）
    const mqtt::properties* props{nullptr}; ///< MQTT v5 属性（可能为空）
    int                     qos{0};
    bool                    retain{false};
    KeepAlive               keep_alive;     ///< 由库填写；异步使用时随视图一起捕获

    /// 自持有的副本：字符串独立，可安全跨线程/延迟使用
    struct Owned {
        std::string      topic;
        std::string      payload;
        mqtt::properties props;
        int              qos{0};
        bool             retain{false};
        KeepAlive        keep_alive;  ///< 同步/异步都保持登记项存活

        /// 注意：返回的视图指向本对象的成员，本对象移动/析构后失效
        [[nodiscard]] MessageView view() const noexcept {
            return MessageView{topic, payload, &props, qos, retain, keep_alive};
        }
    };

    [[nodiscard]] bool empty() const noexcept { return topic.empty(); }

    /// 深拷贝一份可长期保存的消息（保留保活凭据）
    [[nodiscard]] Owned deep_copy() const {
        return Owned{
            std::string{topic},
            std::string{payload},
            props != nullptr ? *props : mqtt::properties{},
            qos,
            retain,
            keep_alive,
        };
    }
};

}  // namespace mqttx

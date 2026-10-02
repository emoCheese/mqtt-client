#include "mqtt_client.hpp"

#include <spdlog/spdlog.h>

#include <chrono>
#include <random>
#include <stdexcept>
#include <unordered_set>
#include <utility>

namespace mqttx {
namespace {

/// 生成默认 client id：mqttx-<随机>（避免多个进程用同一个 id 互相踢下线）
std::string generate_client_id() {
    static constexpr char kHex[] = "0123456789abcdef";
    std::random_device      rd;
    std::uniform_int_distribution<int> dist(0, 15);

    std::string id = "mqttx-";
    id.reserve(id.size() + 12);
    for (int i = 0; i < 12; ++i) id.push_back(kHex[dist(rd)]);
    return id;
}

}  // namespace

// ---------------------------------------------------------------------------
// 构造 / 析构
// ---------------------------------------------------------------------------

MqttClient::MqttClient(MqttConfig cfg) : cfg_{std::move(cfg)} {
    if (cfg_.broker_url.empty())
        throw std::invalid_argument("mqttx::MqttClient: MqttConfig::broker_url 不能为空");
    if (cfg_.client_id.empty()) cfg_.client_id = generate_client_id();
    if (cfg_.reconnect_min > cfg_.reconnect_max) cfg_.reconnect_max = cfg_.reconnect_min;

    inner_ = std::make_unique<mqtt::async_client>(cfg_.broker_url, cfg_.client_id);
}

MqttClient::~MqttClient() { stop(); }

// ---------------------------------------------------------------------------
// 连接生命周期
// ---------------------------------------------------------------------------

void MqttClient::start() {
    if (running_.exchange(true, std::memory_order_acq_rel)) return;  // 已在运行

    try {
        auto opts = mqtt::connect_options_builder::v5()
                        .clean_start(cfg_.clean_session)
                        .keep_alive_interval(std::chrono::seconds(cfg_.keep_alive_sec))
                        .automatic_reconnect(cfg_.reconnect_min, cfg_.reconnect_max)
                        .connect_timeout(cfg_.connect_timeout)
                        .finalize();

        if (!cfg_.username.empty()) opts.set_user_name(cfg_.username);
        if (!cfg_.password.empty()) opts.set_password(cfg_.password);

        // 先开消费队列，避免连接完成后、消费线程起来之前丢消息
        inner_->start_consuming();

        auto tok = inner_->connect(opts);
        if (!tok->wait_for(cfg_.connect_timeout))
            throw std::runtime_error("mqttx::MqttClient: 连接 " + cfg_.broker_url + " 超时");

        const auto rsp = tok->get_connect_response();
        spdlog::info("[mqttx] 已连接 {} (client_id={}, session_present={})",
                     cfg_.broker_url, cfg_.client_id, rsp.is_session_present());

        on_connected(rsp.is_session_present());

        consumer_ = std::jthread([this](std::stop_token st) { consume_loop(st); });
    }
    catch (...) {
        // 失败回滚：不能留下 running_=true 但既没连接也没消费线程的状态
        running_.store(false, std::memory_order_release);
        try {
            inner_->stop_consuming();
        }
        catch (...) {
        }
        throw;
    }
}

void MqttClient::stop() noexcept {
    if (!running_.exchange(false, std::memory_order_acq_rel)) return;  // 幂等

    consumer_.request_stop();

    try {
        inner_->stop_consuming();  // 唤醒可能正在等待事件的消费线程
    }
    catch (...) {
    }

    // 在消费线程内部调用 stop() 时不能 join 自己
    if (consumer_.joinable() && consumer_.get_id() != std::this_thread::get_id()) {
        try {
            consumer_.join();
        }
        catch (...) {
        }
    }

    try {
        if (inner_->is_connected()) inner_->disconnect()->wait();
    }
    catch (...) {
    }
}

bool MqttClient::is_running() const noexcept { return running_.load(std::memory_order_acquire); }

bool MqttClient::is_connected() const noexcept {
    try {
        return inner_ != nullptr && inner_->is_connected();
    }
    catch (...) {
        return false;
    }
}

// ---------------------------------------------------------------------------
// topic 目录
// ---------------------------------------------------------------------------

std::uint64_t MqttClient::install(const std::string& topic, void* obj, InvokeFn fn, int qos) {
    const int eff_qos = (qos < 0) ? cfg_.qos : qos;

    const auto res = dir_.add(topic, obj, fn, eff_qos);

    // 目录说需要（重新）订阅，且客户端已在运行时才立即下发；
    // 未运行时由 on_connected()/subscribe_all() 统一补齐。
    if (res.subscribe && is_running()) {
        const std::lock_guard lk{session_mtx_};
        try {
            inner_->subscribe(topic, res.qos);
            session_subs_.insert(topic);
            spdlog::debug("[mqttx] SUBSCRIBE {} (qos={})", topic, res.qos);
        }
        catch (const mqtt::exception& e) {
            spdlog::error("[mqttx] SUBSCRIBE {} 失败: {}", topic, e.what());
        }
    }
    return res.slot;
}

void MqttClient::uninstall(std::uint64_t slot) noexcept {
    try {
        const auto res = dir_.remove(slot);
        if (!res.removed || res.topic.empty()) return;

        if (res.unsubscribe && is_running()) {
            const std::lock_guard lk{session_mtx_};
            session_subs_.erase(res.topic);
            try {
                inner_->unsubscribe(res.topic);
                spdlog::debug("[mqttx] UNSUBSCRIBE {}", res.topic);
            }
            catch (const mqtt::exception& e) {
                spdlog::error("[mqttx] UNSUBSCRIBE {} 失败: {}", res.topic, e.what());
            }
        }
    }
    catch (...) {
        // noexcept：摘除失败也不能让析构路径抛异常
    }
}

void MqttClient::rebind(std::uint64_t slot, void* obj) noexcept { dir_.rebind(slot, obj); }

std::vector<TopicDirectory::Subscription> MqttClient::subscriptions() const {
    return dir_.subscriptions();
}

std::size_t MqttClient::topic_count() const { return dir_.topic_count(); }

std::size_t MqttClient::handler_count() const { return dir_.handler_count(); }

// ---------------------------------------------------------------------------
// 发布
// ---------------------------------------------------------------------------

mqtt::delivery_token_ptr
MqttClient::publish(const std::string& topic, std::string payload, int qos, bool retain) {
    const int eff_qos = (qos < 0) ? cfg_.qos : qos;
    return inner_->publish(mqtt::make_message(topic, std::move(payload), eff_qos, retain));
}

mqtt::delivery_token_ptr MqttClient::publish_binary(const std::string& topic,
                                                    const void*        data,
                                                    std::size_t        len,
                                                    int                qos,
                                                    bool               retain) {
    const int eff_qos = (qos < 0) ? cfg_.qos : qos;
    return inner_->publish(mqtt::make_message(topic, data, len, eff_qos, retain));
}

// ---------------------------------------------------------------------------
// 本地派发
// ---------------------------------------------------------------------------

std::size_t MqttClient::dispatch(std::string_view topic, const MessageView& msg) {
    try {
        return dir_.dispatch(topic, msg);
    }
    catch (const std::exception& e) {
        spdlog::error("[mqttx] 派发 topic={} 失败: {}", topic, e.what());
    }
    catch (...) {
        spdlog::error("[mqttx] 派发 topic={} 失败: 未知异常", topic);
    }
    return 0;
}

// ---------------------------------------------------------------------------
// 内部：连接建立后的订阅补齐
// ---------------------------------------------------------------------------

void MqttClient::on_connected(bool session_present) {
    {
        const std::lock_guard lk{session_mtx_};
        // 会话被 broker 丢弃 → 之前的订阅记录全部失效
        if (!session_present) session_subs_.clear();
    }

    subscribe_all();

    const std::lock_guard lk{session_mtx_};
    connected_once_ = true;  // 此后 session_subs_ 可信，重连只需补订阅缺失项
}

void MqttClient::subscribe_all() {
    const auto subs = dir_.subscriptions();
    if (subs.empty()) return;

    const std::lock_guard lk{session_mtx_};

    // 本进程首次连接且 broker 说会话仍然存在时，我们无从得知 broker 侧的订阅集合，
    // 宁可多收一次 retained，也不能漏订阅。
    const bool trust_session = connected_once_;

    for (const auto& s : subs) {
        if (trust_session && session_subs_.contains(s.topic)) continue;

        try {
            inner_->subscribe(s.topic, s.qos);
            session_subs_.insert(s.topic);
            spdlog::debug("[mqttx] SUBSCRIBE {} (qos={})", s.topic, s.qos);
        }
        catch (const mqtt::exception& e) {
            spdlog::error("[mqttx] SUBSCRIBE {} 失败: {}", s.topic, e.what());
        }
    }
}

// ---------------------------------------------------------------------------
// 内部：消费线程
// ---------------------------------------------------------------------------

void MqttClient::consume_loop(std::stop_token st) {
    spdlog::debug("[mqttx] 消费线程启动");

    while (!st.stop_requested() && running_.load(std::memory_order_acquire)) {
        mqtt::event evt;
        bool        got = false;
        try {
            // paho.mqtt.cpp >= 1.5：bool try_consume_event_for(event*, duration)
            got = inner_->try_consume_event_for(&evt, cfg_.poll_timeout);
        } catch (const mqtt::exception& e) {
            spdlog::error("[mqttx] 消费队列异常，退出消费线程: {}", e.what());
            break;
        }
        if (!got) continue;  // 超时：回到循环检查 stop_token

        if (auto* pmsg = evt.get_message_if()) {
            if (!*pmsg) continue;  // 超时/空事件占位

            const mqtt::message& src = **pmsg;
            const MessageView    mv{
                .topic   = src.get_topic(),
                .payload = src.get_payload_str(),
                .props   = &src.get_properties(),
                .qos     = src.get_qos(),
                .retain  = src.is_retained(),
                // 视图是借用的；把消息本身放进保活凭据，handler 把视图副本投递到
                // 线程池后，数据仍然有效（直到副本与凭据一起释放）。
                .keep_alive = KeepAlive{std::shared_ptr<const void>{*pmsg}},
            };
            dispatch(mv.topic, mv);
        }
        else if (evt.is_connected()) {
            // 连接就绪事件：补齐订阅（幂等；重连时同一路径即可恢复订阅）
            spdlog::info("[mqttx] 连接就绪，补齐订阅（幂等）");
            on_connected(true);
        }
        else if (evt.is_connection_lost()) {
            spdlog::warn("[mqttx] 连接丢失，等待自动重连");
        }
        else if (evt.is_disconnected()) {
            spdlog::warn("[mqttx] broker 主动断开连接");
        }
    }

    spdlog::debug("[mqttx] 消费线程退出");
}

}  // namespace mqttx

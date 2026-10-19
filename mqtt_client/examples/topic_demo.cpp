// ---------------------------------------------------------------------------
// mqtt_client 示例：以 topic 为核心的订阅 / 发布
//
//   ./mqttx_topic_demo                    # 离线演示：不连 broker，本地派发
//   ./mqttx_topic_demo tcp://127.0.0.1:1883   # 在线演示：真连 broker 收发
// ---------------------------------------------------------------------------

#include "mqtt_client.hpp"
#include "topic.hpp"

#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

using mqttx::MessageView;
using mqttx::MqttClient;
using mqttx::MqttConfig;

/// 业务对象：只关心“收到消息后做什么”，不关心 MQTT
class RobotService {
public:
    void on_cmd(const MessageView& m) {
        std::cout << "  [cmd]    topic=" << m.topic << " qos=" << m.qos << " payload=" << m.payload
                  << '\n';
        ++cmds_;
    }

    void on_status(const MessageView& m) {
        std::cout << "  [status] topic=" << m.topic << " payload=" << m.payload << '\n';
        ++status_;
    }

    /// const 成员函数同样可以直接绑定
    int cmds() const { return cmds_; }
    int status() const { return status_; }

private:
    int cmds_{0};
    int status_{0};
};

/// Session：把 client 与一组 Topic 组合起来，构造体末尾统一 open()
class RobotSession {
public:
    explicit RobotSession(MqttClient& cli)
        : cmd_{mqttx::make_topic<&RobotService::on_cmd>(cli, "robot/+/cmd", svc_, 1)}
        , status_{mqttx::make_topic<&RobotService::on_status>(cli, "robot/+/status", svc_, 0)}
        , alarm_{mqttx::make_topic(cli, "robot/alarm", [this](const MessageView& m) {
              std::cout << "  [alarm]  payload=" << m.payload << '\n';
              ++alarms_;
          }, 2)} {
        // Topic 构造完成后才注册，避免构造期把 this 交给其它线程
        cmd_.open();
        status_.open();
        alarm_.open();
    }

    RobotSession(const RobotSession&)            = delete;
    RobotSession& operator=(const RobotSession&) = delete;

    int alarms() const { return alarms_; }
    int cmds() const { return svc_.cmds(); }
    int status() const { return svc_.status(); }

private:
    RobotService                     svc_;
    mqttx::Topic<RobotService, &RobotService::on_cmd>    cmd_;
    mqttx::Topic<RobotService, &RobotService::on_status> status_;
    mqttx::TopicFn                                       alarm_;  ///< 可调用对象形式
    int                              alarms_{0};
};

void dump_directory(const MqttClient& cli) {
    std::cout << "  当前订阅集合（topic → qos）:\n";
    for (const auto& s : cli.subscriptions()) std::cout << "    " << s.topic << " → " << s.qos << '\n';
}

/// 构造一条测试消息（借用视图，仅在 dispatch 期间有效）
MessageView msg(std::string_view topic, std::string_view payload, int qos = 1) {
    return MessageView{topic, payload, nullptr, qos, false, mqttx::KeepAlive{}};
}

/// 离线演示：直接向目录注入消息，不需要 broker
void demo_offline() {
    std::cout << "== 离线演示（本地派发，不连接 broker）==\n";

    MqttConfig cfg;
    cfg.broker_url = "tcp://127.0.0.1:1883";
    cfg.client_id  = "mqttx-demo-offline";
    MqttClient cli{cfg};

    RobotSession session{cli};

    dump_directory(cli);

    cli.dispatch("robot/1/cmd", msg("robot/1/cmd", R"({"v":1.2})", 1));
    cli.dispatch("robot/1/status", msg("robot/1/status", "idle", 0));
    cli.dispatch("robot/alarm", msg("robot/alarm", "estop", 2));
    cli.dispatch("robot/9/unknown", msg("robot/9/unknown", "ignored"));

    std::cout << "  统计: cmd=" << session.cmds() << " status=" << session.status()
              << " alarm=" << session.alarms() << " topic数=" << cli.topic_count() << '\n';
}

/// 在线演示：连接 broker，订阅后再发布，最后优雅退出
void demo_online(const std::string& broker_url) {
    std::cout << "\n== 在线演示（broker=" << broker_url << "）==\n";

    MqttConfig cfg;
    cfg.broker_url = broker_url;
    cfg.client_id  = "mqttx-demo-online";
    cfg.qos        = 1;
    MqttClient cli{cfg};

    RobotSession session{cli};

    try {
        cli.start();  // 连接 + 启动消费线程；失败会抛异常
    }
    catch (const std::exception& e) {
        std::cout << "  连接失败（跳过在线演示）: " << e.what() << '\n';
        return;
    }

    cli.publish("robot/7/cmd", R"({"v":0.5})");
    cli.publish("robot/7/status", "busy");
    cli.publish("robot/alarm", "low-battery", 2, /*retain=*/true);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));  // 等消息回环

    std::cout << "  统计: cmd=" << session.cmds() << " status=" << session.status()
              << " alarm=" << session.alarms() << '\n';

    cli.stop();  // 幂等；析构也会调用
    std::cout << "  已断开连接\n";
}

/// 异步演示：handler 把消息副本投递到 worker 线程（模拟线程池）
void demo_async() {
    std::cout << "\n== 异步 handler 演示（线程池 / worker 线程）==\n";

    MqttConfig cfg;
    cfg.broker_url = "tcp://127.0.0.1:1883";
    cfg.client_id  = "mqttx-demo-async";
    MqttClient cli{cfg};

    std::mutex               mtx;
    std::vector<std::thread> workers;
    std::atomic<int>         good{0};

    auto topic = mqttx::make_topic(cli, "robot/+/telemetry", [&](const MessageView& m) {
        // 关键：按值捕获整个 MessageView（含保活凭据）。
        // 派发帧立刻返回，但凭据会一直有效到本任务结束：
        //   - m.topic / m.payload 仍然指向有效的消息数据；
        //   - 该 Topic 在此期间不会被析构（close()/析构会等待）。
        std::lock_guard lk{mtx};
        workers.emplace_back([copy = m, &good] {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));  // 模拟耗时处理
            if (copy.payload == "42") good.fetch_add(1);
        });
    }, 0);

    topic.open();
    cli.dispatch("robot/3/telemetry", msg("robot/3/telemetry", "42", 0));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));  // 让 worker 先跑起来

    const auto t0 = std::chrono::steady_clock::now();
    topic.close();  // 会等待异步任务释放凭据，而不是立刻回收
    const auto waited = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();

    {
        std::lock_guard lk{mtx};
        for (auto& w : workers) w.join();
    }

    std::cout << "  close() 等待了 " << waited
              << " ms（worker 总耗时 200ms），worker 读到正确载荷 " << good.load() << " 次\n";
}

}  // namespace

int main(int argc, char** argv) {
    spdlog::set_level(spdlog::level::info);

    demo_offline();
    demo_async();

    if (argc > 1) demo_online(argv[1]);
    else std::cout << "\n（想跑在线演示：./mqttx_topic_demo tcp://127.0.0.1:1883）\n";

    return 0;
}

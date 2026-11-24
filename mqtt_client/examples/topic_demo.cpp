// ---------------------------------------------------------------------------
// mqtt_client 示例：以 topic 为核心的订阅 / 发布
//
//   ./mqttx_topic_demo                    # 离线演示：不连 broker，本地派发
//   ./mqttx_topic_demo tcp://127.0.0.1:1883   # 在线演示：真连 broker 收发
// ---------------------------------------------------------------------------

#include "binding.hpp"
#include "mqtt_client.hpp"
#include "publisher.hpp"

#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <stdexcept>
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

/// Session：service 先构造，再用 Binding + SubscriptionGroup 批量激活。
///
/// 成员顺序即析构顺序（逆序）：group_ 最后声明 → 最先析构，先等待在飞 handler，
/// 然后才析构 alarms_ 与 svc_，避免异步任务访问已销毁的业务对象。
class RobotSession {
public:
    explicit RobotSession(MqttClient& cli) {
        group_.add(mqttx::make_binding<&RobotService::on_cmd>("robot/+/cmd", svc_, 1));
        group_.add(mqttx::make_binding<&RobotService::on_status>("robot/+/status", svc_, 0));
        group_.add(mqttx::make_binding(
            "robot/alarm",
            [this](const MessageView& m) {
                std::cout << "  [alarm]  payload=" << m.payload << '\n';
                ++alarms_;
            },
            2));

        // 所有成员构造完成后才激活，避免构造期把 this 交给消费线程
        group_.open_all(cli);
    }

    RobotSession(const RobotSession&)            = delete;
    RobotSession& operator=(const RobotSession&) = delete;

    int alarms() const { return alarms_; }
    int cmds() const { return svc_.cmds(); }
    int status() const { return svc_.status(); }

private:
    RobotService             svc_;      ///< 必须先于 group_ 构造、晚于 group_ 析构
    int                      alarms_{0};///< 异步/回调可访问，必须在 group_ 之前声明
    mqttx::SubscriptionGroup group_;    ///< 析构时关闭全部并等待在飞 handler
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

/// 新 API 演示：Binding / Subscription / SubscriptionGroup / Publisher
void demo_bindings() {
    std::cout << "\n== Binding / Subscription / Publisher 演示 ==\n";

    MqttConfig cfg;
    cfg.broker_url = "tcp://127.0.0.1:1883";
    cfg.client_id  = "mqttx-demo-bindings";
    MqttClient cli{cfg};

    RobotService svc;
    int          lambda_hits = 0;
    int          group_hits  = 0;

    // 两步式：描述符可在构造期安全创建，完全构造后再激活
    auto cmd_binding = mqttx::make_binding<&RobotService::on_cmd>("bind/+/cmd", svc, 1);
    auto member      = cmd_binding.open(cli);

    // 一步式：适合已完全构造的对象
    auto lambda = mqttx::subscribe(cli, "bind/lambda",
                                   [&lambda_hits](const MessageView&) { ++lambda_hits; }, 1);

    // Subscription 只移动：move 后源对象失活，注册仍有效
    auto moved_lambda = std::move(lambda);
    std::cout << "  lambda.active=" << lambda.active() << " moved.active=" << moved_lambda.active()
              << '\n';

    // QoS 目录级更新：slot 不变，不产生退订/重订窗口
    (void)member.set_qos(2);

    // 批量订阅：open_all 失败会回滚；析构关闭全部
    mqttx::SubscriptionGroup group;
    group.add(mqttx::make_binding("bind/g1", [&group_hits](const MessageView&) { ++group_hits; }, 0));
    group.add(mqttx::make_binding("bind/g2", [&group_hits](const MessageView&) { ++group_hits; }, 1));
    group.open_all(cli);

    // 发布是独立的值对象；构造期即拒绝 +/#
    mqttx::Publisher pub{cli, "bind/1/cmd", 1};
    std::cout << "  Publisher topic=" << pub.topic() << " qos=" << pub.qos()
              << "（离线模式不实际 publish）\n";

    try {
        mqttx::Publisher bad{cli, "bind/+/cmd"};
        (void)bad;
    }
    catch (const std::invalid_argument& e) {
        std::cout << "  Publisher 拒绝通配符: " << e.what() << '\n';
    }

    cli.dispatch("bind/1/cmd", msg("bind/1/cmd", R"({"v":1})", 1));
    cli.dispatch("bind/lambda", msg("bind/lambda", "x", 1));
    cli.dispatch("bind/g1", msg("bind/g1", "x", 0));
    cli.dispatch("bind/g2", msg("bind/g2", "x", 1));

    std::cout << "  member.cmds=" << svc.cmds() << " lambda=" << lambda_hits
              << " group=" << group_hits
              << " requested_qos=" << member.qos()
              << " effective_qos=" << member.effective_qos() << '\n';
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

    // 发布与订阅解耦：每个具体 topic 一个 Publisher
    mqttx::Publisher cmd_pub{cli, "robot/7/cmd", 1};
    mqttx::Publisher status_pub{cli, "robot/7/status", 0};
    mqttx::Publisher alarm_pub{cli, "robot/alarm", 2, /*retain=*/true};

    cmd_pub.publish(R"({"v":0.5})");
    status_pub.publish("busy");
    alarm_pub.publish("low-battery");

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

    auto sub = mqttx::subscribe(cli, "robot/+/telemetry", [&](const MessageView& m) {
        // 关键：按值捕获整个 MessageView（含保活凭据）。
        // 派发帧立刻返回，但凭据会一直有效到本任务结束：
        //   - m.topic / m.payload 仍然指向有效的消息数据；
        //   - 该 Subscription 在此期间不会被回收（close()/析构会等待）。
        std::lock_guard lk{mtx};
        workers.emplace_back([copy = m, &good] {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));  // 模拟耗时处理
            if (copy.payload == "42") good.fetch_add(1);
        });
    }, 0);

    cli.dispatch("robot/3/telemetry", msg("robot/3/telemetry", "42", 0));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));  // 让 worker 先跑起来

    const auto t0 = std::chrono::steady_clock::now();
    sub.close();  // 会等待异步任务释放凭据，而不是立刻回收
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
    demo_bindings();
    demo_async();

    if (argc > 1) demo_online(argv[1]);
    else std::cout << "\n（想跑在线演示：./mqttx_topic_demo tcp://127.0.0.1:1883）\n";

    return 0;
}

// ---------------------------------------------------------------------------
// MqttClient + Topic<Svc> 单元测试
// 全部用例都不需要真实 broker：订阅注册与消息派发都是本地行为。
// ---------------------------------------------------------------------------

#include "mini_test.hpp"

#include "message.hpp"
#include "mqtt_client.hpp"
#include "topic.hpp"

#include <mqtt/exception.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace mqttx;

namespace {

/// 仅用于构造客户端（测试不会真的发起连接）
MqttConfig test_config() {
    MqttConfig cfg;
    cfg.broker_url = "tcp://127.0.0.1:1883";
    cfg.client_id  = "mqttx-test";
    cfg.qos        = 1;
    return cfg;
}

MessageView make_view(std::string_view topic, std::string_view payload, int qos = 1) {
    return MessageView{.topic = topic, .payload = payload, .props = nullptr, .qos = qos, .retain = false, .keep_alive = {}};
}

/// 业务对象（典型 Session 角色）
struct Robot {
    std::vector<std::string> cmds;
    int                      status_calls{0};
    mutable int              const_calls{0};

    void on_cmd(const MessageView& m) { cmds.emplace_back(m.payload); }
    void on_status(const MessageView&) { ++status_calls; }
    void on_ping(const MessageView&) const { ++const_calls; }
};

}  // namespace

// ---------------------------------------------------------------------------
// MqttClient 基础
// ---------------------------------------------------------------------------

TEST(client_rejects_empty_broker_url) {
    CHECK_THROWS_AS(MqttClient{MqttConfig{}}, std::invalid_argument);
}

TEST(client_generates_client_id_when_empty) {
    MqttConfig cfg;
    cfg.broker_url = "tcp://127.0.0.1:1883";
    MqttClient cli{cfg};

    CHECK_FALSE(cli.config().client_id.empty());
    CHECK(cli.config().client_id.rfind("mqttx-", 0) == 0);
}

TEST(client_starts_empty_and_stop_without_start_is_noop) {
    MqttClient cli{test_config()};

    CHECK_FALSE(cli.is_running());
    CHECK_FALSE(cli.is_connected());
    CHECK_EQ(cli.topic_count(), std::size_t{0});
    CHECK_EQ(cli.handler_count(), std::size_t{0});
    CHECK(cli.subscriptions().empty());

    CHECK_NOTHROW(cli.stop());  // 未启动时 stop 幂等
    CHECK_NOTHROW(cli.stop());
}

TEST(client_install_empty_topic_throws) {
    MqttClient cli{test_config()};
    Robot      robot;

    CHECK_THROWS_AS(cli.install("", &robot, [](void*, const MessageView&) {}), std::invalid_argument);
}

TEST(client_publish_without_connection_throws) {
    MqttClient cli{test_config()};
    CHECK_THROWS_AS(cli.publish("t/x", "payload"), mqtt::exception);
    CHECK_THROWS_AS(cli.publish_binary("t/x", "ab", 2), mqtt::exception);
}

// ---------------------------------------------------------------------------
// topic 目录：注册 / 派发 / 引用计数
// ---------------------------------------------------------------------------

TEST(client_install_and_dispatch_by_topic) {
    MqttClient cli{test_config()};
    Robot      robot;

    const auto slot = cli.install(
        "robot/1/cmd", &robot, [](void* obj, const MessageView& m) {
            static_cast<Robot*>(obj)->on_cmd(m);
        }, -1);

    CHECK(slot != 0);
    CHECK_EQ(cli.topic_count(), std::size_t{1});
    CHECK_EQ(cli.handler_count(), std::size_t{1});

    // qos = -1 → 使用 cfg.qos
    const auto subs = cli.subscriptions();
    CHECK_EQ(subs.size(), std::size_t{1});
    CHECK_EQ(subs.at(0).topic, std::string{"robot/1/cmd"});
    CHECK_EQ(subs.at(0).qos, 1);

    CHECK_EQ(cli.dispatch("robot/1/cmd", make_view("robot/1/cmd", "go")), std::size_t{1});
    CHECK_EQ(robot.cmds.size(), std::size_t{1});
    CHECK_EQ(robot.cmds.at(0), std::string{"go"});

    // 其它 topic 不会串台
    CHECK_EQ(cli.dispatch("robot/2/cmd", make_view("robot/2/cmd", "stop")), std::size_t{0});
    CHECK_EQ(robot.cmds.size(), std::size_t{1});

    cli.uninstall(slot);
    CHECK_EQ(cli.topic_count(), std::size_t{0});
    CHECK_EQ(cli.dispatch("robot/1/cmd", make_view("robot/1/cmd", "x")), std::size_t{0});
}

TEST(client_multicast_two_handlers_same_topic) {
    MqttClient cli{test_config()};
    Robot      a;
    Robot      b;

    const auto sa = cli.install("evt", &a, [](void* o, const MessageView& m) { static_cast<Robot*>(o)->on_cmd(m); }, 1);
    const auto sb = cli.install("evt", &b, [](void* o, const MessageView& m) { static_cast<Robot*>(o)->on_cmd(m); }, 1);

    CHECK(sa != sb);
    CHECK_EQ(cli.topic_count(), std::size_t{1});
    CHECK_EQ(cli.handler_count(), std::size_t{2});
    CHECK_EQ(cli.dispatch("evt", make_view("evt", "ping")), std::size_t{2});
    CHECK_EQ(a.cmds.size(), std::size_t{1});
    CHECK_EQ(b.cmds.size(), std::size_t{1});

    cli.uninstall(sa);
    CHECK_EQ(cli.dispatch("evt", make_view("evt", "ping")), std::size_t{1});
    CHECK_EQ(a.cmds.size(), std::size_t{1});
    CHECK_EQ(b.cmds.size(), std::size_t{2});
}

// ---------------------------------------------------------------------------
// Topic<Svc>：生命周期与 move
// ---------------------------------------------------------------------------

TEST(topic_open_close_and_refcount) {
    MqttClient cli{test_config()};
    Robot      a;
    Robot      b;

    Topic<Robot> ta{cli, "sensor/temp", a, &Robot::on_status, 0};
    Topic<Robot> tb{cli, "sensor/temp", b, &Robot::on_status, 0};

    CHECK_FALSE(ta.opened());
    CHECK_EQ(cli.topic_count(), std::size_t{0});

    CHECK(ta.open());
    CHECK(ta.opened());
    CHECK(ta.open());  // 幂等
    CHECK_EQ(cli.handler_count(), std::size_t{1});

    CHECK(tb.open());
    CHECK_EQ(cli.handler_count(), std::size_t{2});
    CHECK_EQ(cli.topic_count(), std::size_t{1});

    CHECK_EQ(cli.dispatch("sensor/temp", make_view("sensor/temp", "42")), std::size_t{2});
    CHECK_EQ(a.status_calls, 1);
    CHECK_EQ(b.status_calls, 1);

    ta.close();
    CHECK_FALSE(ta.opened());
    CHECK_EQ(cli.topic_count(), std::size_t{1});  // b 还挂着
    ta.close();                                   // 幂等
    CHECK_EQ(cli.dispatch("sensor/temp", make_view("sensor/temp", "43")), std::size_t{1});
    CHECK_EQ(a.status_calls, 1);
    CHECK_EQ(b.status_calls, 2);

    tb.close();
    CHECK_EQ(cli.topic_count(), std::size_t{0});
}

TEST(topic_destructor_uninstalls) {
    MqttClient cli{test_config()};
    Robot      robot;

    {
        Topic<Robot> t{cli, "a/b", robot, &Robot::on_cmd, 1};
        CHECK(t.open());
        CHECK_EQ(cli.topic_count(), std::size_t{1});
    }
    CHECK_EQ(cli.topic_count(), std::size_t{0});
    CHECK_EQ(cli.dispatch("a/b", make_view("a/b", "x")), std::size_t{0});
}

TEST(topic_name_qos_service_accessors) {
    MqttClient cli{test_config()};
    Robot      robot;

    Topic<Robot> t{cli, "a/b", robot, &Robot::on_cmd, 2};
    CHECK_EQ(t.name(), std::string{"a/b"});
    CHECK_EQ(t.qos(), 2);
    CHECK(&t.service() == &robot);
    CHECK(&t.client() == &cli);

    t.set_qos(1);
    CHECK_EQ(t.qos(), 1);
    CHECK(t.open());
    const auto subs = cli.subscriptions();
    CHECK_EQ(subs.at(0).qos, 1);

    t.set_qos(2);  // 已打开：重新注册并提升 QoS
    CHECK(t.opened());
    CHECK_EQ(cli.subscriptions().at(0).qos, 2);
    CHECK_EQ(cli.handler_count(), std::size_t{1});
}

TEST(topic_null_handler_throws) {
    MqttClient cli{test_config()};
    Robot      robot;

    using Method = void (Robot::*)(const MessageView&);
    CHECK_THROWS_AS((Topic<Robot>{cli, "a/b", robot, static_cast<Method>(nullptr), 1}),
                    std::invalid_argument);
}

/// 强制走 move 构造（先 move 到另一个局部对象再返回，避免被 NRVO 优化掉）：
/// 目录里的指针必须在移动后重新绑定到新对象
static Topic<Robot> make_moved_topic(MqttClient& cli, Robot& robot, const std::string& topic) {
    Topic<Robot> tmp{cli, topic, robot, &Robot::on_cmd, 1};
    tmp.open();
    Topic<Robot> moved{std::move(tmp)};
    return moved;
}

TEST(topic_move_ctor_rebinds_registration) {
    MqttClient cli{test_config()};
    Robot      robot;

    Topic<Robot> t = make_moved_topic(cli, robot, "a/b");

    CHECK(t.opened());
    CHECK_EQ(cli.topic_count(), std::size_t{1});
    CHECK_EQ(cli.dispatch("a/b", make_view("a/b", "moved")), std::size_t{1});
    CHECK_EQ(robot.cmds.size(), std::size_t{1});
    CHECK_EQ(robot.cmds.at(0), std::string{"moved"});
}

TEST(topic_move_assignment_rebinds_registration) {
    MqttClient cli{test_config()};
    Robot      a;
    Robot      b;

    Topic<Robot> ta{cli, "a/b", a, &Robot::on_cmd, 1};
    CHECK(ta.open());

    {
        Topic<Robot> tb{cli, "c/d", b, &Robot::on_cmd, 1};
        CHECK(tb.open());
        CHECK_EQ(cli.topic_count(), std::size_t{2});

        ta = std::move(tb);  // ta 先 close 自己，再接管 tb 的注册
        CHECK_EQ(cli.topic_count(), std::size_t{1});
        CHECK(cli.subscriptions().at(0).topic == "c/d");
    }
    CHECK(ta.opened());
    CHECK_EQ(cli.dispatch("c/d", make_view("c/d", "assigned")), std::size_t{1});
    CHECK_EQ(b.cmds.size(), std::size_t{1});
    CHECK_EQ(cli.dispatch("a/b", make_view("a/b", "old")), std::size_t{0});  // 旧注册已释放
}

TEST(topic_moved_from_object_is_inert) {
    MqttClient cli{test_config()};
    Robot      robot;

    Topic<Robot> t1{cli, "a/b", robot, &Robot::on_cmd, 1};
    CHECK(t1.open());
    CHECK_EQ(cli.topic_count(), std::size_t{1});

    Topic<Robot> t2{std::move(t1)};
    CHECK_FALSE(t1.opened());          // 移动源不再持有注册
    CHECK_THROWS_AS(t1.publish("x"), std::logic_error);
    CHECK_THROWS_AS(t1.client(), std::logic_error);
    CHECK_NOTHROW(t1.close());         // 幂等且不碰空指针

    CHECK_EQ(cli.topic_count(), std::size_t{1});  // 注册仍归 t2
    CHECK_EQ(cli.dispatch("a/b", make_view("a/b", "x")), std::size_t{1});
    CHECK_EQ(robot.cmds.size(), std::size_t{1});
}

// ---------------------------------------------------------------------------
// Topic：const 成员函数 / 可调用对象
// ---------------------------------------------------------------------------

TEST(topic_supports_const_member_function) {
    MqttClient cli{test_config()};
    Robot      robot;

    Topic<Robot> t{cli, "a/ping", robot, &Robot::on_ping, 1};
    CHECK(t.open());
    CHECK_EQ(cli.dispatch("a/ping", make_view("a/ping", "?")), std::size_t{1});
    CHECK_EQ(robot.const_calls, 1);
}

TEST(topic_supports_callable_lambda) {
    MqttClient cli{test_config()};
    int        hits = 0;
    std::string last;

    auto t = make_topic(cli, "a/lambda", [&](const MessageView& m) {
        ++hits;
        last.assign(m.payload);
    }, 1);

    CHECK(t.open());
    CHECK_EQ(cli.dispatch("a/lambda", make_view("a/lambda", "hi")), std::size_t{1});
    CHECK_EQ(hits, 1);
    CHECK_EQ(last, std::string{"hi"});

    // 可调用对象模式同样支持 move（重新绑定 + 迁移自持有的 std::function）
    auto t2 = std::move(t);
    CHECK_EQ(cli.dispatch("a/lambda", make_view("a/lambda", "again")), std::size_t{1});
    CHECK_EQ(hits, 2);
    CHECK_EQ(cli.handler_count(), std::size_t{1});
}

// ---------------------------------------------------------------------------
// 异步 handler：调用帧已返回，异步侧仍持有消息副本
// ---------------------------------------------------------------------------

TEST(topic_close_waits_for_async_handler_copy) {
    MqttClient cli{test_config()};

    struct AsyncSink {
        MessageView view;  // 模拟线程池任务持有的副本
    } async;

    auto topic = std::make_unique<TopicFn>(make_topic(cli, "a/async", [&async](const MessageView& m) {
        async.view = m;  // 按值捕获：凭据随副本存活
    }, 1));
    topic->open();
    CHECK_EQ(cli.topic_count(), std::size_t{1});

    // 与消费线程一致：视图借用真实 paho 消息，凭据持有该消息
    auto msg = mqtt::make_message("a/async", std::string{"async-payload"}, 1, false);
    {
        const MessageView mv{.topic      = msg->get_topic(),
                             .payload    = msg->get_payload_str(),
                             .props      = &msg->get_properties(),
                             .qos        = 1,
                             .retain     = false,
                             .keep_alive = KeepAlive{std::shared_ptr<const void>{msg}}};
        CHECK_EQ(cli.dispatch("a/async", mv), std::size_t{1});
    }
    msg.reset();  // 调用方与派发帧都已放手，只剩异步副本的凭据

    // 异步侧读到的数据仍然有效（不会悬垂）
    CHECK_EQ(async.view.payload, std::string_view{"async-payload"});
    CHECK(async.view.props != nullptr);
    CHECK(async.view.keep_alive.valid());

    // 析构 Topic（= 摘除登记项）必须等待异步副本释放
    std::atomic<bool> destroyed{false};
    std::thread       destroyer([&] {
        topic.reset();
        destroyed.store(true, std::memory_order_release);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    CHECK_FALSE(destroyed.load());  // 还在等异步任务

    async.view = {};  // 异步任务完成
    destroyer.join();
    CHECK(destroyed.load());
    CHECK_EQ(cli.topic_count(), std::size_t{0});
}

TEST(sync_handler_leaves_nothing_to_wait_for) {
    // 同步 handler 不保留副本：析构 Topic 时不应有任何额外等待
    MqttClient cli{test_config()};
    Robot      robot;

    {
        Topic<Robot> t{cli, "a/sync", robot, &Robot::on_cmd, 1};
        CHECK(t.open());
        CHECK_EQ(cli.topic_count(), std::size_t{1});
        CHECK_EQ(cli.dispatch("a/sync", make_view("a/sync", "x")), std::size_t{1});
    }
    CHECK_EQ(cli.topic_count(), std::size_t{0});
    CHECK_EQ(robot.cmds.size(), std::size_t{1});
}

// ---------------------------------------------------------------------------
// MessageView
// ---------------------------------------------------------------------------

TEST(message_view_deep_copy_outlives_source) {
    MessageView::Owned owned;
    {
        const std::string topic   = "a/b";
        const std::string payload = "payload";
        MessageView       mv{.topic = topic, .payload = payload, .props = nullptr, .qos = 2, .retain = true, .keep_alive = {}};

        owned = mv.deep_copy();
    }

    CHECK_EQ(owned.topic, std::string{"a/b"});
    CHECK_EQ(owned.payload, std::string{"payload"});
    CHECK_EQ(owned.qos, 2);
    CHECK(owned.retain);

    const MessageView back = owned.view();
    CHECK_EQ(back.topic, std::string_view{"a/b"});
    CHECK_EQ(back.payload, std::string_view{"payload"});
    CHECK(back.props != nullptr);
}

// ---------------------------------------------------------------------------
// Binding / Subscription 单元测试：描述与激活分离、RAII 退订、move 语义
// ---------------------------------------------------------------------------

#include "mini_test.hpp"

#include "binding.hpp"
#include "mqtt_client.hpp"

#include <stdexcept>
#include <string>
#include <utility>

using namespace mqttx;

namespace {

MqttConfig test_config() {
    MqttConfig cfg;
    cfg.broker_url = "tcp://127.0.0.1:1883";
    cfg.client_id  = "mqttx-binding-test";
    cfg.qos        = 1;
    return cfg;
}

MessageView make_view(std::string_view topic, std::string_view payload, int qos = 1) {
    return MessageView{.topic      = topic,
                       .payload    = payload,
                       .props      = nullptr,
                       .qos        = qos,
                       .retain     = false,
                       .keep_alive = {}};
}

struct Sink {
    int         count{0};
    std::string payload;

    void on(const MessageView& msg) {
        ++count;
        payload.assign(msg.payload);
    }
};

Handler sink_handler(Sink& s) { return Handler::member<Sink, &Sink::on>(s); }

}  // namespace

TEST(binding_validates_arguments) {
    Sink s;

    CHECK_THROWS_AS(Binding("", sink_handler(s), 1), std::invalid_argument);
    CHECK_THROWS_AS(Binding("t", Handler{}, 1), std::invalid_argument);
    CHECK_THROWS_AS(Binding("t", sink_handler(s), 3), std::invalid_argument);
    CHECK_THROWS_AS(Binding("t", sink_handler(s), -2), std::invalid_argument);
    CHECK_NOTHROW(Binding("t", sink_handler(s), -1));
    CHECK_NOTHROW(Binding("t", sink_handler(s), 2));
}

TEST(binding_open_returns_raii_subscription) {
    MqttClient cli{test_config()};
    Sink       s;

    const Binding b{"t/raii", sink_handler(s), 1};

    {
        auto sub = b.open(cli);
        CHECK(sub.active());
        CHECK_EQ(sub.filter(), std::string_view{"t/raii"});
        CHECK_EQ(sub.qos(), 1);
        CHECK_EQ(cli.topic_count(), std::size_t{1});
        CHECK_EQ(cli.dispatch("t/raii", make_view("t/raii", "hello")), std::size_t{1});
        CHECK_EQ(s.count, 1);

        sub.close();
        CHECK_FALSE(sub.active());
        sub.close();  // 幂等
        CHECK_EQ(cli.topic_count(), std::size_t{0});
    }
    CHECK_EQ(cli.dispatch("t/raii", make_view("t/raii", "again")), std::size_t{0});
    CHECK_EQ(s.count, 1);
}

TEST(subscription_move_preserves_registration) {
    MqttClient cli{test_config()};
    Sink       s;

    const Binding b{"t/move", sink_handler(s), 1};

    auto sub   = b.open(cli);
    auto moved = std::move(sub);

    CHECK_FALSE(sub.active());
    CHECK(moved.active());
    CHECK_EQ(cli.topic_count(), std::size_t{1});
    CHECK_EQ(cli.dispatch("t/move", make_view("t/move", "moved")), std::size_t{1});
    CHECK_EQ(s.count, 1);

    // 移动源析构不能退订
    sub = std::move(moved);
    CHECK(sub.active());
    CHECK_EQ(cli.topic_count(), std::size_t{1});
    CHECK_EQ(cli.dispatch("t/move", make_view("t/move", "back")), std::size_t{1});
}

TEST(binding_can_open_multiple_subscriptions) {
    MqttClient cli{test_config()};
    Sink       s;

    const Binding b{"t/multi", sink_handler(s), 1};

    auto a = b.open(cli);
    auto c = b.open(cli);

    CHECK_EQ(cli.handler_count(), std::size_t{2});
    CHECK_EQ(cli.dispatch("t/multi", make_view("t/multi", "x")), std::size_t{2});
    CHECK_EQ(s.count, 2);

    a.close();
    CHECK_EQ(cli.handler_count(), std::size_t{1});
    CHECK_EQ(cli.dispatch("t/multi", make_view("t/multi", "y")), std::size_t{1});
    CHECK_EQ(s.count, 3);
}

TEST(subscription_destructor_unsubscribes) {
    MqttClient cli{test_config()};
    Sink       s;

    const Binding b{"t/dtor", sink_handler(s), 1};
    {
        auto sub = b.open(cli);
        CHECK_EQ(cli.topic_count(), std::size_t{1});
    }
    CHECK_EQ(cli.topic_count(), std::size_t{0});
}

TEST(binding_factories_and_subscribe) {
    MqttClient cli{test_config()};
    Sink       a;
    Sink       b;
    Sink       c;

    auto b1 = make_binding<&Sink::on>("f/member", a, 1);
    CHECK_EQ(b1.filter(), std::string{"f/member"});
    CHECK_EQ(b1.qos(), 1);

    auto s1 = b1.open(cli);
    auto s2 = subscribe(cli, "f/lambda", [&b](const MessageView& m) { b.on(m); }, 1);
    auto s3 = subscribe(cli, "f/handler", on<&Sink::on>(c), 1);

    CHECK(s1.active());
    CHECK(s2.active());
    CHECK(s3.active());
    CHECK_EQ(cli.handler_count(), std::size_t{3});

    CHECK_EQ(cli.dispatch("f/member", make_view("f/member", "1")), std::size_t{1});
    CHECK_EQ(cli.dispatch("f/lambda", make_view("f/lambda", "2")), std::size_t{1});
    CHECK_EQ(cli.dispatch("f/handler", make_view("f/handler", "3")), std::size_t{1});
    CHECK_EQ(a.count, 1);
    CHECK_EQ(b.count, 1);
    CHECK_EQ(c.count, 1);

    const auto callable = [](const MessageView&) {};
    static_assert(MessageHandler<decltype(callable)>);
}

TEST(subscription_effective_qos_and_set_qos) {
    MqttClient cli{test_config()};
    Sink       a;
    Sink       b;

    auto sub = Binding{"q/a", sink_handler(a), -1}.open(cli);
    CHECK_EQ(sub.qos(), -1);
    CHECK_EQ(sub.effective_qos(), 1);  // cfg.qos

    CHECK(sub.set_qos(2));
    CHECK_EQ(sub.qos(), 2);
    CHECK_EQ(sub.effective_qos(), 2);
    CHECK_EQ(cli.subscriptions().at(0).qos, 2);
    CHECK_EQ(cli.handler_count(), std::size_t{1});  // slot 未变

    auto sub2 = Binding{"q/a", sink_handler(b), 0}.open(cli);
    CHECK_EQ(cli.handler_count(), std::size_t{2});

    CHECK(sub.set_qos(0));
    CHECK_EQ(sub.effective_qos(), 0);
    CHECK_EQ(cli.subscriptions().at(0).qos, 0);

    sub.close();
    CHECK_THROWS_AS(sub.set_qos(1), std::logic_error);
    CHECK_EQ(sub2.effective_qos(), 0);
}

TEST(subscription_group_opens_and_closes_atomically) {
    MqttClient cli{test_config()};
    Sink       a;
    Sink       b;

    {
        SubscriptionGroup group;
        group.add(make_binding<&Sink::on>("g/a", a, 1));
        group.add(make_binding<&Sink::on>("g/b", b, 0));
        CHECK_EQ(group.size(), std::size_t{2});
        CHECK_FALSE(group.opened());

        group.open_all(cli);
        CHECK(group.opened());
        CHECK_EQ(cli.topic_count(), std::size_t{2});
        CHECK_EQ(cli.dispatch("g/a", make_view("g/a", "1")), std::size_t{1});
        CHECK_EQ(cli.dispatch("g/b", make_view("g/b", "2")), std::size_t{1});
        CHECK_EQ(a.count, 1);
        CHECK_EQ(b.count, 1);

        CHECK_THROWS_AS(group.add(make_binding("g/c", Handler::callable([](const MessageView&) {}), 1)),
                        std::logic_error);

        group.close_all();
        CHECK_FALSE(group.opened());
        CHECK_EQ(cli.topic_count(), std::size_t{0});

        group.open_all(cli);  // 可重新打开全部描述符
        CHECK_EQ(cli.topic_count(), std::size_t{2});
        group.open_all(cli);  // 幂等
        CHECK_EQ(cli.topic_count(), std::size_t{2});
    }
    CHECK_EQ(cli.topic_count(), std::size_t{0});  // 析构关闭全部
}

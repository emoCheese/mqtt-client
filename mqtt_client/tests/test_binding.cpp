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

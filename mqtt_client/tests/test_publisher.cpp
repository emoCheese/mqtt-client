// ---------------------------------------------------------------------------
// Publisher 单元测试：发布与绑定解耦、通配符守卫
// ---------------------------------------------------------------------------

#include "mini_test.hpp"

#include "mqtt_client.hpp"
#include "publisher.hpp"
#include "topic.hpp"

#include <mqtt/exception.h>

#include <stdexcept>
#include <string>

using namespace mqttx;

namespace {

MqttConfig test_config() {
    MqttConfig cfg;
    cfg.broker_url = "tcp://127.0.0.1:1883";
    cfg.client_id  = "mqttx-publisher-test";
    cfg.qos        = 1;
    return cfg;
}

struct Robot {
    void on(const MessageView&) {}
};

}  // namespace

TEST(publisher_validates_topic_and_qos) {
    MqttClient cli{test_config()};

    CHECK_THROWS_AS(Publisher(cli, ""), std::invalid_argument);
    CHECK_THROWS_AS(Publisher(cli, "robot/+/cmd"), std::invalid_argument);
    CHECK_THROWS_AS(Publisher(cli, "robot/#"), std::invalid_argument);
    CHECK_THROWS_AS(Publisher(cli, "robot/1/cmd", 3), std::invalid_argument);
    CHECK_THROWS_AS(Publisher(cli, "robot/1/cmd", -2), std::invalid_argument);
    CHECK_NOTHROW(Publisher(cli, "robot/1/cmd", 2));
}

TEST(publisher_accessors) {
    MqttClient cli{test_config()};
    Publisher  pub{cli, "robot/1/cmd", 2, true};

    CHECK_EQ(pub.topic(), std::string{"robot/1/cmd"});
    CHECK_EQ(pub.qos(), 2);
    CHECK(pub.retain());
}

TEST(publisher_publish_without_connection_throws) {
    MqttClient cli{test_config()};
    Publisher  pub{cli, "robot/1/cmd"};

    CHECK_THROWS_AS(pub.publish("payload"), mqtt::exception);
    CHECK_THROWS_AS(pub.publish_binary("ab", 2), mqtt::exception);
}

TEST(topic_publish_rejects_wildcard_filter) {
    MqttClient cli{test_config()};
    Robot      robot;

    Topic<Robot, &Robot::on> filter{cli, "robot/+/cmd", robot, 1};

    CHECK_THROWS_AS(filter.publish("x"), std::invalid_argument);
    CHECK_THROWS_AS(filter.publish_binary("x", 1), std::invalid_argument);

    Topic<Robot, &Robot::on> exact{cli, "robot/1/cmd", robot, 1};
    CHECK_THROWS_AS(exact.publish("x"), mqtt::exception);  // 未连接，而不是通配符错误
}

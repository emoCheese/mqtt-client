// ---------------------------------------------------------------------------
// MQTT topic filter 匹配规则测试（对照 MQTT 3.1.1 / 5.0 §4.7 的规范用例）
// ---------------------------------------------------------------------------

#include "mini_test.hpp"
#include "topic_filter.hpp"

#include <string_view>

using mqttx::is_wildcard_filter;
using mqttx::topic_matches_filter;

namespace {

constexpr bool m(std::string_view topic, std::string_view filter) {
    return topic_matches_filter(topic, filter);
}

}  // namespace

TEST(filter_exact_match_requires_same_levels) {
    CHECK(m("sport/tennis/player1", "sport/tennis/player1"));
    CHECK_FALSE(m("sport/tennis/player1", "sport/tennis/player2"));
    CHECK_FALSE(m("sport/tennis", "sport"));
    CHECK_FALSE(m("sport", "sport/tennis"));
    CHECK(m("sport", "sport"));
    CHECK_FALSE(m("sport/tennis", "sport/tennis/"));
}

TEST(filter_single_level_wildcard) {
    CHECK(m("sport/tennis/player1", "sport/tennis/+"));
    CHECK(m("sport/tennis/player2", "sport/tennis/+"));
    CHECK_FALSE(m("sport/tennis/player1/ranking", "sport/tennis/+"));

    // 规范用例："sport/+" 不匹配 "sport"，但匹配 "sport/"
    CHECK_FALSE(m("sport", "sport/+"));
    CHECK(m("sport/", "sport/+"));

    // 多级 '+' 与空级
    CHECK(m("a/b/c", "a/+/c"));
    CHECK(m("a/b/c", "a/+/+"));
    CHECK(m("a//c", "a/+/c"));
    CHECK_FALSE(m("a/b/c", "a/+"));
}

TEST(filter_multi_level_wildcard) {
    // 规范用例
    CHECK(m("sport/tennis/player1/ranking", "sport/tennis/player1/#"));
    CHECK(m("sport/tennis/player1", "sport/tennis/player1/#"));
    CHECK(m("sport", "sport/#"));
    CHECK(m("sport/tennis", "sport/#"));
    CHECK(m("any/thing/at/all", "#"));
    CHECK(m("sport", "#"));

    // '#' 必须是最后一级，否则永不匹配
    CHECK_FALSE(m("a/b/c", "a/#/c"));
    CHECK_FALSE(m("sport/tennis", "sport/+/#/x"));
}

TEST(filter_dollar_topics_are_not_matched_by_leading_wildcard) {
    // [MQTT-4.7.2-1]
    CHECK_FALSE(m("$SYS/broker/uptime", "#"));
    CHECK_FALSE(m("$SYS/broker/uptime", "+/broker/uptime"));
    CHECK(m("$SYS/broker/uptime", "$SYS/#"));
    CHECK(m("$SYS/broker/uptime", "$SYS/broker/+"));
}

TEST(filter_is_wildcard_filter_detection) {
    CHECK_FALSE(is_wildcard_filter("sport/tennis"));
    CHECK(is_wildcard_filter("sport/+"));
    CHECK(is_wildcard_filter("#"));
    CHECK(is_wildcard_filter("a/#/b"));
}

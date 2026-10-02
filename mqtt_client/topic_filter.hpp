#pragma once

// ---------------------------------------------------------------------------
// MQTT topic filter 匹配（MQTT 3.1.1 / 5.0 §4.7）
//
// 订阅侧用的是 topic filter（可含 '+' / '#'），而消息到达时用的是具体 topic，
// 因此目录派发必须按 filter 规则匹配，而不是简单的字符串相等。
// ---------------------------------------------------------------------------

#include <cstddef>
#include <string_view>

namespace mqttx {

/// filter 是否包含通配符（'+' / '#'）
[[nodiscard]] constexpr bool is_wildcard_filter(std::string_view filter) noexcept {
    return filter.find('+') != std::string_view::npos || filter.find('#') != std::string_view::npos;
}

/// 具体 topic 是否命中 filter。
///
/// 规则：
///   - '+'  匹配且仅匹配一级（可以是空级："a/+" 命中 "a/"，但不命中 "a"）；
///   - '#'  匹配父级及其之后的所有级（"a/#" 命中 "a"、"a/b"、"a/b/c"），且只能是最后一级；
///   - 以 '$' 开头的 topic 不被以通配符开头的 filter 命中（[MQTT-4.7.2-1]）；
///   - 级数必须一致（除 '#' 覆盖的部分外）。
[[nodiscard]] constexpr bool topic_matches_filter(std::string_view topic,
                                                  std::string_view filter) noexcept {
    using sv            = std::string_view;
    constexpr auto npos = sv::npos;

    // [MQTT-4.7.2-1] 通配符 filter 不得命中 '$' 开头的系统 topic
    if (!topic.empty() && topic.front() == '$' && !filter.empty() &&
        (filter.front() == '+' || filter.front() == '#'))
        return false;

    std::size_t ti = 0;
    std::size_t fi = 0;

    while (true) {
        const auto t_slash = topic.find('/', ti);
        const auto f_slash = filter.find('/', fi);

        const sv t_seg = topic.substr(ti, t_slash == npos ? npos : t_slash - ti);
        const sv f_seg = filter.substr(fi, f_slash == npos ? npos : f_slash - fi);

        // '#' 必须是最后一级，命中当前及之后的所有级
        if (f_seg == "#") return f_slash == npos;

        // 普通级必须完全相等，'+' 命中任意一级
        if (f_seg != "+" && f_seg != t_seg) return false;

        const bool t_done = (t_slash == npos);
        const bool f_done = (f_slash == npos);

        if (t_done && f_done) return true;
        if (t_done) return filter.substr(f_slash) == "/#";  // "a/#" 命中 "a"
        if (f_done) return false;                           // filter 用尽但 topic 还有级

        ti = t_slash + 1;
        fi = f_slash + 1;
    }
}

}  // namespace mqttx

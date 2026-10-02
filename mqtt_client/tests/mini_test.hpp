#pragma once

// ---------------------------------------------------------------------------
// 极简测试框架（不引入外部依赖）
//   TEST(name) { CHECK(...); CHECK_EQ(...); ... }
//   test_main.cpp 中的 main() 调用 minitest::run_all() 执行全部用例。
// ---------------------------------------------------------------------------

#include <exception>
#include <functional>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace minitest {

struct Case {
    std::string           name;
    std::function<void()> fn;
};

inline std::vector<Case>& cases() {
    static std::vector<Case> v;
    return v;
}

struct Registrar {
    Registrar(std::string name, std::function<void()> fn) {
        cases().push_back(Case{std::move(name), std::move(fn)});
    }
};

struct Failure : std::runtime_error {
    using std::runtime_error::runtime_error;
};

inline int& failure_count() {
    static int n = 0;
    return n;
}

inline int& check_count() {
    static int n = 0;
    return n;
}

inline void report_failure(const char* file, int line, const std::string& msg) {
    ++failure_count();
    std::cout << "    [FAIL] " << file << ':' << line << "  " << msg << '\n';
}

/// 把任意可打印值转成字符串（不可打印时给出占位符）
template <typename T>
std::string to_str(const T& v) {
    if constexpr (requires(std::ostringstream& os, const T& x) { os << x; }) {
        std::ostringstream os;
        os << v;
        return os.str();
    }
    else {
        return "<不可打印>";
    }
}

int run_all(const char* suite);

}  // namespace minitest

#define MQTTX_TEST_CONCAT_(a, b) a##b
#define MQTTX_TEST_CONCAT(a, b)  MQTTX_TEST_CONCAT_(a, b)

/// 定义一个测试用例
#define TEST(name)                                                              \
    static void MQTTX_TEST_CONCAT(mqttx_case_, name)();                         \
    static const ::minitest::Registrar MQTTX_TEST_CONCAT(mqttx_reg_, name){     \
        #name, &MQTTX_TEST_CONCAT(mqttx_case_, name)};                          \
    static void MQTTX_TEST_CONCAT(mqttx_case_, name)()

#define MQTTX_CHECK_IMPL(cond, text)                                            \
    do {                                                                        \
        ++::minitest::check_count();                                            \
        if (!(cond)) ::minitest::report_failure(__FILE__, __LINE__, (text));    \
    } while (false)

#define CHECK(cond) MQTTX_CHECK_IMPL((cond), "CHECK 失败: " #cond)

#define CHECK_FALSE(cond) MQTTX_CHECK_IMPL(!(cond), "CHECK_FALSE 失败: " #cond)

#define CHECK_EQ(a, b)                                                          \
    do {                                                                        \
        ++::minitest::check_count();                                            \
        /* 按值取：避免绑定到临时对象的子对象（如 f().at(0).x）造成悬垂引用 */    \
        const auto mqttx_lhs = (a);                                             \
        const auto mqttx_rhs = (b);                                             \
        if (!(mqttx_lhs == mqttx_rhs)) {                                        \
            ::minitest::report_failure(                                         \
                __FILE__, __LINE__,                                             \
                std::string{"CHECK_EQ 失败: " #a " == " #b " (左="} +            \
                    ::minitest::to_str(mqttx_lhs) + ", 右=" +                   \
                    ::minitest::to_str(mqttx_rhs) + ")");                       \
        }                                                                       \
    } while (false)

#define CHECK_THROWS_AS(expr, extype)                                           \
    do {                                                                        \
        ++::minitest::check_count();                                            \
        bool mqttx_thrown = false;                                              \
        try {                                                                   \
            (void)(expr);                                                       \
        }                                                                       \
        catch (const extype&) {                                                 \
            mqttx_thrown = true;                                                \
        }                                                                       \
        catch (...) {                                                           \
            ::minitest::report_failure(__FILE__, __LINE__,                      \
                                       "CHECK_THROWS_AS: 抛出了其它类型异常: " #expr); \
            mqttx_thrown = true;                                                \
        }                                                                       \
        if (!mqttx_thrown)                                                      \
            ::minitest::report_failure(__FILE__, __LINE__,                      \
                                       "CHECK_THROWS_AS: 未抛出异常: " #expr);  \
    } while (false)

#define CHECK_NOTHROW(expr)                                                     \
    do {                                                                        \
        ++::minitest::check_count();                                            \
        try {                                                                   \
            (void)(expr);                                                       \
        }                                                                       \
        catch (const std::exception& e) {                                       \
            ::minitest::report_failure(__FILE__, __LINE__,                      \
                                       std::string{"CHECK_NOTHROW: 抛出异常: "} + e.what()); \
        }                                                                       \
        catch (...) {                                                           \
            ::minitest::report_failure(__FILE__, __LINE__,                      \
                                       "CHECK_NOTHROW: 抛出未知异常: " #expr);  \
        }                                                                       \
    } while (false)

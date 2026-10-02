#include "mini_test.hpp"

#include <exception>
#include <iostream>

namespace minitest {

int run_all(const char* suite) {
    std::cout << "===== " << suite << " =====" << std::endl;

    int failed = 0;
    for (const auto& c : cases()) {
        const int before = failure_count();
        std::cout << "[ RUN  ] " << c.name << std::endl;
        try {
            c.fn();
        }
        catch (const std::exception& e) {
            report_failure(__FILE__, __LINE__, std::string{"用例抛出未捕获异常: "} + e.what());
        }
        catch (...) {
            report_failure(__FILE__, __LINE__, "用例抛出未捕获的未知异常");
        }

        if (failure_count() == before) {
            std::cout << "[  OK  ] " << c.name << std::endl;
        }
        else {
            ++failed;
            std::cout << "[ FAIL ] " << c.name << std::endl;
        }
    }

    std::cout << "-----------------------------------------" << std::endl;
    std::cout << "用例: " << cases().size() << "  断言: " << check_count()
              << "  失败用例: " << failed << "  失败断言: " << failure_count() << std::endl;

    if (cases().empty()) {
        std::cout << "没有任何用例被执行" << std::endl;
        return 2;
    }
    return failed == 0 ? 0 : 1;
}

}  // namespace minitest

int main() { return minitest::run_all("mqtt_client 单元测试"); }

# 单元测试（自带极简测试框架，不依赖外部测试库）
add_executable(mqttx_tests
    test_main.cpp
    test_topic_filter.cpp
    test_topic_directory.cpp
    test_mqtt_client.cpp
    mini_test.hpp
)

target_link_libraries(mqttx_tests PRIVATE mqttx::mqtt_client)

# 端到端测试：连本仓库自带的 mock broker（tests/integration/mock_broker.py）
add_executable(mqttx_online_test integration/test_online.cpp)
target_link_libraries(mqttx_online_test PRIVATE mqttx::mqtt_client)

if(NOT MSVC)
    target_compile_options(mqttx_tests PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(mqttx_online_test PRIVATE -Wall -Wextra -Wpedantic)
endif()

add_test(NAME mqttx_tests COMMAND mqttx_tests)
# 出现死锁时由 ctest 兜底超时，而不是永久挂住
set_tests_properties(mqttx_tests PROPERTIES TIMEOUT 120)

find_package(Python3 COMPONENTS Interpreter QUIET)
if(Python3_Interpreter_FOUND)
    add_test(NAME mqttx_online
        COMMAND ${Python3_EXECUTABLE}
                ${CMAKE_CURRENT_SOURCE_DIR}/integration/run_online_test.py
                $<TARGET_FILE:mqttx_online_test>
                ${CMAKE_CURRENT_SOURCE_DIR}/integration/mock_broker.py)
    set_tests_properties(mqttx_online PROPERTIES TIMEOUT 90)
else()
    message(STATUS "mqtt_client: 未找到 python3，跳过 mqttx_online 端到端测试")
endif()

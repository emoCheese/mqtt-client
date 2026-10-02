# 示例：以 topic 为核心的用法演示
add_executable(mqttx_topic_demo topic_demo.cpp)
target_link_libraries(mqttx_topic_demo PRIVATE mqttx::mqtt_client cpp_3rdparty::spdlog)

if(NOT MSVC)
    target_compile_options(mqttx_topic_demo PRIVATE -Wall -Wextra -Wpedantic)
endif()

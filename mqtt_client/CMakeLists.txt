# ---------------------------------------------------------------------------
# mqtt_client —— 以 topic 为核心的 MQTT 客户端
# ---------------------------------------------------------------------------

find_package(cpp_3rdparty REQUIRED)
find_package(Threads REQUIRED)

add_library(mqtt_client STATIC
    mqtt_client.cpp
    topic_directory.cpp
    mqtt_client.hpp
    topic_directory.hpp
    topic.hpp
    message.hpp
    imqtt_client.h
    RpcTopic.hpp
)

add_library(mqttx::mqtt_client ALIAS mqtt_client)

target_include_directories(mqtt_client
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

# 公共头文件直接暴露 mqtt:: 类型（async_client / delivery_token），
# 因此 paho 必须作为 PUBLIC 依赖传递出去。
target_link_libraries(mqtt_client
    PUBLIC  cpp_3rdparty::paho_mqtt_cpp
    PRIVATE Threads::Threads cpp_3rdparty::spdlog
)

target_compile_features(mqtt_client PUBLIC cxx_std_20)
set_target_properties(mqtt_client PROPERTIES POSITION_INDEPENDENT_CODE ON)

if(MSVC)
    target_compile_options(mqtt_client PRIVATE /W4 /permissive-)
else()
    target_compile_options(mqtt_client PRIVATE
        -Wall -Wextra -Wpedantic -Wshadow -Wnon-virtual-dtor -Woverloaded-virtual)
endif()

# --- sanitizer（可选）-------------------------------------------------------
#   cmake -DMQTTX_SANITIZERS=address,undefined ..   # 内存/未定义行为
#   cmake -DMQTTX_SANITIZERS=thread ..              # 数据竞争
set(MQTTX_SANITIZERS "" CACHE STRING "为 mqtt_client 及其测试/示例启用 sanitizer")
if(MQTTX_SANITIZERS)
    message(STATUS "mqtt_client: sanitizers = ${MQTTX_SANITIZERS}")
    add_compile_options(-fsanitize=${MQTTX_SANITIZERS} -fno-omit-frame-pointer)
    add_link_options(-fsanitize=${MQTTX_SANITIZERS})
endif()

option(MQTTX_BUILD_TESTS "构建 mqtt_client 单元测试" ON)
option(MQTTX_BUILD_EXAMPLES "构建 mqtt_client 示例" ON)

if(MQTTX_BUILD_TESTS)
    add_subdirectory(tests)
endif()

if(MQTTX_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

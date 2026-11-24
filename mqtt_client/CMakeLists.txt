# ---------------------------------------------------------------------------
# mqtt_client —— 以 topic 为核心的 MQTT 客户端
# ---------------------------------------------------------------------------

find_package(spdlog CONFIG REQUIRED)
find_package(PahoMqttCpp CONFIG REQUIRED)
find_package(Threads REQUIRED)

# paho.mqtt.cpp 不同分发方式导出的目标名不一样：
#   - vcpkg（当前使用）：PahoMqttCpp::paho-mqttpp3
#   - 手工构建的 cpp_3rdparty 安装：PahoMqttCpp::paho_mqtt_cpp
if(TARGET PahoMqttCpp::paho-mqttpp3)
    set(MQTTX_PAHO_TARGET PahoMqttCpp::paho-mqttpp3)
elseif(TARGET PahoMqttCpp::paho_mqtt_cpp)
    set(MQTTX_PAHO_TARGET PahoMqttCpp::paho_mqtt_cpp)
else()
    message(FATAL_ERROR
        "find_package(PahoMqttCpp) 已成功，但没有找到可用的导入目标；\n"
        "期望 PahoMqttCpp::paho-mqttpp3（vcpkg）或 PahoMqttCpp::paho_mqtt_cpp。")
endif()

add_library(mqtt_client STATIC
    mqtt_client.cpp
    topic_directory.cpp
    mqtt_client.hpp
    topic_directory.hpp
    topic.hpp
    handler.hpp
    binding.hpp
    publisher.hpp
    message.hpp
    imqtt_client.h
)

add_library(mqttx::mqtt_client ALIAS mqtt_client)

target_include_directories(mqtt_client
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

# 公共头文件直接暴露 mqtt:: 类型（async_client / delivery_token），
# 因此 paho 必须作为 PUBLIC 依赖传递出去。
target_link_libraries(mqtt_client
    PUBLIC  ${MQTTX_PAHO_TARGET}
    PRIVATE Threads::Threads spdlog::spdlog
)

target_compile_features(mqtt_client PUBLIC cxx_std_20)
set_target_properties(mqtt_client PROPERTIES POSITION_INDEPENDENT_CODE ON)

# vcpkg 的 paho-mqttpp3 在 Windows 上以 DLL 形式安装，message.h 中的
# EMPTY_STR / EMPTY_BIN 需要 __declspec(dllimport) 才能正确链接；
# 上游约定由使用方定义 PAHO_MQTTPP_IMPORTS。
if(MSVC AND TARGET PahoMqttCpp::paho-mqttpp3-shared)
    target_compile_definitions(mqtt_client PUBLIC PAHO_MQTTPP_IMPORTS)
endif()

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

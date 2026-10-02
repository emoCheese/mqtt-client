cmake_minimum_required(VERSION 3.10.0)
project(test_main VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

list(APPEND CMAKE_PREFIX_PATH "/home/fox/workspace/my-cpp-3rdparty/install_shared")

# 让顶层 ctest 能收集子目录里的测试
enable_testing()

add_subdirectory(mqtt_client)
add_subdirectory(mqtt_client0)

add_executable(test_main main.cpp)


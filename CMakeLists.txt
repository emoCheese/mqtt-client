cmake_minimum_required(VERSION 3.21)

# ---------------------------------------------------------------------------
# vcpkg 工具链定位（必须在 project() 之前完成）
#
# 背景：使用 CMake Tools / Visual Studio 的
#   “Visual Studio Community 2022 Release - amd64” 工具包时，CMake 会复用
#   CMakeCache.txt 中上一次写入的 CMAKE_TOOLCHAIN_FILE。一旦 vcpkg 被移动、
#   重装或从 VS 自带的 <VS>/VC/vcpkg 换成独立安装的 vcpkg，缓存里就会残留一个
#   不存在的路径，于是 CMake 在 project() 阶段直接报：
#     Could not find toolchain file: .../vcpkg/scripts/buildsystems/vcpkg.cmake
#
# 现在的做法：
#   1) 调用方（命令行 / IDE 工具包）给出的工具链文件存在才沿用；
#   2) 否则忽略失效路径，按 VCPKG_ROOT（-D 变量、环境变量）、PATH 中的
#      vcpkg 依次探测，始终使用当前安装的 vcpkg；
#   3) 探测结果写回缓存，从而自动修复历史遗留的失效配置。
# ---------------------------------------------------------------------------

if(NOT CMAKE_TOOLCHAIN_FILE OR NOT EXISTS "${CMAKE_TOOLCHAIN_FILE}")
    # 1) -DVCPKG_ROOT=... 优先，其次环境变量
    if(NOT VCPKG_ROOT)
        set(VCPKG_ROOT "$ENV{VCPKG_ROOT}")
    endif()
    file(TO_CMAKE_PATH "${VCPKG_ROOT}" VCPKG_ROOT)
    string(REGEX REPLACE "/+$" "" VCPKG_ROOT "${VCPKG_ROOT}")

    # 2) 兜底：PATH 里的 vcpkg，其所在目录就是 vcpkg 根目录
    if(NOT EXISTS "${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake")
        find_program(_mqttx_vcpkg_exe NAMES vcpkg vcpkg.exe NO_CACHE)
        if(_mqttx_vcpkg_exe)
            get_filename_component(VCPKG_ROOT "${_mqttx_vcpkg_exe}" DIRECTORY)
        endif()
    endif()

    if(NOT VCPKG_ROOT OR NOT EXISTS "${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake")
        message(FATAL_ERROR
            "未找到当前安装的 vcpkg：需要 <vcpkg 根目录>/scripts/buildsystems/vcpkg.cmake。\n"
            "请设置环境变量 VCPKG_ROOT，或在配置时指定：\n"
            "  cmake -S . -B build -DVCPKG_ROOT=<vcpkg 安装目录>")
    endif()

    # 3) 写回缓存：本次配置生效，同时覆盖缓存里历史遗留的失效路径
    message(STATUS "使用当前安装的 vcpkg：${VCPKG_ROOT}")
    set(CMAKE_TOOLCHAIN_FILE "${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake"
        CACHE FILEPATH "CMake 工具链文件（由根 CMakeLists.txt 校验/探测）" FORCE)
endif()

project(test_main VERSION 0.1.0 LANGUAGES C CXX)

if(MSVC)
    # 源码为 UTF-8（含中文注释与字符串），避免 MSVC 按本地代码页(936)解析
    # 导致 C4819 警告甚至 “常量中有换行符” 的编译错误。
    add_compile_options(/utf-8)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# list(APPEND CMAKE_PREFIX_PATH "/home/fox/workspace/cpp_3rdparty/install_shared")

# 让顶层 ctest 能收集子目录里的测试
enable_testing()

add_subdirectory(mqtt_client)

add_executable(test_main main.cpp)

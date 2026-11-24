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
#      vcpkg、常见安装目录依次探测，始终使用当前安装的 vcpkg；
#   3) 探测结果写回缓存，从而自动修复历史遗留的失效配置。
# ---------------------------------------------------------------------------

set(VCPKG_ROOT "" CACHE PATH "vcpkg 安装根目录（留空则自动探测）")

# 判定一个目录是否为 vcpkg 根目录的唯一标准：
# 其下的 scripts/buildsystems/vcpkg.cmake 真实存在。
set(_mqttx_vcpkg_toolchain "")

# --- 1) 调用方显式指定的工具链文件 ----------------------------------------
if(CMAKE_TOOLCHAIN_FILE AND EXISTS "${CMAKE_TOOLCHAIN_FILE}")
    set(_mqttx_vcpkg_toolchain "${CMAKE_TOOLCHAIN_FILE}")
    message(STATUS "沿用已指定的 CMake 工具链文件：${_mqttx_vcpkg_toolchain}")
elseif(CMAKE_TOOLCHAIN_FILE)
    message(STATUS "忽略已失效的工具链文件：${CMAKE_TOOLCHAIN_FILE}")
    message(STATUS "（vcpkg 可能已被移动或重装，正在重新定位当前安装的 vcpkg）")
endif()

# --- 2) 自动探测当前安装的 vcpkg -------------------------------------------
if(NOT _mqttx_vcpkg_toolchain)
    set(_mqttx_vcpkg_roots "")

    # 2.1 -DVCPKG_ROOT=... 显式指定的根目录优先
    if(VCPKG_ROOT)
        list(APPEND _mqttx_vcpkg_roots "${VCPKG_ROOT}")
    endif()

    # 2.2 环境变量（vcpkg 官方约定；VCPKG_INSTALLATION_ROOT 为 CI 常用写法）
    foreach(_mqttx_env IN ITEMS VCPKG_ROOT VCPKG_INSTALLATION_ROOT)
        if(DEFINED ENV{${_mqttx_env}} AND NOT "$ENV{${_mqttx_env}}" STREQUAL "")
            list(APPEND _mqttx_vcpkg_roots "$ENV{${_mqttx_env}}")
        endif()
    endforeach()

    # 2.3 PATH 中的 vcpkg（vcpkg.exe 所在目录即根目录）
    find_program(_mqttx_vcpkg_exe NAMES vcpkg vcpkg.exe NO_CACHE)
    if(_mqttx_vcpkg_exe)
        get_filename_component(_mqttx_vcpkg_exe_dir "${_mqttx_vcpkg_exe}" DIRECTORY)
        list(APPEND _mqttx_vcpkg_roots "${_mqttx_vcpkg_exe_dir}")
    endif()

    # 2.4 常见安装位置兜底
    foreach(_mqttx_guess
            "$ENV{LOCALAPPDATA}/vcpkg"
            "$ENV{USERPROFILE}/vcpkg"
            "$ENV{HOME}/vcpkg"
            "C:/vcpkg"
            "C:/dev/vcpkg"
            "C:/tools/vcpkg")
        if(EXISTS "${_mqttx_guess}")
            list(APPEND _mqttx_vcpkg_roots "${_mqttx_guess}")
        endif()
    endforeach()

    list(REMOVE_DUPLICATES _mqttx_vcpkg_roots)
    foreach(_mqttx_root IN LISTS _mqttx_vcpkg_roots)
        file(TO_CMAKE_PATH "${_mqttx_root}" _mqttx_root_cmake)
        # 去掉结尾的斜杠，避免拼出 "//scripts"
        string(REGEX REPLACE "/+$" "" _mqttx_root_cmake "${_mqttx_root_cmake}")
        if(EXISTS "${_mqttx_root_cmake}/scripts/buildsystems/vcpkg.cmake")
            set(_mqttx_vcpkg_toolchain "${_mqttx_root_cmake}/scripts/buildsystems/vcpkg.cmake")
            set(VCPKG_ROOT "${_mqttx_root_cmake}" CACHE PATH
                "vcpkg 安装根目录（留空则自动探测）" FORCE)
            message(STATUS "使用当前安装的 vcpkg：${_mqttx_root_cmake}")
            break()
        endif()
    endforeach()
endif()

# --- 3) 仍未找到则给出可操作的错误，而不是让人费解的 toolchain 报错 ----------
if(NOT _mqttx_vcpkg_toolchain)
    message(FATAL_ERROR
        "未找到可用的 vcpkg（需要 <vcpkg 根目录>/scripts/buildsystems/vcpkg.cmake）。\n"
        "已尝试的候选：\n"
        "  VCPKG_ROOT（-D 变量）      = ${VCPKG_ROOT}\n"
        "  VCPKG_ROOT（环境变量）     = $ENV{VCPKG_ROOT}\n"
        "  VCPKG_INSTALLATION_ROOT    = $ENV{VCPKG_INSTALLATION_ROOT}\n"
        "  PATH 中的 vcpkg            = ${_mqttx_vcpkg_exe}\n"
        "请设置环境变量 VCPKG_ROOT，或在配置时显式指定：\n"
        "  cmake -S . -B build -DVCPKG_ROOT=<vcpkg 安装目录>\n"
        "  cmake -S . -B build -DCMAKE_TOOLCHAIN_FILE=<vcpkg>/scripts/buildsystems/vcpkg.cmake")
endif()

# 写回缓存：既用于本次配置，也覆盖缓存中历史遗留的失效路径
set(CMAKE_TOOLCHAIN_FILE "${_mqttx_vcpkg_toolchain}" CACHE FILEPATH
    "CMake 工具链文件（由根 CMakeLists.txt 校验/探测）" FORCE)

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

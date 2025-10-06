cmake_minimum_required(VERSION 3.25)
project(rocksdb_bench VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_TOOLCHAIN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/vcpkg/scripts/buildsystems/vcpkg.cmake
    CACHE STRING "Vcpkg toolchain file")

find_package(fmt CONFIG REQUIRED)
find_package(RocksDB CONFIG REQUIRED)
find_package(CLI11 CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)
find_package(GTest CONFIG REQUIRED)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 收集版本信息
execute_process(
  COMMAND git rev-parse --short HEAD
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  OUTPUT_VARIABLE GIT_COMMIT_HASH
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET
)

execute_process(
  COMMAND git rev-parse --abbrev-ref HEAD
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  OUTPUT_VARIABLE GIT_BRANCH
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET
)

execute_process(
  COMMAND git log -1 --format=%cd --date=short
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  OUTPUT_VARIABLE GIT_COMMIT_DATE
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET
)

string(TIMESTAMP BUILD_TIME "%Y-%m-%d %H:%M:%S")

# 如果不是git仓库，设置默认值
if(NOT GIT_COMMIT_HASH)
  set(GIT_COMMIT_HASH "unknown")
endif()
if(NOT GIT_BRANCH)
  set(GIT_BRANCH "unknown")
endif()
if(NOT GIT_COMMIT_DATE)
  set(GIT_COMMIT_DATE "unknown")
endif()

message(STATUS "Git Commit: ${GIT_COMMIT_HASH}")
message(STATUS "Git Branch: ${GIT_BRANCH}")
message(STATUS "Git Date: ${GIT_COMMIT_DATE}")
message(STATUS "Build Time: ${BUILD_TIME}")

add_subdirectory(src)
add_subdirectory(tests)

# Main executable at top level
add_executable(rocksdb_bench_app src/main.cpp)

# 定义编译宏
target_compile_definitions(rocksdb_bench_app PRIVATE
    GIT_COMMIT_HASH="${GIT_COMMIT_HASH}"
    GIT_BRANCH="${GIT_BRANCH}"
    GIT_COMMIT_DATE="${GIT_COMMIT_DATE}"
    BUILD_TIME="${BUILD_TIME}"
)

target_link_libraries(rocksdb_bench_app
    PRIVATE
        core_lib
        benchmark_lib
        utils_lib
        strategies_lib
        RocksDB::rocksdb
        fmt::fmt
        CLI11::CLI11
        nlohmann_json::nlohmann_json
        spdlog::spdlog
)

# Config generator utility
add_executable(generate_config_example src/generate_config_example.cpp)

target_link_libraries(generate_config_example
    PRIVATE
        core_lib
        CLI11::CLI11
        nlohmann_json::nlohmann_json
)
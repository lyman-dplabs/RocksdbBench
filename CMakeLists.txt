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

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_subdirectory(src)
add_subdirectory(tests)

# Main executable at top level
add_executable(rocksdb_bench_app src/main.cpp)

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
)

# Config generator utility
add_executable(generate_config_example src/generate_config_example.cpp)

target_link_libraries(generate_config_example
    PRIVATE
        core_lib
        CLI11::CLI11
        nlohmann_json::nlohmann_json
)
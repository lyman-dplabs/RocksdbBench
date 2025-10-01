cmake_minimum_required(VERSION 3.25)

# Add src directory to include path
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../src)

# Test executables
add_executable(test_historical_queries test_historical_queries.cpp)
add_executable(test_quick_keyblock test_quick_keyblock.cpp)
add_executable(test_dual_strategy test_dual_strategy.cpp)
add_executable(test_args test_args.cpp)
add_executable(test_historical_fix test_historical_fix.cpp)
add_executable(test_random_value_generation test_random_value_generation.cpp)
add_executable(test_initial_load_optimization test_initial_load_optimization.cpp)
add_executable(test_performance_comparison test_performance_comparison.cpp)
add_executable(test_direct_version_optimized test_direct_version_optimized.cpp)
add_executable(test_direct_version_batch_config test_direct_version_batch_config.cpp)
add_executable(test_spdlog test_spdlog.cpp)

# Common test libraries
target_link_libraries(test_historical_queries
    PRIVATE
        core_lib
        benchmark_lib
        utils_lib
        strategies_lib
        RocksDB::rocksdb
        fmt::fmt
)

target_link_libraries(test_quick_keyblock
    PRIVATE
        core_lib
        benchmark_lib
        utils_lib
        strategies_lib
        RocksDB::rocksdb
        fmt::fmt
)

target_link_libraries(test_dual_strategy
    PRIVATE
        core_lib
        benchmark_lib
        utils_lib
        strategies_lib
        RocksDB::rocksdb
        fmt::fmt
)

target_link_libraries(test_args
    PRIVATE
        core_lib
        utils_lib
        strategies_lib
        CLI11::CLI11
        nlohmann_json::nlohmann_json
)

target_link_libraries(test_historical_fix
    PRIVATE
        core_lib
        benchmark_lib
        utils_lib
        strategies_lib
        RocksDB::rocksdb
        fmt::fmt
)

target_link_libraries(test_random_value_generation
    PRIVATE
        utils_lib
)

target_link_libraries(test_initial_load_optimization
    PRIVATE
        core_lib
        benchmark_lib
        utils_lib
        strategies_lib
        RocksDB::rocksdb
        fmt::fmt
)

target_link_libraries(test_performance_comparison
    PRIVATE
        core_lib
        benchmark_lib
        utils_lib
        strategies_lib
        RocksDB::rocksdb
        fmt::fmt
)

target_link_libraries(test_direct_version_optimized
    PRIVATE
        core_lib
        benchmark_lib
        utils_lib
        strategies_lib
        RocksDB::rocksdb
        fmt::fmt
)

target_link_libraries(test_direct_version_batch_config
    PRIVATE
        core_lib
        benchmark_lib
        utils_lib
        strategies_lib
        RocksDB::rocksdb
        fmt::fmt
)

target_link_libraries(test_spdlog
    PRIVATE
        utils_lib
        spdlog::spdlog
)


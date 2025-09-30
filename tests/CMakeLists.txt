cmake_minimum_required(VERSION 3.25)

# Add src directory to include path
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../src)

# Test executables
add_executable(test_metrics test_metrics.cpp)
add_executable(test_historical_queries test_historical_queries.cpp)
add_executable(test_quick_keyblock test_quick_keyblock.cpp)
add_executable(test_bloom_simple test_bloom_simple.cpp)
add_executable(test_index_search test_index_search.cpp)
add_executable(test_dual_strategy test_dual_strategy.cpp)
add_executable(test_args test_args.cpp)
add_executable(test_simple_keyblock test_simple_keyblock.cpp)
add_executable(test_reduced_keyblock test_reduced_keyblock.cpp)

# Common test libraries
target_link_libraries(test_metrics
    PRIVATE
        core_lib
        benchmark_lib
        utils_lib
        strategies_lib
        RocksDB::rocksdb
        fmt::fmt
)

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

target_link_libraries(test_bloom_simple
    PRIVATE
        core_lib
        utils_lib
        strategies_lib
        RocksDB::rocksdb
        fmt::fmt
)

target_link_libraries(test_index_search
    PRIVATE
        core_lib
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

target_link_libraries(test_simple_keyblock
    PRIVATE
        core_lib
        benchmark_lib
        utils_lib
        strategies_lib
        RocksDB::rocksdb
        fmt::fmt
)

target_link_libraries(test_reduced_keyblock
    PRIVATE
        core_lib
        benchmark_lib
        utils_lib
        strategies_lib
        RocksDB::rocksdb
        fmt::fmt
)
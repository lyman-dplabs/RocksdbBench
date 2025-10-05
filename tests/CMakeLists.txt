cmake_minimum_required(VERSION 3.25)

# Add src directory to include path
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../src)

# Test executables for historical version query functionality
add_executable(test_historical_version_query test_historical_version_query.cpp)
add_executable(test_direct_strategy_basic test_direct_strategy_basic.cpp)
add_executable(test_dual_strategy_basic test_dual_strategy_basic.cpp)
add_executable(test_concurrent_read_write test_concurrent_read_write.cpp)
add_executable(test_lock_optimization test_lock_optimization.cpp)

# Historical version query test
target_link_libraries(test_historical_version_query
    PRIVATE
        core_lib
        benchmark_lib
        utils_lib
        strategies_lib
        RocksDB::rocksdb
        fmt::fmt
)

# Basic strategy tests
target_link_libraries(test_direct_strategy_basic
    PRIVATE
        core_lib
        strategies_lib
        utils_lib
        RocksDB::rocksdb
        fmt::fmt
)

target_link_libraries(test_dual_strategy_basic
    PRIVATE
        core_lib
        strategies_lib
        utils_lib
        RocksDB::rocksdb
        fmt::fmt
)

# Concurrent read-write test
target_link_libraries(test_concurrent_read_write
    PRIVATE
        core_lib
        benchmark_lib
        utils_lib
        strategies_lib
        RocksDB::rocksdb
        fmt::fmt
)

# Lock optimization test
target_link_libraries(test_lock_optimization
    PRIVATE
        core_lib
        benchmark_lib
        utils_lib
        strategies_lib
        RocksDB::rocksdb
        fmt::fmt
)
add_subdirectory(core)
add_subdirectory(benchmark)
add_subdirectory(utils)
add_subdirectory(strategies)

add_executable(rocksdb_bench_app main.cpp)

add_executable(test_metrics ../test_metrics.cpp)
add_executable(test_historical_queries ../test_historical_queries.cpp)
add_executable(test_quick_keyblock ../test_quick_keyblock.cpp)
add_executable(test_bloom_simple ../test_bloom_simple.cpp)
add_executable(test_index_search ../test_index_search.cpp)

target_link_libraries(rocksdb_bench_app
    PRIVATE
        core_lib
        benchmark_lib
        utils_lib
        strategies_lib
        RocksDB::rocksdb
        fmt::fmt
)

target_link_libraries(test_metrics
    PRIVATE
        core_lib
        benchmark_lib
        utils_lib
        RocksDB::rocksdb
        fmt::fmt
)

target_link_libraries(test_historical_queries
    PRIVATE
        core_lib
        benchmark_lib
        utils_lib
        RocksDB::rocksdb
        fmt::fmt
)

target_link_libraries(test_quick_keyblock
    PRIVATE
        core_lib
        benchmark_lib
        utils_lib
        RocksDB::rocksdb
        fmt::fmt
)

target_link_libraries(test_bloom_simple
    PRIVATE
        core_lib
        utils_lib
        RocksDB::rocksdb
        fmt::fmt
)

target_link_libraries(test_index_search
    PRIVATE
        core_lib
        utils_lib
        RocksDB::rocksdb
        fmt::fmt
)
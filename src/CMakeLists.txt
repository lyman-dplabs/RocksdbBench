add_subdirectory(core)
add_subdirectory(benchmark)
add_subdirectory(utils)

add_executable(rocksdb_bench_app main.cpp)

target_link_libraries(rocksdb_bench_app
    PRIVATE
        core_lib
        benchmark_lib
        utils_lib
        RocksDB::rocksdb
        fmt::fmt
)
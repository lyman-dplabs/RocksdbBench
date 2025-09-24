add_library(benchmark_lib
    scenario_runner.hpp
    scenario_runner.cpp
    metrics_collector.hpp
    metrics_collector.cpp
)

target_link_libraries(benchmark_lib
    PUBLIC
        core_lib
        utils_lib
        RocksDB::rocksdb
)
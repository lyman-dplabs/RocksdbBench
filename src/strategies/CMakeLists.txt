# Strategies library
add_library(strategies_lib
    page_index_strategy.cpp
    direct_version_strategy.cpp
    dual_rocksdb_strategy.cpp
)

target_include_directories(strategies_lib
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(strategies_lib
    PUBLIC
        core_lib
        utils_lib
        RocksDB::rocksdb
        fmt::fmt
)
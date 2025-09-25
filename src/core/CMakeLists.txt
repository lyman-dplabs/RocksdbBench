add_library(core_lib
    storage_strategy.hpp
    strategy_db_manager.hpp
    strategy_db_manager.cpp
    config.hpp
    config.cpp
    db_manager.hpp
    db_manager.cpp
    types.hpp
)

target_link_libraries(core_lib
    PUBLIC
        RocksDB::rocksdb
        fmt::fmt
)
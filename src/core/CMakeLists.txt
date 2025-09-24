add_library(core_lib
    types.hpp
    db_manager.hpp
    db_manager.cpp
)

target_link_libraries(core_lib
    PUBLIC
        RocksDB::rocksdb
)
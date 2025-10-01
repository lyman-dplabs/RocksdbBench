add_library(core_lib
    storage_strategy.hpp
    strategy_db_manager.hpp
    strategy_db_manager.cpp
    config.hpp
    config.cpp
    types.hpp
    version.cpp
)

# 定义版本编译宏
target_compile_definitions(core_lib PRIVATE
    GIT_COMMIT_HASH="${GIT_COMMIT_HASH}"
    GIT_BRANCH="${GIT_BRANCH}"
    GIT_COMMIT_DATE="${GIT_COMMIT_DATE}"
    BUILD_TIME="${BUILD_TIME}"
)

target_link_libraries(core_lib
    PUBLIC
        RocksDB::rocksdb
        fmt::fmt
        CLI11::CLI11
        nlohmann_json::nlohmann_json
    PRIVATE
        strategies_lib
        utils_lib
)
add_library(utils_lib
    logger.hpp
    data_generator.hpp
    data_generator.cpp
)

target_link_libraries(utils_lib
    PUBLIC
        fmt::fmt
        spdlog::spdlog
)
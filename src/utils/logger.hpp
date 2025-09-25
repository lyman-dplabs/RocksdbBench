#pragma once
#include <fmt/core.h>

namespace utils {

template<typename... Args>
void log_info(fmt::format_string<Args...> fmt_str, Args&&... args) {
    fmt::print(fmt_str, std::forward<Args>(args)...);
    fmt::print("\n");
}

template<typename... Args>
void log_error(fmt::format_string<Args...> fmt_str, Args&&... args) {
    fmt::print(stderr, "[ERROR] ");
    fmt::print(stderr, fmt_str, std::forward<Args>(args)...);
    fmt::print(stderr, "\n");
}

template<typename... Args>
void log_debug(fmt::format_string<Args...> fmt_str, Args&&... args) {
    fmt::print("[DEBUG] ");
    fmt::print(fmt_str, std::forward<Args>(args)...);
    fmt::print("\n");
}

template<typename... Args>
void log_warn(fmt::format_string<Args...> fmt_str, Args&&... args) {
    fmt::print(stderr, "[WARN] ");
    fmt::print(stderr, fmt_str, std::forward<Args>(args)...);
    fmt::print(stderr, "\n");
}

}
#include "config.hpp"
#include <fmt/core.h>
#include <iostream>

void print_version_info() {
    std::cout << fmt::format(R"(
RocksDB Benchmark Tool
=======================
Version: 1.0.0
Git Commit: {}
Git Branch: {}
Git Date: {}
Build Time: {}

Build System: CMake + vcpkg
Compiler: C++23
)",
#ifdef GIT_COMMIT_HASH
        GIT_COMMIT_HASH,
#else
        "unknown",
#endif
#ifdef GIT_BRANCH
        GIT_BRANCH,
#else
        "unknown", 
#endif
#ifdef GIT_COMMIT_DATE
        GIT_COMMIT_DATE,
#else
        "unknown",
#endif
#ifdef BUILD_TIME
        BUILD_TIME
#else
        "unknown"
#endif
    );
}
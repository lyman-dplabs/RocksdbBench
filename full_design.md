# RocksDB 基准测试项目详细设计文档 (Full Design)

## 1. 概述

本文档基于 `prd.md` 的产品需求，提供一个全面的技术设计方案。该方案旨在构建一个高效、可维护且易于使用的 RocksDB 基准测试工具。

### 1.1. 技术栈

- **语言**: C++23
- **构建系统**: CMake
- **依赖管理**: vcpkg (通过 Git Submodule 集成)
- **核心依赖**:
    - `rocksdb`: 核心存储引擎
    - `fmt`: 高性能格式化日志输出
    - `gtest`: (可选) 单元测试框架

## 2. 项目结构

采用模块化、关注点分离的原则，设计以下目录结构：

```
rocksdb_bench/
├── .gitmodules
├── CMakeLists.txt
├── README.md
├── vcpkg/                # Git Submodule for vcpkg
├── src/
│   ├── main.cpp
│   ├── core/
│   │   ├── CMakeLists.txt
│   │   ├── types.hpp         # 定义核心数据结构 (Key, Value, BlockNum, etc.)
│   │   ├── db_manager.hpp    # RocksDB 封装类
│   │   └── db_manager.cpp
│   ├── benchmark/
│   │   ├── CMakeLists.txt
│   │   ├── scenario_runner.hpp # 测试场景调度器
│   │   ├── scenario_runner.cpp
│   │   ├── metrics_collector.hpp # 性能指标收集器
│   │   └── metrics_collector.cpp
│   └── utils/
│       ├── CMakeLists.txt
│       ├── logger.hpp        # 基于 fmt 的日志工具
│       └── data_generator.hpp  # 测试数据生成器
└── scripts/
    ├── setup.sh            # 初始化 vcpkg 并安装依赖
    ├── build.sh            # 构建项目
    └── run.sh              # 运行基准测试
```

## 3. 依赖管理与构建系统 (vcpkg & CMake)

### 3.1. vcpkg 集成

- `vcpkg` 将作为 Git Submodule 添加到项目根目录，确保所有开发者使用统一版本的依赖管理器。
- **初始化流程**:
    1. `git submodule update --init --recursive`
    2. 运行 `vcpkg/bootstrap-vcpkg.sh`
- `scripts/setup.sh` 脚本将封装此流程，并自动安装 `rocksdb` 和 `fmt`。

### 3.2. CMake 配置

- **根 `CMakeLists.txt`**:
    - 设置 C++ 标准为 `c++23`。
    - 集成 vcpkg：通过设置 `CMAKE_TOOLCHAIN_FILE` 指向 `vcpkg/scripts/buildsystems/vcpkg.cmake`，实现对 `find_package` 的无缝支持。
    - 添加 `src` 作为子目录。
- **模块化 `CMakeLists.txt` (`src/core/CMakeLists.txt` 等)**:
    - 每个子目录（`core`, `benchmark`, `utils`）拥有自己的 `CMakeLists.txt`。
    - 定义各自的库目标 (`add_library`)，并明确其源文件和对其他库的依赖 (`target_link_libraries`)。
    - 这种结构使得模块可以独立编译和测试。
- **缓存与增量编译**:
    - **vcpkg 缓存**: vcpkg 会自动缓存已编译的依赖库。只要依赖版本不变，`scripts/setup.sh` 再次运行时会直接使用缓存，无需重新编译 `rocksdb` 等大型库。
    - **CMake/Ninja 缓存**: CMake 会生成构建系统（如 Ninja 或 Make），它们能精确跟踪文件变更。只有被修改过的源文件及其依赖项才会被重新编译，从而实现高效的增量构建。

## 4. 核心组件设计 (C++23)

遵循“高内聚、低耦合”的设计原则。

### 4.1. `src/core/types.hpp` - 核心数据结构

定义项目中流转的基础数据类型，增强代码的可读性和类型安全。

```cpp
// src/core/types.hpp
#pragma once
#include <cstdint>
#include <string>
#include <vector>

// 使用 using 替代裸的内置类型
using BlockNum = uint64_t;
using PageNum = uint64_t;
using Key = std::string;
using Value = std::string; // 32-byte binary data

// ChangeSet 表的记录
struct ChangeSetRecord {
    BlockNum block_num;
    std::string addr_slot;
    Value value;

    // 生成 RocksDB Key
    Key to_key() const;
};

// Index 表的记录
struct IndexRecord {
    PageNum page_num;
    std::string addr_slot;
    std::vector<BlockNum> block_history;

    // 生成 RocksDB Key
    Key to_key() const;
};
```

### 4.2. `src/utils/logger.hpp` - 日志工具

- 封装 `fmt` 库，提供统一的日志接口。
- **禁止使用 `std::cout` / `printf`**。所有输出必须通过该日志工具。

```cpp
// src/utils/logger.hpp
#pragma once
#include <fmt/core.h>

namespace utils {
    // 提供不同级别的日志
    template<typename... Args>
    void log_info(fmt::format_string<Args...> fmt_str, Args&&... args) {
        fmt::print(fmt_str, std::forward<Args>(args)...);
    }
    // ... 可添加 log_error, log_debug 等
}
```

### 4.3. `src/core/db_manager.hpp` - 数据库管理器

- **高内聚**: 封装所有与 RocksDB 相关的操作，包括数据库的打开/关闭、写入、读取和 `MergeOperator` 的实现。
- **低耦合**: 对外只暴露业务逻辑相关的接口，如 `write_batch`, `get_historical_state`，隐藏 RocksDB 的底层细节。

```cpp
// src/core/db_manager.hpp
#pragma once
#include "types.hpp"
#include <rocksdb/db.h>
#include <memory>
#include <vector>

class DBManager {
public:
    explicit DBManager(const std::string& db_path);
    ~DBManager();

    // 禁止拷贝和移动
    DBManager(const DBManager&) = delete;
    DBManager& operator=(const DBManager&) = delete;

    // 批量写入 ChangeSet 和 Index
    bool write_batch(const std::vector<ChangeSetRecord>& changes, const std::vector<IndexRecord>& indices);

    // 查询历史状态
    std::optional<Value> get_historical_state(const std::string& addr_slot, BlockNum target_block_num);

private:
    std::unique_ptr<rocksdb::DB> db_;
    // ... 其他 RocksDB 配置和句柄
};
```

### 4.4. `src/benchmark/scenario_runner.hpp` - 场景调度器

- 负责执行 `prd.md` 中定义的各个测试阶段。
- 依赖 `DBManager`, `MetricsCollector`, `DataGenerator`。

```cpp
// src/benchmark/scenario_runner.hpp
#pragma once
#include "../core/db_manager.hpp"
#include "metrics_collector.hpp"
#include "../utils/data_generator.hpp"

class ScenarioRunner {
public:
    ScenarioRunner(std::shared_ptr<DBManager> db, std::shared_ptr<MetricsCollector> metrics);

    void run_initial_load_phase();
    void run_hotspot_update_phase();

private:
    std::shared_ptr<DBManager> db_manager_;
    std::shared_ptr<MetricsCollector> metrics_collector_;
    DataGenerator data_generator_;
};
```

### 4.5. `src/benchmark/metrics_collector.hpp` - 指标收集器

- 负责测量和报告 `prd.md` 中定义的各项性能指标。
- 使用 `std::chrono` 进行精确计时。

```cpp
// src/benchmark/metrics_collector.hpp
#pragma once
#include <chrono>

class MetricsCollector {
public:
    void start_write_timer();
    void stop_and_record_write(size_t keys_written, size_t bytes_written);

    // ... 其他指标的记录方法

    void report_summary() const;

private:
    std::chrono::high_resolution_clock::time_point start_time_;
    // ... 其他用于存储指标的成员变量
};
```

## 5. 脚本与文档

### 5.1. `scripts/`

- **`setup.sh`**:
    ```bash
    #!/bin/bash
    set -e # Abort on error
    git submodule update --init --recursive
    ./vcpkg/bootstrap-vcpkg.sh
    ./vcpkg/vcpkg install rocksdb fmt
    ```
- **`build.sh`**:
    ```bash
    #!/bin/bash
    set -e
    cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE=./vcpkg/scripts/buildsystems/vcpkg.cmake
    cmake --build build
    ```
- **`run.sh`**:
    ```bash
    #!/bin/bash
    set -e
    # 传递命令行参数来配置测试
    ./build/src/rocksdb_bench_app --keys=100000000 --hotspot_ratio=0.8
    ```

### 5.2. `README.md`

`README.md` 文件应包含以下部分：

1.  **项目简介**: 简要说明项目的目标。
2.  **环境要求**: 列出必要的依赖（CMake, C++23 编译器等）。
3.  **快速开始**:
    - **第一步：克隆仓库**
        ```bash
        git clone --recurse-submodules <repo-url>
        ```
    - **第二步：安装依赖**
        ```bash
        ./scripts/setup.sh
        ```
    - **第三步：编译项目**
        ```bash
        ./scripts/build.sh
        ```
    - **第四步：运行测试**
        ```bash
        ./scripts/run.sh
        ```
4.  **配置选项**: 说明 `run.sh` 或可执行文件接受的命令行参数，如 Key 的数量、分布模式等。
5.  **设计说明**: 简要链接或概括 `full_design.md` 的核心设计思想。

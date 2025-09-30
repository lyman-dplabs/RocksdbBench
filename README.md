# RocksDB 基准测试工具

一个用于评估 RocksDB 在模拟区块链数据存储场景下性能的基准测试工具，支持多种存储策略的性能对比。

## 🎯 项目特色

- **多策略支持**: 内置 PageIndexStrategy 和 DirectVersionStrategy 两种存储策略
- **可扩展架构**: 基于策略模式设计，便于添加新的存储策略
- **真实工作负载**: 模拟区块链状态访问模式，包括批量写入、热点更新和历史查询
- **性能对比**: 支持不同策略在相同工作负载下的公平性能对比
- **详细监控**: 提供全面的性能指标和 RocksDB 内部统计信息

## 📖 详细文档

- **📚 实现指南**: 查看 [guide.md](guide.md) 了解详细的架构设计和实现原理
- **🔧 使用说明**: 查看 [scripts/README.md](scripts/README.md) 了解具体的使用方法和选项
- **📊 策略对比**: 查看 [STRATEGY_COMPARISON.md](STRATEGY_COMPARISON.md) 了解不同策略的特点和适用场景

## 🚀 快速开始

### 环境要求

- **操作系统**: Linux (推荐 Ubuntu 20.04+)
- **编译器**: 支持 C++23 的编译器 (GCC 11+ 或 Clang 14+)
- **构建工具**: CMake 3.25+
- **内存**: 建议 8GB 以上
- **磁盘空间**: 建议 50GB 以上可用空间
- **依赖管理**: vcpkg (自动安装RocksDB、fmt、CLI11、nlohmann-json)

### 安装步骤

1. **克隆仓库**
```bash
git clone --recurse-submodules <repository-url>
cd rocksdb_bench
```

2. **安装依赖**
```bash
./scripts/setup.sh
```

3. **编译项目**
```bash
./scripts/build.sh
```

4. **快速验证**
```bash
./scripts/test_both_strategies.sh
```

5. **运行基准测试**
```bash
# 使用默认策略
./scripts/run.sh

# 使用 DirectVersionStrategy
./scripts/run.sh --strategy direct_version --clean

# 使用新的双RocksDB自适应策略
./scripts/run.sh --strategy dual_rocksdb_adaptive --clean

# 使用JSON配置文件
./scripts/run.sh --config config.json --clean
```

## 🛠️ 使用方法

### 主要脚本

#### setup.sh - 环境设置
```bash
./scripts/setup.sh
```
此脚本会：
- 初始化 vcpkg 子模块
- 安装 RocksDB 和 fmt 库依赖

#### build.sh - 编译项目
```bash
./scripts/build.sh
```
此脚本会：
- 配置 CMake 构建环境
- 编译所有源代码
- 生成可执行文件 `./build/src/rocksdb_bench_app`

#### run.sh - 运行测试（主要脚本）
```bash
# 基础用法
./scripts/run.sh

# 策略选择
./scripts/run.sh --strategy direct_version

# 自定义参数
./scripts/run.sh --strategy page_index --initial-records 50000000 --hotspot-updates 5000000 --clean

# 查看帮助
./scripts/run.sh --help
```

#### test_both_strategies.sh - 快速验证
```bash
# 快速测试两种策略（小数据集）
./scripts/test_both_strategies.sh
```

### 支持的存储策略

| 策略名称 | 描述 | 特点 |
|---------|------|------|
| `page_index` | 传统的 ChangeSet+Index 表结构 | 成熟稳定，基于页面的索引组织 |
| `direct_version` | 两层版本索引存储 | 单次查找最新值，版本与数据分离 |
| `dual_rocksdb_adaptive` | 双RocksDB自适应缓存策略 | 双数据库实例，三级智能缓存，Seek-Last优化 |
| `simple_keyblock` | 简单键块策略 | 简化的键值存储，适合基础测试 |
| `reduced_keyblock` | 减少键块策略 | 优化的键块存储，减少内存占用 |

### 命令行选项

#### 现代化CLI11界面

本项目使用CLI11库提供现代化的命令行界面，支持丰富的配置选项和JSON配置文件。

#### run.sh 支持的选项
```bash
./scripts/run.sh [OPTIONS]

选项：
  --db-path PATH             数据库路径（默认：./rocksdb_data）
  --strategy STRATEGY        存储策略：page_index, direct_version, dual_rocksdb_adaptive, simple_keyblock, reduced_keyblock（默认：page_index）
  --clean                    清理现有数据后开始
  --initial-records N        初始记录数（默认：100000000）
  --hotspot-updates N        热点更新数（默认：10000000）
  --config FILE              JSON配置文件路径
  --help, -h                 显示帮助信息
  --version, -v              显示版本信息
```

#### 直接运行可执行文件
```bash
# 基础用法
./build/rocksdb_bench_app [database_path] --strategy STRATEGY

# 完整命令行示例
./build/rocksdb_bench_app ./data --strategy dual_rocksdb_adaptive --clean --initial-records 1000000 --hotspot-updates 100000

# 使用JSON配置文件
./build/rocksdb_bench_app ./data --config config.json

# 组合使用：命令行参数会覆盖配置文件中的对应设置
./build/rocksdb_bench_app ./data --config config.json --initial-records 5000000
```

#### JSON配置文件格式

支持JSON格式的配置文件，可以方便地管理复杂的测试配置：

```json
{
  "database_path": "./rocksdb_data",
  "storage_strategy": "dual_rocksdb_adaptive",
  "clean_start": true,
  "initial_records": 100000000,
  "hotspot_updates": 10000000,
  "strategy_config": {
    "dual_rocksdb_adaptive": {
      "enable_dynamic_cache": true,
      "enable_sharding": false,
      "l1_cache_size_mb": 1024,
      "l2_cache_size_mb": 2048,
      "l3_cache_size_mb": 4096
    }
  }
}
```

### 交互式选项

当检测到数据库目录已存在数据时，程序会提示：

```
[ERROR] Database data already exists at: /path/to/database
[ERROR] Options:
[ERROR]   1. Delete existing data and start fresh benchmark
[ERROR]   2. Exit program
[ERROR] Enter your choice (1 or 2):
```

- **选项 1**: 删除现有数据并开始新的基准测试
- **选项 2**: 退出程序

**建议**: 使用 `--clean` 参数避免交互式提示

## 📊 基准测试场景

### 阶段一：初始化写入测试

**目标**: 测试纯粹的批量写入性能

**流程**:
- 一次性写入 1 亿条变更记录（可配置）
- 每次写入 10,000 条记录
- 使用选定的存储策略进行写入
- 写入时遵循热点数据分布模式

**数据分布**:
- 热点 Key: 10% (写入概率 80%)
- 中等活跃 Key: 20% (写入概率 10%)  
- 长尾 Key: 70% (写入概率 10%)

### 阶段二：热点更新与查询测试

**目标**: 模拟真实业务中带有热点访问的持续更新和周期性查询场景

**流程**:
1. **模拟区块更新**:
   - 处理 1000 万个热点更新（可配置）
   - 每个模拟区块更新 10,000 个 Key
   - Key 选择遵循热点分布 (8000 热点，1000 中等，1000 长尾)
   - 使用选定的存储策略进行更新

2. **周期性历史状态查询**:
   - 每处理 50 万个 KV，执行一次随机历史状态查询
   - 模拟 `eth_call`，读取 Key 在历史区块的状态
   - 测试不同策略的历史查询性能

### 策略对比要点

不同策略在以下方面会有不同表现：
- **写入性能**: 批量写入速度和资源消耗
- **查询性能**: 最新值查询和历史查询的响应时间
- **存储效率**: 数据占用空间和压缩效果
- **内存使用**: 运行时内存占用和缓存效果

## 输出说明

### 实时日志输出

程序运行时会显示详细的进度信息：

```
RocksDB Benchmark Tool Starting...
Database opened successfully at: ./rocksdb_data
Starting benchmark...
Initial load progress: 0/100000000 (0.0%)
Initial load progress: 100000/100000000 (0.1%)
...
Initial load phase completed. Total blocks written: 10000
Starting hotspot update phase...
Hotspot update progress: 100000/10000000
...
Running 100 historical queries...
Hotspot update phase completed. Total processed: 10000000
```

### 性能指标报告

测试完成后，程序会输出详细的性能指标：

```
=== Performance Metrics Summary ===

Write Metrics:
  Total keys written: 110000000
  Total bytes written: 5720000000
  Total write time: 12543.67 ms
  Write batches: 11000
  Average write throughput: 436.78 MB/s

Query Metrics:
  Total queries: 200
  Successful queries: 195
  Query success rate: 97.50%
  Average query time: 2.345 ms

===================================
```

### 关键指标说明

#### 写入性能指标
- **Total keys written**: 总共写入的键值对数量
- **Total bytes written**: 总共写入的数据量（字节）
- **Total write time**: 总写入时间（毫秒）
- **Write batches**: 写入批次数量
- **Average write throughput**: 平均写入吞吐量（MB/s）

#### 查询性能指标  
- **Total queries**: 总查询次数
- **Successful queries**: 成功的查询次数
- **Query success rate**: 查询成功率（%）
- **Average query time**: 平均查询时间（毫秒）

## 注意事项

### 磁盘空间需求

- **初始数据写入**: 约 5-10GB 磁盘空间
- **完整基准测试**: 约 20-30GB 磁盘空间
- **建议**: 确保有足够的可用磁盘空间

### 内存使用

- **编译时**: 建议 4GB 以上可用内存
- **运行时**: 建议 8GB 以上可用内存
- **大容量测试**: 16GB+ 内存可获得更好性能

### 运行时间

- **编译时间**: 5-15 分钟（取决于网络速度和硬件性能）
- **基准测试时间**: 30-120 分钟（取决于硬件性能）
- **数据清理**: 如果选择删除现有数据，需要额外 1-5 分钟

### 数据安全

- **重要提示**: 程序会完全删除指定的数据库目录
- **建议**: 
  - 使用专门的测试目录
  - 不要在生产环境中使用
  - 重要数据请提前备份

### 性能优化建议

1. **使用 SSD**: 建议使用固态硬盘以获得最佳 I/O 性能
2. **关闭后台程序**: 测试时关闭其他占用资源的程序
3. **监控资源**: 使用 `htop`, `iotop` 等工具监控系统资源使用情况
4. **多次运行**: 建议多次运行取平均值以获得稳定结果

## 🚨 故障排除

### 常见问题及解决方案

#### 1. 编译相关错误

**问题**: CMake 配置失败
```bash
# 错误信息示例
CMake Error at CMakeLists.txt:10 (find_package):
  Could not find a configuration file for "RocksDB"
```

**解决方案**:
```bash
# 清理并重新初始化
rm -rf build vcpkg/buildtrees
git submodule update --init --recursive
./scripts/setup.sh
./scripts/build.sh
```

**问题**: C++ 标准不支持
```bash
# 错误信息示例
error: this feature requires C++23 or later
```

**解决方案**:
```bash
# 检查编译器版本
g++ --version  # 需要 11+
clang++ --version  # 需要 14+

# 升级编译器 (Ubuntu/Debian)
sudo apt update && sudo apt install gcc-11 g++-11
```

#### 2. 运行时错误

**问题**: RocksDB 打开失败
```bash
# 错误信息示例
[ERROR] Failed to open database at path: ./rocksdb_data
```

**解决方案**:
```bash
# 检查目录权限和磁盘空间
ls -la ./rocksdb_data
df -h

# 清理数据重新开始
./scripts/run.sh --clean
```

**问题**: 策略加载失败
```bash
# 错误信息示例
[ERROR] Unknown storage strategy: invalid_strategy
```

**解决方案**:
```bash
# 查看可用策略
./scripts/run.sh --help

# 使用正确的策略名称
./scripts/run.sh --strategy page_index  # 或 direct_version
```

**问题**: 内存不足
```bash
# 错误信息示例
terminate called after throwing an instance of 'std::bad_alloc'
```

**解决方案**:
```bash
# 减少数据规模
./scripts/run.sh --initial-records 10000000 --hotspot-updates 1000000

# 增加交换空间或使用更大内存的机器
```

#### 3. 性能问题

**问题**: 测试运行时间过长
```bash
# 使用小数据集进行快速测试
./scripts/run.sh --initial-records 1000000 --hotspot-updates 100000

# 或者使用快速验证脚本
./scripts/test_both_strategies.sh
```

**问题**: 磁盘 I/O 性能差
```bash
# 检查磁盘状态
iostat -x 1

# 使用 SSD 或内存文件系统进行测试
./scripts/run.sh --db-path /dev/shm/rocksdb_bench
```

#### 4. 数据问题

**问题**: 策略间数据冲突
```bash
# 为不同策略使用不同的数据库路径
./scripts/run.sh --strategy page_index --db-path ./data_page_index --clean
./scripts/run.sh --strategy direct_version --db-path ./data_direct_version --clean
```

**问题**: 历史数据损坏
```bash
# 完全清理并重新开始
rm -rf ./rocksdb_data
./scripts/run.sh --clean
```

### 调试技巧

#### 1. 启用详细日志
```bash
# 运行时查看详细输出
./scripts/run.sh 2>&1 | tee benchmark.log

# 检查 RocksDB 内部统计
ls -la ./rocksdb_data/LOG*
```

#### 2. 性能分析
```bash
# 使用系统监控工具
top -p $(pgrep rocksdb_bench_app)
iotop -o

# 使用 perf 进行性能分析
perf record -g ./build/src/rocksdb_bench_app --strategy page_index --clean
perf report
```

#### 3. 内存和磁盘监控
```bash
# 监控内存使用
free -h
watch -n 1 'free -h'

# 监控磁盘使用
df -h ./rocksdb_data
watch -n 1 'du -sh ./rocksdb_data'
```

### 获取帮助

如果以上解决方案都无法解决你的问题：

1. **检查日志文件**: 查看编译和运行时的详细日志
2. **查看文档**: 重新阅读 [guide.md](guide.md) 和 [scripts/README.md](scripts/README.md)
3. **最小化测试**: 使用小数据集 (`--initial-records 1000`) 重现问题
4. **环境信息**: 收集操作系统、编译器版本、RocksDB 版本等信息

## 📁 项目结构

```
rocksdb_bench/
├── src/
│   ├── core/                          # 核心组件
│   │   ├── storage_strategy.hpp       # 存储策略接口
│   │   ├── strategy_db_manager.hpp    # 支持策略的数据库管理器
│   │   ├── config.hpp                 # 配置管理
│   │   ├── types.hpp                 # 通用数据结构
│   │   └── *.cpp
│   ├── strategies/                    # 存储策略实现
│   │   ├── page_index_strategy.hpp    # 原始的 ChangeSet+Index 策略
│   │   ├── direct_version_strategy.hpp # 两层版本索引策略
│   │   └── *.cpp
│   ├── benchmark/                     # 基准测试执行
│   │   ├── strategy_scenario_runner.hpp # 基于策略的测试运行器
│   │   ├── metrics_collector.hpp     # 性能指标收集
│   │   └── *.cpp
│   ├── utils/                         # 工具类
│   │   ├── logger.hpp                # 日志工具
│   │   ├── data_generator.hpp         # 数据生成器
│   │   └── random_helper.hpp         # 随机数生成
│   └── main.cpp                       # 应用程序入口
├── scripts/                           # 构建和运行脚本
│   ├── setup.sh                      # 环境设置
│   ├── build.sh                      # 编译项目
│   ├── run.sh                        # 主要运行脚本
│   ├── test_both_strategies.sh       # 快速验证脚本
│   └── examples.sh                   # 使用示例
├── docs/                              # 项目文档
│   ├── guide.md                      # 实现指南（中文）
│   ├── refactor.md                   # 重构设计方案
│   └── STRATEGY_COMPARISON.md       # 策略对比
├── vcpkg/                            # 依赖管理
├── build/                            # 构建输出
└── README.md                         # 项目说明
```

### 核心架构说明

- **策略模式**: `IStorageStrategy` 接口允许添加新的存储策略
- **统一管理**: `StrategyDBManager` 管理数据库并委托给当前策略
- **配置驱动**: 支持命令行和配置文件的灵活配置
- **性能监控**: 详细的指标收集和 RocksDB 统计信息

## 🎯 最佳实践建议

### 性能测试建议

1. **环境准备**
   - 使用 SSD 硬盘获得最佳 I/O 性能
   - 测试前关闭其他占用资源的程序
   - 确保充足的内存和磁盘空间

2. **测试策略**
   - 先用小数据集验证功能 (`--initial-records 1000`)
   - 每种策略运行多次取平均值
   - 使用不同的数据库路径避免数据冲突

3. **结果分析**
   - 记录完整的性能指标
   - 监控系统资源使用情况
   - 对比不同策略的优缺点

### 开发新策略

如果你想添加新的存储策略：

1. **阅读文档**: 查看 [guide.md](guide.md) 的详细指南
2. **实现接口**: 继承 `IStorageStrategy` 类
3. **注册策略**: 在 `StorageStrategyFactory` 中注册
4. **测试验证**: 使用小数据集进行功能测试

### 典型工作流程

```bash
# 1. 快速验证功能
./scripts/test_both_strategies.sh

# 2. 性能对比测试
./scripts/run.sh --strategy page_index --db-path ./data_page --clean
./scripts/run.sh --strategy direct_version --db-path ./data_direct --clean

# 3. 自定义测试
./scripts/run.sh --strategy direct_version --initial-records 50000000 --hotspot-updates 5000000 --clean
```

## 🔗 相关链接

- **实现指南**: [guide.md](guide.md) - 详细的架构设计和开发指南
- **使用说明**: [scripts/README.md](scripts/README.md) - 脚本使用和配置选项
- **策略对比**: [STRATEGY_COMPARISON.md](STRATEGY_COMPARISON.md) - 不同策略的特点和性能对比
- **重构设计**: [refactor.md](refactor.md) - 项目重构方案和技术决策

## 📄 许可证

本项目采用 MIT 许可证。详情请参阅 LICENSE 文件。
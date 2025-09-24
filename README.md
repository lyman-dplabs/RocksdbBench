# RocksDB 基准测试工具

一个用于评估 RocksDB 在模拟区块链分页索引和 Changeset 存储场景下性能的基准测试工具。

## 项目背景

本工具旨在通过模拟真实世界的数据访问模式，量化 RocksDB 在写入、更新、查询等关键操作上的性能指标，为系统优化和架构选型提供数据支持。

## 快速开始

### 环境要求

- **操作系统**: Linux (推荐 Ubuntu 20.04+)
- **编译器**: 支持 C++23 的编译器 (GCC 11+ 或 Clang 14+)
- **构建工具**: CMake 3.25+
- **内存**: 建议 8GB 以上
- **磁盘空间**: 建议 50GB 以上可用空间

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

4. **运行基准测试**
```bash
./scripts/run.sh
```

## 使用方法

### 脚本使用

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

#### run.sh - 运行测试
```bash
# 使用默认数据库路径 ./rocksdb_data
./scripts/run.sh

# 指定自定义数据库路径
./scripts/run.sh /path/to/database
```

### 命令行选项

程序启动时支持以下参数：

```
./build/src/rocksdb_bench_app [database_path]
```

- `database_path`: 可选参数，指定 RocksDB 数据存储路径
  - 默认值: `./rocksdb_data`
  - 如果路径不存在，程序会自动创建

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

## 基准测试场景

### 阶段一：初始化写入测试

**目标**: 测试纯粹的批量写入性能

**流程**:
- 一次性写入 1 亿条变更记录
- 每次写入 10,000 条记录
- 同时写入 ChangeSet 表和 Index 表
- 写入时遵循热点数据分布模式

**数据分布**:
- 热点 Key: 1000 万 (写入概率 80%)
- 中等活跃 Key: 2000 万 (写入概率 10%)  
- 长尾 Key: 7000 万 (写入概率 10%)

### 阶段二：热点更新与查询测试

**目标**: 模拟真实业务中带有热点访问的持续更新和周期性查询场景

**流程**:
1. **模拟区块更新**:
   - 每个模拟区块更新 10,000 个 Key
   - Key 选择遵循热点分布 (8000 热点，1000 中等，1000 长尾)
   - 同时更新 ChangeSet 表和 Index 表

2. **周期性历史状态查询**:
   - 每处理 50 万个 KV，执行一次随机历史状态查询
   - 模拟 `eth_call`，读取 Key 在历史区块的状态
   - 测试历史状态查询性能

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

## 故障排除

### 常见问题

1. **编译失败**
   ```bash
   # 清理构建目录并重新编译
   rm -rf build
   ./scripts/build.sh
   ```

2. **依赖安装失败**
   ```bash
   # 清理 vcpkg 缓存并重新安装
   rm -rf vcpkg/buildtrees
   ./scripts/setup.sh
   ```

3. **权限问题**
   ```bash
   # 确保对目录有写权限
   chmod 755 ./scripts/*.sh
   ```

4. **磁盘空间不足**
   ```bash
   # 检查可用空间
   df -h
   # 清理旧数据
   rm -rf ./rocksdb_data
   ```

### 日志调试

如果程序运行异常，可以检查：
- 编译错误信息
- 运行时日志输出
- RocksDB 日志文件（位于数据库目录）

## 项目结构

```
rocksdb_bench/
├── src/
│   ├── core/              # 核心组件
│   │   ├── types.hpp     # 数据结构定义
│   │   ├── db_manager.hpp # 数据库管理
│   │   └── db_manager.cpp
│   ├── benchmark/         # 基准测试逻辑
│   │   ├── scenario_runner.hpp  # 测试场景
│   │   ├── metrics_collector.hpp # 性能指标
│   │   └── *.cpp
│   ├── utils/             # 工具类
│   │   ├── logger.hpp    # 日志工具
│   │   └── data_generator.hpp # 数据生成器
│   └── main.cpp           # 程序入口
├── scripts/               # 构建脚本
│   ├── setup.sh          # 环境设置
│   ├── build.sh          # 编译项目
│   └── run.sh            # 运行测试
├── vcpkg/                # 依赖管理
└── build/                # 构建输出
```

## 许可证

本项目采用 MIT 许可证。详情请参阅 LICENSE 文件。
# RocksDB 基准测试实现指南

## 目录
1. [概述](#概述)
2. [架构设计](#架构设计)
3. [核心组件](#核心组件)
4. **基准测试执行流程**
5. [现有策略](#现有策略)
6. [添加新策略](#添加新策略)
7. [性能考虑](#性能考虑)
8. [示例和最佳实践](#示例和最佳实践)

## 概述

本指南解释了 RocksDB 基准测试工具的实现，重点关注不同的存储策略如何进行基准测试，以及如何添加具有自定义 RocksDB 表结构的新基准测试场景。

该基准测试工具旨在评估类区块链数据访问模式的不同存储策略，具体包括：
- **初始加载阶段**: 批量写入历史数据
- **热点更新阶段**: 模拟持续的链状态更新和周期性查询
- **历史查询**: 读取特定历史区块号的键值

## 架构设计

```
src/
├── core/                          # 核心接口和管理器
│   ├── storage_strategy.hpp       # 策略接口定义
│   ├── strategy_db_manager.hpp   # 支持策略的数据库管理器
│   ├── config.hpp                 # 配置管理
│   └── types.hpp                 # 通用数据结构
├── strategies/                     # 存储策略实现
│   ├── page_index_strategy.hpp    # 原始的 ChangeSet+Index 策略
│   └── direct_version_strategy.hpp # 两层版本索引策略
├── benchmark/                     # 基准测试执行和指标
│   ├── strategy_scenario_runner.hpp # 基于策略的测试运行器
│   └── metrics_collector.hpp     # 性能指标收集
└── main.cpp                       # 应用程序入口
```

### 关键设计原则

1. **策略模式**: 每个存储策略实现相同的接口，但可以使用完全不同的数据结构
2. **工厂模式**: 策略在启动时根据配置创建
3. **统一测试**: 所有策略使用相同的工作负载进行测试，确保公平比较
4. **数据隔离**: 每个策略独立运行，避免相互干扰

## 核心组件

### 1. IStorageStrategy 接口

所有存储策略必须实现的核心抽象接口：

```cpp
class IStorageStrategy {
public:
    // 初始化存储结构（创建表、列族等）
    virtual bool initialize(rocksdb::DB* db) = 0;
    
    // 写入批量数据记录
    virtual bool write_batch(rocksdb::DB* db, 
                           const std::vector<DataRecord>& records) = 0;
    
    // 查询键的最新值
    virtual std::optional<Value> query_latest_value(rocksdb::DB* db, 
                                                   const std::string& addr_slot) = 0;
    
    // 查询特定区块的历史值
    virtual std::optional<Value> query_historical_value(rocksdb::DB* db, 
                                                       const std::string& addr_slot, 
                                                       BlockNum target_block) = 0;
    
    // 策略元数据
    virtual std::string get_strategy_name() const = 0;
    virtual std::string get_description() const = 0;
    
    // 清理资源
    virtual bool cleanup(rocksdb::DB* db) = 0;
};
```

### 2. 统一数据格式

所有策略都使用相同的输入数据格式：

```cpp
struct DataRecord {
    BlockNum block_num;      // 发生此变更的区块号
    std::string addr_slot;   // 键标识符（如 "address:slot"）
    Value value;             // 实际值数据
};
```

### 3. StrategyDBManager

管理 RocksDB 数据库并将操作委托给选定的策略：

```cpp
class StrategyDBManager {
public:
    // 使用当前策略写入数据
    bool write_batch(const std::vector<DataRecord>& records);
    
    // 使用当前策略查询
    std::optional<Value> query_latest_value(const std::string& addr_slot);
    std::optional<Value> query_historical_value(const std::string& addr_slot, 
                                               BlockNum target_block);
    
    // 获取当前策略信息
    std::string get_strategy_name() const;
    std::string get_strategy_description() const;
};
```

## 基准测试执行流程

### 第一阶段：初始加载

```cpp
void StrategyScenarioRunner::run_initial_load_phase() {
    const size_t batch_size = 10000;
    const auto& all_keys = data_generator_.get_all_keys();
    
    for (size_t i = 0; i < all_keys.size(); i += batch_size) {
        // 生成批量 DataRecords
        std::vector<DataRecord> records;
        for (size_t j = 0; j < batch_size; ++j) {
            DataRecord record{
                current_block,
                all_keys[key_idx],
                random_values[j]
            };
            records.push_back(record);
        }
        
        // 使用当前策略写入
        metrics_collector_->start_write_timer();
        db_manager_->write_batch(records);
        metrics_collector_->stop_and_record_write(records.size(), calculate_bytes(records));
        
        current_block++;
    }
}
```

**目的**: 测试历史数据的批量写入性能（默认 1 亿条记录）

### 第二阶段：热点更新与查询

```cpp
void StrategyScenarioRunner::run_hotspot_update_phase() {
    while (total_processed < hotspot_update_target) {
        // 生成热点更新（10% 的键获得 90% 的更新）
        auto update_indices = data_generator_.generate_hotspot_update_indices(batch_size);
        
        // 创建并写入更新记录
        std::vector<DataRecord> records = create_update_records(update_indices);
        db_manager_->write_batch(records);
        
        // 周期性历史查询
        if (total_processed % query_interval == 0) {
            run_historical_queries(100);
        }
    }
}
```

**目的**: 模拟真实的区块链工作负载：
- 热点键分布（某些键更新频率更高）
- 周期性历史状态查询（模拟 `eth_call`）

### 历史查询模式

```cpp
void StrategyScenarioRunner::run_historical_queries(size_t query_count) {
    // 加权分布：10% 热点键，20% 中等键，70% 长尾键
    std::discrete_distribution<int> type_dist({1, 2, 7});
    
    for (size_t i = 0; i < query_count; ++i) {
        // 根据访问模式选择键类型
        int key_type = type_dist(gen);
        std::string key = select_key_by_type(key_type);
        
        // 使用当前策略查询最新值
        metrics_collector_->start_query_timer();
        auto result = db_manager_->query_latest_value(key);
        metrics_collector_->stop_and_record_query(result.has_value());
    }
}
```

**目的**: 使用真实的访问模式测试查询性能

## 现有策略

### 1. PageIndexStrategy（原始策略）

**数据结构**：
```
ChangeSet 表: {block_num}{address_slot} -> value
Index 表:     {page_num}{address_slot} -> [block_num_list]
```

**查询流程**：
1. 查找最新区块：扫描 Index 表的页面
2. 获取值：使用区块号从 ChangeSet 表读取

**特点**：
- 使用 RocksDB merge 操作符进行高效的索引更新
- 基于页面的索引减少内存开销
- 值查找需要两次查询开销

### 2. DirectVersionStrategy（新策略）

**数据结构**：
```
版本索引: VERSION|{address_slot}:{version} -> block_number  
数据存储: DATA|{block_num}|{address_slot} -> value
```

**查询流程**：
1. 构建最大版本键：`VERSION|{address_slot}:FFFFFFFF`
2. 使用 `seek_last` 查找最新版本
3. 从版本索引获取区块号
4. 从数据存储读取实际值

**特点**：
- 使用键前缀组织而非列族
- 单次查找操作即可获取最新值
- 两层存储将版本管理与数据分离

## 添加新策略

### 第1步：创建策略类

在 `src/strategies/` 目录中创建新文件：

```cpp
// src/strategies/my_custom_strategy.hpp
#pragma once
#include "../core/storage_strategy.hpp"
#include "../utils/logger.hpp"
#include <rocksdb/db.h>

class MyCustomStrategy : public IStorageStrategy {
public:
    bool initialize(rocksdb::DB* db) override;
    bool write_batch(rocksdb::DB* db, const std::vector<DataRecord>& records) override;
    std::optional<Value> query_latest_value(rocksdb::DB* db, const std::string& addr_slot) override;
    std::optional<Value> query_historical_value(rocksdb::DB* db, 
                                               const std::string& addr_slot, 
                                               BlockNum target_block) override;
    
    std::string get_strategy_name() const override { return "my_custom"; }
    std::string get_description() const override { 
        return "我的自定义存储策略，具有特定优化"; 
    }
    
    bool cleanup(rocksdb::DB* db) override;

private:
    // 你的策略特定的辅助方法和成员
    std::string build_my_key(const std::string& addr_slot, BlockNum block_num);
    std::optional<BlockNum> find_latest_block(rocksdb::DB* db, const std::string& addr_slot);
};
```

### 第2步：实现核心方法

```cpp
// src/strategies/my_custom_strategy.cpp
#include "my_custom_strategy.hpp"

bool MyCustomStrategy::initialize(rocksdb::DB* db) {
    // 创建你的表结构、列族或索引
    // 示例：为不同数据类型创建列族
    rocksdb::ColumnFamilyOptions cf_options;
    cf_options.compression = rocksdb::kNoCompression;
    
    // 创建你的特定存储结构
    // rocksdb::Status status = db->CreateColumnFamily(cf_options, "my_data_cf", &data_cf_);
    
    utils::log_info("MyCustomStrategy 初始化成功");
    return true;
}

bool MyCustomStrategy::write_batch(rocksdb::DB* db, const std::vector<DataRecord>& records) {
    rocksdb::WriteBatch batch;
    
    for (const auto& record : records) {
        // 将 DataRecord 转换为你的存储格式
        std::string key = build_my_key(record.addr_slot, record.block_num);
        
        // 写入到你的表结构
        batch.Put(key, record.value);
        
        // 如有需要，添加额外索引
        // batch.Put(index_key, index_value);
    }
    
    rocksdb::WriteOptions write_options;
    write_options.sync = false;
    auto status = db->Write(write_options, &batch);
    
    if (!status.ok()) {
        utils::log_error("写入失败: {}", status.ToString());
        return false;
    }
    
    return true;
}

std::optional<Value> MyCustomStrategy::query_latest_value(rocksdb::DB* db, const std::string& addr_slot) {
    // 实现你的最新值查询逻辑
    // 示例：使用前缀迭代来查找最新版本
    
    rocksdb::ReadOptions read_options;
    auto it = std::unique_ptr<rocksdb::Iterator>(db->NewIterator(read_options));
    
    std::string prefix = build_prefix(addr_slot);
    std::string latest_value;
    BlockNum latest_block = 0;
    
    // 遍历具有此前缀的键以找到最新区块
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
        BlockNum current_block = extract_block_from_key(it->key().ToString());
        if (current_block > latest_block) {
            latest_block = current_block;
            latest_value = it->value().ToString();
        }
    }
    
    return latest_value.empty() ? std::nullopt : std::make_optional(latest_value);
}
```

### 第3步：在工厂中注册

更新 `src/core/config.cpp`：

```cpp
// 在 StorageStrategyFactory::create_strategy() 中
if (normalized_type == "my_custom" || normalized_type == "mycustom") {
    return std::make_unique<MyCustomStrategy>();
}

// 在 StorageStrategyFactory::get_available_strategies() 中
return {"page_index", "direct_version", "my_custom"};
```

### 第4步：更新 CMakeLists.txt

添加到 `src/strategies/CMakeLists.txt`：

```cmake
add_library(strategies_lib
    page_index_strategy.cpp
    direct_version_strategy.cpp
    my_custom_strategy.cpp          # 添加你的策略
)
```

### 第5步：测试你的策略

```bash
# 构建项目
./scripts/build.sh

# 测试你的策略
./scripts/run.sh --strategy my_custom --initial_records 1000 --hotspot_updates 100 --clean

# 与现有策略比较
./scripts/test_both_strategies.sh
```

## 策略设计模式

### 模式1：简单键值存储

```cpp
// 直接键值：{address_slot}:{block_num} -> value
std::string build_key(const std::string& addr_slot, BlockNum block_num) {
    return addr_slot + ":" + std::to_string(block_num);
}

std::optional<Value> query_latest(rocksdb::DB* db, const std::string& addr_slot) {
    // 使用前缀扫描查找所有键，过滤最大区块号
    auto it = db->NewIterator(rocksdb::ReadOptions());
    std::string prefix = addr_slot + ":";
    
    BlockNum max_block = 0;
    std::string latest_value;
    
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
        BlockNum current_block = parse_block_from_key(it->key());
        if (current_block > max_block) {
            max_block = current_block;
            latest_value = it->value().ToString();
        }
    }
    
    return latest_value.empty() ? std::nullopt : std::make_optional(latest_value);
}
```

### 模式2：列族分离

```cpp
// 按类型将数据分离到不同的列族
class ColumnFamilyStrategy : public IStorageStrategy {
private:
    rocksdb::ColumnFamilyHandle* data_cf_;
    rocksdb::ColumnFamilyHandle* index_cf_;
    
public:
    bool initialize(rocksdb::DB* db) override {
        rocksdb::ColumnFamilyOptions options;
        db->CreateColumnFamily(options, "data", &data_cf_);
        db->CreateColumnFamily(options, "index", &index_cf_);
        return true;
    }
    
    bool write_batch(rocksdb::DB* db, const std::vector<DataRecord>& records) override {
        rocksdb::WriteBatch batch;
        for (const auto& record : records) {
            batch.Put(data_cf_, build_data_key(record), record.value);
            batch.Put(index_cf_, build_index_key(record), serialize_meta(record));
        }
        return db->Write(rocksdb::WriteOptions(), &batch).ok();
    }
};
```

### 模式3：带时间戳的版本化存储

```cpp
// 在键中包含时间戳以实现高效的时间范围查询
std::string build_versioned_key(const std::string& addr_slot, BlockNum block_num) {
    // 格式：{addr_slot}|{timestamp}|{block_num}
    uint64_t timestamp = get_block_timestamp(block_num);
    return addr_slot + "|" + std::to_string(timestamp) + "|" + std::to_string(block_num);
}

std::optional<Value> query_historical(rocksdb::DB* db, const std::string& addr_slot, BlockNum target_block) {
    uint64_t target_timestamp = get_block_timestamp(target_block);
    std::string seek_key = addr_slot + "|" + std::to_string(target_timestamp) + "|FFFFFFFF";
    
    auto it = db->NewIterator(rocksdb::ReadOptions());
    it->Seek(seek_key);
    
    // 查找时间戳 <= target_timestamp 的第一个键
    while (it->Valid() && it->key().ToString() > seek_key) {
        it->Prev();
    }
    
    if (it->Valid() && it->key().ToString().starts_with(addr_slot + "|")) {
        return it->value().ToString();
    }
    
    return std::nullopt;
}
```

### 模式4：基于哈希的键分布

```cpp
// 使用哈希在键空间中分布键以实现更好的负载均衡
std::string build_hashed_key(const std::string& addr_slot, BlockNum block_num) {
    // 创建哈希前缀以实现更好的分布
    uint64_t hash = std::hash<std::string>{}(addr_slot);
    uint16_t prefix = hash % 1024;  // 1024 个桶
    
    // 格式：{prefix:04x}|{addr_slot}|{block_num}
    std::ostringstream oss;
    oss << std::hex << std::setw(4) << std::setfill('0') << prefix 
        << "|" << addr_slot << "|" << block_num;
    return oss.str();
}
```

## 性能考虑

### 1. 键设计优化

- **前缀压缩**：设计具有公共前缀的键以获得更好的压缩效果
- **有序访问**：利用 RocksDB 的有序特性进行范围查询
- **键长度**：在可读性和存储效率之间取得平衡

### 2. 访问模式优化

- **顺序访问 vs 随机访问**：为你期望的访问模式进行优化
- **热点处理**：考虑为频繁访问的键使用缓存策略
- **批量操作**：使用 WriteBatch 进行原子性多键操作

### 3. RocksDB 配置

```cpp
// 示例策略特定的 RocksDB 优化
rocksdb::Options get_optimized_options() {
    rocksdb::Options options;
    options.create_if_missing = true;
    
    // 为读取密集型工作负载优化
    options.max_open_files = -1;
    options.use_fsync = false;
    options.stats_dump_period_sec = 60;
    
    // 策略特定优化
    options.compression = rocksdb::kNoCompression;  // 或 kSnappyCompression
    options.optimize_filters_for_hits = true;
    options.level_compaction_dynamic_level_bytes = true;
    
    return options;
}
```

### 4. 内存和磁盘使用

- **列族**：为不同的访问模式使用独立的列族
- **布隆过滤器**：为点查询启用，为范围扫描禁用
- **块大小**：根据典型值大小进行调整

## 示例和最佳实践

### 示例1：时间序列策略

```cpp
class TimeSeriesStrategy : public IStorageStrategy {
private:
    // 将数据存储在时间排序的桶中以实现高效的范围查询
    std::string build_time_bucket_key(uint64_t timestamp, const std::string& addr_slot) {
        uint64_t bucket = timestamp / (24 * 60 * 60);  // 每日桶
        return std::to_string(bucket) + "|" + addr_slot;
    }
    
public:
    std::optional<Value> query_time_range(rocksdb::DB* db, 
                                         const std::string& addr_slot,
                                         uint64_t start_time, 
                                         uint64_t end_time) {
        // 使用桶组织进行高效的时间范围查询
        auto it = db->NewIterator(rocksdb::ReadOptions());
        
        uint64_t start_bucket = start_time / (24 * 60 * 60);
        uint64_t end_bucket = end_time / (24 * 60 * 60);
        
        std::vector<Value> results;
        
        for (uint64_t bucket = start_bucket; bucket <= end_bucket; ++bucket) {
            std::string prefix = std::to_string(bucket) + "|" + addr_slot + "|";
            
            for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
                uint64_t record_time = extract_timestamp_from_key(it->key().ToString());
                if (record_time >= start_time && record_time <= end_time) {
                    results.push_back(it->value().ToString());
                }
            }
        }
        
        return results.empty() ? std::nullopt : std::make_optional(results.back());
    }
};
```

### 示例2：压缩值策略

```cpp
class CompressedValueStrategy : public IStorageStrategy {
private:
    std::string compress_value(const std::string& value) {
        // 实现压缩（例如 zlib、snappy）
        if (value.size() < 64) return value;  // 不压缩小值
        
        std::string compressed;
        // ... 压缩逻辑 ...
        return compressed;
    }
    
    std::string decompress_value(const std::string& compressed) {
        // ... 解压缩逻辑 ...
    }
    
public:
    bool write_batch(rocksdb::DB* db, const std::vector<DataRecord>& records) override {
        rocksdb::WriteBatch batch;
        
        for (const auto& record : records) {
            std::string compressed = compress_value(record.value);
            std::string key = build_key(record.addr_slot, record.block_num);
            batch.Put(key, compressed);
        }
        
        return db->Write(rocksdb::WriteOptions(), &batch).ok();
    }
    
    std::optional<Value> query_latest_value(rocksdb::DB* db, const std::string& addr_slot) override {
        auto compressed_result = find_latest_compressed_value(db, addr_slot);
        if (!compressed_result) return std::nullopt;
        
        return decompress_value(*compressed_result);
    }
};
```

### 最佳实践

1. **错误处理**：始终检查 RocksDB 状态码
2. **资源管理**：清理列族和迭代器
3. **日志记录**：使用日志工具进行调试
4. **测试**：在大规模基准测试之前先用小数据集测试
5. **文档**：记录你策略的权衡和使用场景

### 调试技巧

1. **使用 RocksDB 统计**：启用统计信息以了解性能
2. **记录键操作**：记录读/写操作以进行调试
3. **使用小数据测试**：在 1 亿条记录之前先从 1000 条开始
4. **比较策略**：始终与基线策略进行比较
5. **监控资源**：监视内存使用和磁盘 I/O

## 结论

本指南提供了 RocksDB 基准测试实现的全面概述，以及如何用新的存储策略扩展它。该架构设计为灵活且可扩展，允许您试验不同的存储方法，同时保持公平的性能比较。

关键要点：
- **实现接口**：遵循 IStorageStrategy 接口
- **为用例设计**：优化键结构和访问模式
- **公平测试**：使用相同的工作负载进行比较
- **记录权衡**：解释何时以及为什么使用你的策略

该基准测试工具为在类似区块链的工作负载中评估不同的 RocksDB 存储策略提供了坚实的基础。通过遵循本指南，你可以轻松添加新策略，并为理解不同用例的最佳存储模式做出贡献。
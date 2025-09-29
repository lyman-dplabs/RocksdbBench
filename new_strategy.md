# 双RocksDB范围分区存储策略设计方案

## 概述

本文档详细说明了一个新的存储策略，使用两个独立的RocksDB实例来优化类区块链数据访问模式，特别针对大规模数据集（如20亿KV）的热、中、冷数据分布进行优化。该设计采用范围分区和分层缓存策略，在内存使用和查询性能之间取得平衡。

## 架构概览

```
┌─────────────────────────────────────────────────────────────────┐
│                    应用层                                        │
├─────────────────────────────────────────────────────────────────┤
│                  StrategyDBManager                               │
├─────────────────────────────────────────────────────────────────┤
│    DualRocksDBStrategy                                           │
│    ┌─────────────────────┐    ┌───────────────────────────────┐  │
│    │   RangeIndexDB      │    │      DataStorageDB            │  │
│    │   (RocksDB 0)       │    │      (RocksDB 1)              │  │
│    │                     │    │                               │  │
│    │  addr -> [ranges]   │    │  range|addr|block -> value    │  │
│    │                     │    │                               │  │
│    │  多级缓存系统        │    │  数据预取缓存                 │  │
│    └─────────────────────┘    └───────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## 核心设计原则

### 1. 范围分区策略
- **范围大小**：每10,000个块为一个范围（可配置）
- **范围计算**：`range_number = block_num / 10000`
- **性能考虑**：按高性能链1秒3个块计算，每个范围约3+小时才会有一次更新

### 2. 双数据库分离
- **RangeIndexDB**：存储地址到修改范围的映射关系
- **DataStorageDB**：存储实际数据，使用范围前缀组织

### 3. 智能缓存管理
- **内存限制优先**：根据可用内存动态调整缓存策略
- **分层缓存**：L1热点缓存 + L2范围缓存 + L3预取缓存
- **自适应算法**：基于访问模式和内存压力自动调整

## 数据结构设计

### RangeIndexDB 架构
```
键: {address_slot}
值: 序列化的范围列表

范围列表格式:
[range_number_1, range_number_2, range_number_3, ...]

示例:
"0x1234...:0" -> [0, 3, 7]  // 在范围 0, 3, 7 中有修改
```

### DataStorageDB 架构
```
键: {range_number:06d}|{address_slot}|{block_num:012d}
值: {实际数据}

示例:
"000000|0x1234...:0|000000000045" -> "0x5678..."
```

## 灵活的缓存策略设计

### 问题分析
对于20亿KV的数据集，即使1%的热数据也需要2亿个缓存条目，传统的1:2:7比例在内存有限的情况下不可行。

### 解决方案：基于内存压力的自适应缓存

```cpp
enum class CacheLevel {
    HOT,        // 热点数据：完整数据缓存
    MEDIUM,     // 中等数据：范围列表缓存
    COLD,       // 冷数据：无缓存
    PASSIVE     // 被动缓存：仅查询时缓存
};

struct AdaptiveCacheConfig {
    size_t max_memory_bytes;           // 最大内存限制
    double hot_data_ratio = 0.01;      // 热数据比例 (1%)
    double medium_data_ratio = 0.05;   // 中等数据比例 (5%)
    size_t avg_entry_size = 200;       // 平均条目大小
    bool enable_memory_monitor = true;  // 启用内存监控
    uint32_t sampling_interval = 1000; // 采样间隔
};
```

### 分层缓存实现

```cpp
class AdaptiveCacheManager {
private:
    // L1缓存：热点数据完整缓存
    std::unordered_map<std::string, CacheEntry> hot_cache_;
    
    // L2缓存：中等数据范围列表缓存
    std::unordered_map<std::string, std::vector<uint32_t>> range_cache_;
    
    // L3缓存：被动查询缓存
    std::unordered_map<std::string, std::string> passive_cache_;
    
    // 内存监控
    size_t current_memory_usage_ = 0;
    size_t max_memory_limit_;
    std::mutex cache_mutex_;
    
    // 访问统计
    std::unordered_map<std::string, AccessStats> access_stats_;
    
public:
    void update_access_pattern(const std::string& key, CacheLevel level);
    CacheLevel determine_cache_level(const std::string& key) const;
    void manage_memory_pressure();
    void evict_least_used();
};
```

### 动态缓存级别判断

```cpp
CacheLevel AdaptiveCacheManager::determine_cache_level(const std::string& key) const {
    auto stats = access_stats_.find(key);
    if (stats == access_stats_.end()) {
        return CacheLevel::PASSIVE;
    }
    
    const auto& access_info = stats->second;
    auto now = std::chrono::system_clock::now();
    auto age = std::chrono::duration_cast<std::chrono::seconds>(now - access_info.last_access);
    
    // 基于访问频率和内存压力动态判断
    double access_frequency = access_info.access_count / std::max(1.0, age.count());
    
    if (access_frequency > 100.0 && has_memory_for_hot_data()) {
        return CacheLevel::HOT;
    } else if (access_frequency > 10.0 && has_memory_for_medium_data()) {
        return CacheLevel::MEDIUM;
    } else {
        return CacheLevel::PASSIVE;
    }
}

bool AdaptiveCacheManager::has_memory_for_hot_data() const {
    size_t projected_usage = current_memory_usage_ + 200; // 假设热点条目200字节
    return projected_usage < max_memory_limit_ * hot_data_ratio;
}
```

## 详细实现设计

### 核心类结构

```cpp
class DualRocksDBStrategy : public IStorageStrategy {
private:
    std::unique_ptr<rocksdb::DB> range_index_db_;
    std::unique_ptr<rocksdb::DB> data_storage_db_;
    
    // 自适应缓存管理器
    std::unique_ptr<AdaptiveCacheManager> cache_manager_;
    
    // 配置参数
    struct Config {
        uint32_t range_size = 10000;
        size_t max_cache_memory = 1024 * 1024 * 1024; // 1GB默认
        double hot_cache_ratio = 0.01;
        double medium_cache_ratio = 0.05;
        bool enable_compression = true;
        bool enable_bloom_filters = true;
    } config_;
    
public:
    bool initialize(rocksdb::DB* main_db) override;
    bool write_batch(rocksdb::DB* db, const std::vector<DataRecord>& records) override;
    std::optional<Value> query_latest_value(rocksdb::DB* db, const std::string& addr_slot) override;
    std::optional<Value> query_historical_value(rocksdb::DB* db, const std::string& addr_slot, BlockNum target_block) override;
    
    std::string get_strategy_name() const override { return "dual_rocksdb_adaptive"; }
    std::string get_description() const override { 
        return "双RocksDB范围分区存储，具有自适应内存管理"; 
    }
    
    bool cleanup(rocksdb::DB* db) override;

private:
    // 核心操作
    uint32_t calculate_range(BlockNum block_num) const;
    std::string build_data_key(uint32_t range_num, const std::string& addr_slot, BlockNum block_num) const;
    std::optional<BlockNum> find_latest_block_in_range(rocksdb::DB* db, uint32_t range_num, const std::string& addr_slot) const;
    
    // 智能缓存接口
    void update_access_pattern(const std::string& addr_slot, bool is_write);
    CacheLevel get_cache_level(const std::string& addr_slot) const;
    
    // 范围管理
    bool update_range_index(rocksdb::DB* db, const std::string& addr_slot, uint32_t range_num);
    std::vector<uint32_t> get_address_ranges(rocksdb::DB* db, const std::string& addr_slot) const;
    
    // 内存管理
    void check_memory_pressure();
    void optimize_cache_usage();
};
```

### 写入操作实现

```cpp
bool DualRocksDBStrategy::write_batch(rocksdb::DB* db, const std::vector<DataRecord>& records) {
    rocksdb::WriteBatch range_batch;
    rocksdb::WriteBatch data_batch;
    
    for (const auto& record : records) {
        uint32_t range_num = calculate_range(record.block_num);
        
        // 更新范围索引
        update_range_index(db, record.addr_slot, range_num);
        
        // 存储数据（带范围前缀）
        std::string data_key = build_data_key(range_num, record.addr_slot, record.block_num);
        data_batch.Put(data_key, record.value);
        
        // 更新访问模式（写入也算访问）
        update_access_pattern(record.addr_slot, true);
    }
    
    // 检查内存压力
    check_memory_pressure();
    
    // 写入两个数据库
    auto range_status = range_index_db_->Write(rocksdb::WriteOptions(), &range_batch);
    auto data_status = data_storage_db_->Write(rocksdb::WriteOptions(), &data_batch);
    
    return range_status.ok() && data_status.ok();
}
```

### 查询操作实现

```cpp
std::optional<Value> DualRocksDBStrategy::query_latest_value(rocksdb::DB* db, const std::string& addr_slot) {
    CacheLevel level = get_cache_level(addr_slot);
    
    // L1缓存检查：热点数据
    if (level == CacheLevel::HOT) {
        auto cached = cache_manager_->get_hot_data(addr_slot);
        if (cached) {
            return cached;
        }
    }
    
    // 获取地址的范围列表
    std::vector<uint32_t> ranges;
    if (level >= CacheLevel::MEDIUM) {
        // 从缓存获取范围列表
        ranges = cache_manager_->get_range_list(addr_slot);
    }
    
    if (ranges.empty()) {
        // 从数据库获取范围列表
        ranges = get_address_ranges(db, addr_slot);
        if (level >= CacheLevel::MEDIUM) {
            cache_manager_->cache_range_list(addr_slot, ranges);
        }
    }
    
    if (ranges.empty()) {
        return std::nullopt;
    }
    
    // 找到最新范围并搜索最新块
    uint32_t latest_range = *std::max_element(ranges.begin(), ranges.end());
    auto latest_block = find_latest_block_in_range(db, latest_range, addr_slot);
    
    if (!latest_block) {
        return std::nullopt;
    }
    
    // 获取值
    std::string data_key = build_data_key(latest_range, addr_slot, *latest_block);
    std::string value;
    auto status = data_storage_db_->Get(rocksdb::ReadOptions(), data_key, &value);
    
    if (status.ok()) {
        // 根据缓存级别决定是否缓存结果
        if (level == CacheLevel::HOT) {
            cache_manager_->cache_hot_data(addr_slot, value);
        } else if (level == CacheLevel::PASSIVE) {
            cache_manager_->cache_passive_data(addr_slot, value);
        }
        
        // 更新访问模式
        update_access_pattern(addr_slot, false);
        return value;
    }
    
    return std::nullopt;
}
```

## 内存管理策略

### 1. 内存监控

```cpp
void DualRocksDBStrategy::check_memory_pressure() {
    static size_t last_check = 0;
    size_t current_usage = cache_manager_->get_memory_usage();
    
    if (current_usage > config_.max_cache_memory * 0.9) {
        // 内存压力超过90%，触发优化
        optimize_cache_usage();
    }
    
    // 定期重整缓存结构
    if (current_usage - last_check > config_.max_cache_memory * 0.1) {
        cache_manager_->reorganize_cache();
        last_check = current_usage;
    }
}
```

### 2. 动态缓存优化

```cpp
void DualRocksDBStrategy::optimize_cache_usage() {
    auto current_usage = cache_manager_->get_memory_usage();
    
    if (current_usage > config_.max_cache_memory) {
        // 内存超限，强制清理
        cache_manager_->evict_by_strategy();
    } else if (current_usage > config_.max_cache_memory * 0.8) {
        // 内存压力高，调整缓存比例
        cache_manager_->adjust_cache_ratios(0.8); // 减少缓存比例
    }
    
    // 基于访问模式重新分级
    cache_manager_->reclassify_cache_entries();
}
```

## 性能优化策略



## 大规模数据场景优化

### 1. 分片策略（默认关闭）

**分片策略的目的**：解决单一RocksDB实例在大规模数据集下的性能瓶颈，通过水平分摊数据到多个独立的存储实例中。

**工作原理**：
- 将数据按键的哈希值分配到不同的分片（shard）
- 每个分片运行独立的双RocksDB实例
- 查询时先计算目标分片，再在对应分片中执行查询

**具体实现**：

```cpp
class ShardManager {
private:
    std::vector<std::unique_ptr<DualRocksDBStrategy>> shards_;
    size_t shard_count_;
    
public:
    // 根据键计算分片索引
    size_t get_shard_index(const std::string& key) {
        std::hash<std::string> hasher;
        return hasher(key) % shard_count_;
    }
    
    // 获取指定分片实例
    DualRocksDBStrategy* get_shard(const std::string& key) {
        return shards_[get_shard_index(key)].get();
    }
    
    // 写入数据：自动路由到正确分片
    bool write_to_shard(const std::string& addr_slot, const DataRecord& record) {
        auto shard = get_shard(addr_slot);
        return shard->write_batch({record});
    }
    
    // 查询数据：自动路由到正确分片
    std::optional<Value> query_from_shard(const std::string& addr_slot) {
        auto shard = get_shard(addr_slot);
        return shard->query_latest_value(addr_slot);
    }
};
```

**分片策略的特点**：
- **水平扩展**：数据分散到多个实例，减少单个实例负载
- **并行处理**：不同分片可以并行处理查询
- **内存隔离**：每个分片有独立的缓存和内存管理
- **负载均衡**：通过哈希确保数据均匀分布

**适用场景**：
- 单个分片内存或CPU达到瓶颈
- 需要处理超过10亿条记录
- 对查询性能有极高要求

### 2. 动态缓存优化（默认关闭）

**动态缓存优化的目的**：根据运行时的访问模式和内存压力，自动调整缓存策略和参数。

**工作原理**：
- 监控每个键的访问频率和模式
- 根据内存压力动态调整缓存级别
- 自动进行缓存淘汰和重分级

### 3. Seek-Last查找优化（核心机制，强制启用）

**Seek-Last优化的目的**：利用RocksDB的有序性，高效查找最新版本的数据。

**工作原理**：
```
传统方式：遍历所有键找到最大区块号
↓
Seek-Last方式：构建最大可能键，直接查找前一个存在的键

键格式：{range}|{address_slot}|{block_num}
查找：构建 {range}|{address_slot}|FFFFFFFFFFFFFFFF
然后 SeekForPrev 找到实际存在的最大键
```

**重要说明**：Seek-Last优化是双RocksDB策略的核心查找机制，不是可选配置项。整个策略的设计和性能都依赖于这种查找方式，因此**必须启用且无法关闭**。

**具体实现**：

```cpp
std::optional<BlockNum> DualRocksDBStrategy::find_latest_block_in_range(
    rocksdb::DB* db, uint32_t range_num, const std::string& addr_slot) const {
    
    // 双RocksDB策略必须使用Seek-Last优化，这是核心查找机制
    std::string prefix = build_data_prefix(range_num, addr_slot);
    std::string seek_key = prefix + "FFFFFFFFFFFFFFFF"; // 最大可能的block_num
    
    rocksdb::ReadOptions options;
    auto it = std::unique_ptr<rocksdb::Iterator>(db->NewIterator(options));
    
    // 直接定位到可能的最大键位置
    it->SeekForPrev(seek_key);
    
    if (it->Valid() && it->key().starts_with(prefix)) {
        return extract_block_from_key(it->key().ToString());
    }
    
    return std::nullopt;
}
```

**Seek-Last优化的优势**：
- **时间复杂度**：从O(n)降低到O(1)
- **磁盘I/O**：减少大量不必要的磁盘读取
- **性能提升**：特别适合稀疏更新的地址

**使用示例**：
```cpp
// Seek-Last优化是双RocksDB策略的核心机制，无需配置，始终启用

// 启用动态缓存优化（高级功能）
{
    "performance": {
        "enable_dynamic_cache_optimization": true  // 默认关闭
    }
}

// 启用分片策略（大规模数据）
{
    "scalability": {
        "enable_sharding": true,         // 默认关闭
        "shard_count": 4                // 使用4个分片
    }
}
```

## 边界情况处理

### 1. 范围边界处理
```cpp
uint32_t DualRocksDBStrategy::calculate_range(BlockNum block_num) const {
    // 处理边界情况
    if (block_num == std::numeric_limits<BlockNum>::max()) {
        return std::numeric_limits<uint32_t>::max();
    }
    return block_num / range_size_;
}
```

### 2. 数据一致性检查
```cpp
bool DualRocksDBStrategy::validate_consistency(const std::string& addr_slot) {
    auto ranges = get_address_ranges(range_index_db_.get(), addr_slot);
    
    for (auto range_num : ranges) {
        if (!has_data_in_range(range_num, addr_slot)) {
            // 范围索引存在但数据缺失
            repair_missing_data(range_num, addr_slot);
            return false;
        }
    }
    return true;
}
```

## 配置参数设计

```cpp
struct DualRocksDBConfig {
    // 基础参数
    uint32_t range_size = 10000;           // 每个范围的块数
    std::string db_path_prefix = "./db_";   // 数据库路径前缀
    
    // 内存管理
    size_t max_memory_bytes = 1024 * 1024 * 1024; // 最大内存使用
    double hot_cache_ratio = 0.01;          // 热缓存比例
    double medium_cache_ratio = 0.05;       // 中等缓存比例
    
    // 性能参数
    bool enable_compression = true;         // 启用压缩
    bool enable_bloom_filters = true;      // 启用布隆过滤器
    bool enable_prefetch = true;           // 启用预取
    uint32_t prefetch_threads = 2;          // 预取线程数
    
    // 大规模数据参数
    uint64_t expected_key_count = 0;        // 预期键数量
    bool enable_sharding = false;           // 启用分片
    size_t shard_count = 1;                 // 分片数量
    
    // 监控参数
    bool enable_metrics = true;            // 启用指标收集
    uint32_t metrics_interval = 60;         // 指标收集间隔(秒)
    
    // 序列化方法
    std::string to_string() const {
        json j;
        j["range_size"] = range_size;
        j["max_memory_bytes"] = max_memory_bytes;
        j["hot_cache_ratio"] = hot_cache_ratio;
        j["medium_cache_ratio"] = medium_cache_ratio;
        j["enable_compression"] = enable_compression;
        j["enable_bloom_filters"] = enable_bloom_filters;
        j["expected_key_count"] = expected_key_count;
        j["enable_sharding"] = enable_sharding;
        j["shard_count"] = shard_count;
        return j.dump(4);
    }
    
    static DualRocksDBConfig from_string(const std::string& config_str) {
        auto j = json::parse(config_str);
        DualRocksDBConfig config;
        config.range_size = j.value("range_size", 10000);
        config.max_memory_bytes = j.value("max_memory_bytes", 1024*1024*1024);
        config.hot_cache_ratio = j.value("hot_cache_ratio", 0.01);
        config.medium_cache_ratio = j.value("medium_cache_ratio", 0.05);
        config.enable_compression = j.value("enable_compression", true);
        config.enable_bloom_filters = j.value("enable_bloom_filters", true);
        config.expected_key_count = j.value("expected_key_count", 0);
        config.enable_sharding = j.value("enable_sharding", false);
        config.shard_count = j.value("shard_count", 1);
        return config;
    }
};
```

## 预期性能特征

### 写入性能
- **吞吐量**：高（批量写入两个数据库）
- **延迟**：中等（双写开销）
- **可扩展性**：良好（范围分区）

### 读取性能
- **热点数据**：优秀（内存缓存）
- **中等数据**：良好（范围缓存，单次查找）
- **冷数据**：可接受（磁盘读取，范围受限扫描）

### 内存使用
- **自适应调整**：根据可用内存动态调整缓存策略
- **分层管理**：L1热点 + L2范围 + L3被动缓存
- **内存保护**：防止内存溢出，优先保证系统稳定性

## 测试策略

### 1. 单元测试
- 范围计算准确性
- 缓存管理逻辑
- 内存压力处理
- 键构建和解析

### 2. 集成测试
- 双数据库一致性
- 端到端读写循环
- 缓存效果验证
- 性能基准测试

### 3. 大规模测试
- 20亿KV数据集测试
- 内存压力测试
- 长时间稳定性测试
- 故障恢复测试

## 监控和指标

```cpp
struct PerformanceMetrics {
    // 基础指标
    uint64_t total_writes = 0;
    uint64_t total_reads = 0;
    double avg_write_latency = 0.0;
    double avg_read_latency = 0.0;
    
    // 缓存指标
    size_t cache_hits = 0;
    size_t cache_misses = 0;
    double cache_hit_rate = 0.0;
    size_t memory_usage = 0;
    
    // 分层缓存指标
    size_t hot_cache_size = 0;
    size_t medium_cache_size = 0;
    size_t passive_cache_size = 0;
    
    // 性能指标
    std::vector<double> recent_latencies;
    uint64_t throughput_per_second = 0;
    
    void update_cache_hit_rate() {
        cache_hit_rate = static_cast<double>(cache_hits) / 
                        (cache_hits + cache_misses);
    }
    
    void record_latency(double latency_ms) {
        recent_latencies.push_back(latency_ms);
        if (recent_latencies.size() > 1000) {
            recent_latencies.erase(recent_latencies.begin());
        }
    }
    
    double get_p95_latency() const {
        if (recent_latencies.empty()) return 0.0;
        std::vector<double> sorted = recent_latencies;
        std::sort(sorted.begin(), sorted.end());
        return sorted[sorted.size() * 0.95];
    }
};
```

## 脚本配置和部署

### 现有脚本支持分析

根据 `scripts/README.md`，当前脚本系统支持以下参数：

**基础参数：**
- `--db-path PATH` - 数据库路径（默认：./rocksdb_data）
- `--strategy STRATEGY` - 存储策略：`page_index` 或 `direct_version`（默认：page_index）
- `--clean` - 清理现有数据
- `--initial-records N` - 初始记录数量
- `--hotspot-updates N` - 热点更新数量

### 需要新增的参数

为了支持新的 `dual_rocksdb_adaptive` 策略，需要在现有脚本中添加以下参数：

#### 1. 缓存配置参数
```bash
--max-cache-memory SIZE      # 最大缓存内存（如：1GB, 512MB）
--hot-cache-ratio RATIO     # 热缓存比例（默认：0.01）
--medium-cache-ratio RATIO  # 中等缓存比例（默认：0.05）
--enable-memory-monitor     # 启用内存监控（默认：true）
```

#### 2. 策略特定参数
```bash
--range-size SIZE           # 范围大小（默认：10000）
--enable-compression        # 启用压缩（默认：true）
--enable-bloom-filters      # 启用布隆过滤器（默认：true）
--enable-prefetch           # 启用预取（默认：true）
--prefetch-threads N        # 预取线程数（默认：2）
```

#### 3. 大规模数据参数
```bash
--expected-key-count N      # 预期键数量（用于优化）
--enable-sharding           # 启用分片（默认：false）
--shard-count N             # 分片数量（默认：1）
```

#### 4. 监控参数
```bash
--enable-metrics            # 启用指标收集（默认：true）
--metrics-interval SECONDS  # 指标收集间隔（默认：60）
--config-file PATH          # 配置文件路径（JSON格式）
```

### 脚本修改建议

#### 1. 修改 `run.sh`

需要在 `run.sh` 中添加新参数解析：

```bash
# 在参数解析部分添加
--max-cache-memory)
    MAX_CACHE_MEMORY="--max_cache_memory $2"
    shift 2
    ;;
--hot-cache-ratio)
    HOT_CACHE_RATIO="--hot_cache_ratio $2"
    shift 2
    ;;
--range-size)
    RANGE_SIZE="--range_size $2"
    shift 2
    ;;
--config-file)
    CONFIG_FILE="--config_file $2"
    shift 2
    ;;
```

#### 2. 更新策略验证

```bash
# 修改策略验证逻辑
if [[ "$STRATEGY" != "page_index" && "$STRATEGY" != "direct_version" && "$STRATEGY" != "dual_rocksdb_adaptive" ]]; then
    echo "Error: Invalid strategy '$STRATEGY'. Must be 'page_index', 'direct_version', or 'dual_rocksdb_adaptive'"
    exit 1
fi
```

#### 3. 新增专用脚本

建议创建 `scripts/run_dual_rocksdb.sh` 专门用于双RocksDB策略：

```bash
#!/bin/bash
# Dual RocksDB 专用运行脚本
set -e

# 默认值
DB_PATH="./dual_rocksdb_data"
STRATEGY="dual_rocksdb_adaptive"
MAX_CACHE_MEMORY="1GB"
RANGE_SIZE="10000"
EXPECTED_KEY_COUNT="100000000"  # 1亿

# 解析双RocksDB特定参数...
```

### 配置文件支持

为了更好地支持复杂配置，建议采用分层配置文件结构，将通用配置与策略特有配置分离管理：

#### 1. 通用配置文件 `config.json`

```json
{
    "database": {
        "path": "./rocksdb_data",
        "clean_on_start": false
    },
    "benchmark": {
        "initial_records": 100000000,
        "hotspot_updates": 10000000,
        "batch_size": 10000
    },
    "common": {
        "enable_compression": true,
        "enable_bloom_filters": true,
        "enable_metrics": true,
        "metrics_interval": 60
    }
}
```

#### 2. 策略特有配置文件 `strategy_configs/`

##### `dual_rocksdb_adaptive.json` ⭐ **仅限双RocksDB策略使用**

> **重要提示**：此配置文件包含的配置项专门为双RocksDB自适应策略设计，其他策略（如 `page_index`、`direct_version`）**不能使用**这些配置项。每个策略都有自己独立的配置文件。

```json
{
    "strategy_type": "dual_rocksdb_adaptive",
    "description": "双RocksDB范围分区存储，自适应缓存管理",
    
    // === 核心架构配置（双RocksDB特有） ===
    "architecture": {
        "range_size": 10000,                                    // 【必填】范围大小，其他策略无此概念
        "range_index_db_path": "./range_index_db",             // 【必填】范围索引数据库路径
        "data_storage_db_path": "./data_storage_db",           // 【必填】数据存储数据库路径
        "enable_dual_instance": true                           // 【必填】启用双实例模式
    },
    
    // === 缓存管理配置（双RocksDB特有） ===
    "cache": {
        "max_memory_bytes": 1073741824,                        // 【特有】自适应缓存最大内存
        "hot_cache_ratio": 0.01,                               // 【特有】L1热点缓存比例
        "medium_cache_ratio": 0.05,                            // 【特有】L2中等缓存比例
        "passive_cache_ratio": 0.1,                            // 【特有】L3被动缓存比例
        "enable_memory_monitor": true,                         // 【特有】内存压力监控
        "memory_pressure_threshold": 0.9,                       // 【特有】内存压力阈值
        "cache_eviction_policy": "lru",                        // 【特有】缓存淘汰策略
        "enable_ttl": true,                                     // 【特有】启用TTL过期
        "hot_cache_ttl_seconds": 3600,                         // 【特有】热点缓存TTL
        "medium_cache_ttl_seconds": 1800                       // 【特有】中等缓存TTL
    },
    
    // === 性能优化配置（双RocksDB特有） ===
    "performance": {
        "enable_dynamic_cache_optimization": false             // 【特有】动态缓存优化（默认关闭）
    },
    
    // === 大规模数据配置（双RocksDB特有） ===
    "scalability": {
        "expected_key_count": 2000000000,                      // 【特有】预期键数量
        "enable_sharding": false,                              // 【特有】启用分片模式（默认关闭）
        "shard_count": 1                                       // 【特有】分片数量
    },
    
    // === 数据一致性配置（双RocksDB特有） ===
    "consistency": {
        "enable_validation": true,                             // 【特有】启用数据一致性验证
        "validation_interval": 1000,                           // 【特有】验证间隔
        "auto_repair": true,                                   // 【特有】自动修复损坏数据
        "enable_transaction_log": false,                       // 【特有】启用事务日志
        "sync_mode": "async"                                   // 【特有】同步模式
    },
    
    // === 监控和调试配置（双RocksDB特有） ===
    "monitoring": {
        "enable_detailed_metrics": true,                       // 【特有】启用详细指标收集
        "metrics_collection_interval": 60,                     // 【特有】指标收集间隔
        "enable_access_pattern_tracking": true,                // 【特有】访问模式追踪
        "enable_memory_profiling": true,                       // 【特有】内存分析
        "log_level": "info",                                   // 【特有】日志级别
        "enable_performance_tracing": false                     // 【特有】性能追踪
    }
}
```

##### `page_index.json`（现有策略配置）
```json
{
    "strategy_type": "page_index",
    "description": "传统ChangeSet + Index表存储",
    
    "pages": {
        "page_size": 1000,                                     // 【PageIndex特有】页面大小
        "merge_operator_enabled": true                         // 【PageIndex特有】启用合并操作符
    },
    
    "bloom_filters": {
        "enabled": true,                                       // 【通用】布隆过滤器启用
        "false_positive_rate": 0.01                           // 【通用】误报率
    }
}
```

##### `direct_version.json`（现有策略配置）
```json
{
    "strategy_type": "direct_version",
    "description": "双层版本索引存储",
    
    "versioning": {
        "version_key_prefix": "VERSION|",                     // 【DirectVersion特有】版本键前缀
        "data_key_prefix": "DATA|",                           // 【DirectVersion特有】数据键前缀
        "seek_last_enabled": true                             // 【DirectVersion特有】启用seek-last
    }
}
```

#### 配置文件使用规则

1. **策略隔离原则**
   - 每个策略只能使用自己配置文件中的配置项
   - 双RocksDB策略的配置项（如 `architecture.range_size`）其他策略无法识别
   - 通用配置项（如压缩、布隆过滤器）可在所有策略中使用

2. **配置文件命名规范**
   ```
   strategy_configs/
   ├── dual_rocksdb_adaptive.json     # 双RocksDB自适应策略（特有配置最多）
   ├── page_index.json               # 页面索引策略（配置较少）
   ├── direct_version.json           # 直接版本策略（配置较少）
   └── [future_strategy].json        # 未来扩展策略
   ```

3. **配置验证机制**
   - 策略启动时会验证配置文件的完整性
   - 发现未知配置项时会发出警告
   - 缺少必需配置项时会启动失败

##### `page_index.json`（现有策略）
```json
{
    "strategy_type": "page_index",
    "description": "传统ChangeSet + Index表存储",
    
    "pages": {
        "page_size": 1000,
        "merge_operator_enabled": true
    },
    
    "bloom_filters": {
        "enabled": true,
        "false_positive_rate": 0.01
    }
}
```

##### `direct_version.json`（现有策略）
```json
{
    "strategy_type": "direct_version",
    "description": "双层版本索引存储",
    
    "versioning": {
        "version_key_prefix": "VERSION|",
        "data_key_prefix": "DATA|",
        "seek_last_enabled": true
    }
}
```

#### 3. 运行时配置文件 `runtime_config.json`

```json
{
    "selected_strategy": "dual_rocksdb_adaptive",
    "config_overrides": {
        "cache.max_memory_bytes": "4GB",
        "scalability.expected_key_count": 2000000000
    },
    "environment": "production",
    "debug_mode": false
}
```

### 配置管理器设计

```cpp
class ConfigManager {
private:
    std::unordered_map<std::string, json> strategy_configs_;
    json global_config_;
    json runtime_config_;
    
public:
    bool load_config(const std::string& config_path);
    json get_strategy_config(const std::string& strategy_name);
    void apply_overrides(const json& overrides);
    
    template<typename T>
    T get_config_value(const std::string& path, T default_value) {
        // 支持 "cache.max_memory_bytes" 这样的路径访问
        std::vector<std::string> parts = split_path(path);
        return get_nested_value(global_config_, parts, default_value);
    }
};
```

### 配置文件使用方式

#### 1. 基础使用
```bash
# 使用默认配置
./scripts/run.sh --strategy dual_rocksdb_adaptive --clean

# 指定配置文件
./scripts/run.sh --strategy dual_rocksdb_adaptive \
    --config-file ./strategy_configs/dual_rocksdb_adaptive.json \
    --clean
```

#### 2. 配置覆盖
```bash
# 运行时覆盖配置
./scripts/run.sh --strategy dual_rocksdb_adaptive \
    --config-file ./strategy_configs/dual_rocksdb_adaptive.json \
    --override "cache.max_memory_bytes=4GB" \
    --override "scalability.expected_key_count=2000000000" \
    --clean
```

#### 3. 环境变量配置
```bash
# 通过环境变量设置
export DUAL_ROCKSDB_CACHE_MEMORY="4GB"
export DUAL_ROCKSDB_RANGE_SIZE="50000"
export DUAL_ROCKSDB_EXPECTED_KEYS="2000000000"

./scripts/run.sh --strategy dual_rocksdb_adaptive --clean
```

#### 4. 预设配置
```bash
# 使用预设配置（开发、测试、生产）
./scripts/run.sh --strategy dual_rocksdb_adaptive \
    --preset development \
    --clean

./scripts/run.sh --strategy dual_rocksdb_adaptive \
    --preset production \
    --clean
```

### 预设配置文件 `presets/`

#### `development.json`
```json
{
    "preset_name": "development",
    "description": "开发环境配置",
    "overrides": {
        "cache.max_memory_bytes": 536870912,
        "cache.hot_cache_ratio": 0.05,
        "performance.enable_prefetch": false,
        "monitoring.log_level": "debug"
    }
}
```

#### `testing.json`
```json
{
    "preset_name": "testing",
    "description": "测试环境配置",
    "overrides": {
        "cache.max_memory_bytes": 1073741824,
        "scalability.expected_key_count": 100000000,
        "consistency.enable_validation": true,
        "monitoring.enable_detailed_metrics": true
    }
}
```

#### `production.json`
```json
{
    "preset_name": "production",
    "description": "生产环境配置",
    "overrides": {
        "cache.max_memory_bytes": 8589934592,
        "performance.enable_prefetch": true,
        "consistency.sync_mode": "async",
        "monitoring.log_level": "info"
    }
}
```

### 配置验证和错误处理

```cpp
class ConfigValidator {
public:
    bool validate_strategy_config(const json& config, const std::string& strategy_type);
    std::vector<std::string> get_validation_errors() const;
    
private:
    bool validate_cache_config(const json& cache_config);
    bool validate_scalability_config(const json& scalability_config);
    bool validate_performance_config(const json& performance_config);
};
```

### 配置热更新

```cpp
class HotReloadManager {
private:
    std::thread config_watcher_;
    std::atomic<bool> running_{true};
    std::chrono::seconds check_interval_{30};
    
public:
    void start_watching(const std::string& config_path);
    void stop_watching();
    
    std::function<void(const json&)> on_config_changed;
};
```

这种配置文件设计的优势：

1. **分层管理**：通用配置与策略特有配置分离
2. **类型安全**：每个策略的配置文件独立维护
3. **易于扩展**：新增策略只需添加对应的配置文件
4. **灵活覆盖**：支持运行时配置覆盖
5. **环境适配**：预设配置适配不同环境
6. **类型验证**：配置验证确保参数正确性
7. **热更新**：支持运行时配置更新（可选）

### 使用示例

#### 1. 基础使用
```bash
# 使用默认配置运行双RocksDB策略
./scripts/run.sh --strategy dual_rocksdb_adaptive --clean

# 自定义缓存大小
./scripts/run.sh --strategy dual_rocksdb_adaptive --max-cache-memory 2GB --clean
```

#### 2. 大规模数据测试
```bash
# 20亿KV数据集测试
./scripts/run.sh --strategy dual_rocksdb_adaptive \
    --initial-records 2000000000 \
    --hotspot-updates 200000000 \
    --expected-key-count 2000000000 \
    --max-cache-memory 4GB \
    --range-size 50000 \
    --clean
```

#### 3. 性能调优测试
```bash
# 不同缓存配置测试
./scripts/run.sh --strategy dual_rocksdb_adaptive \
    --hot-cache-ratio 0.02 \
    --medium-cache-ratio 0.1 \
    --max-cache-memory 1GB \
    --clean

# 分片测试
./scripts/run.sh --strategy dual_rocksdb_adaptive \
    --enable-sharding \
    --shard-count 4 \
    --expected-key-count 2000000000 \
    --clean
```

#### 4. 使用配置文件
```bash
# 使用JSON配置文件
./scripts/run.sh --strategy dual_rocksdb_adaptive \
    --config-file ./dual_config.json \
    --clean
```

### 更新 README.md

需要在 `scripts/README.md` 中添加：

#### 新增策略说明
```
### DualRocksDBAdaptiveStrategy
- 双RocksDB实例范围分区存储
- 自适应缓存管理（L1热点 + L2范围 + L3被动）
- 内存压力感知和动态调整
- 支持大规模数据集（20亿+KV）
- 配置文件支持复杂参数设置
```

#### 新增参数说明
在 `run.sh` 参数说明中添加新的参数选项。

#### 新增使用示例
```
# Dual RocksDB 大规模测试
./scripts/run.sh --strategy dual_rocksdb_adaptive \
    --initial-records 2000000000 \
    --max-cache-memory 4GB \
    --range-size 50000 \
    --clean

# 使用配置文件
./scripts/run.sh --strategy dual_rocksdb_adaptive \
    --config-file ./dual_config.json \
    --clean
```

### 部署建议

#### 1. 向后兼容
- 保持现有参数不变
- 新参数以可选方式添加
- 默认值确保开箱即用

#### 2. 渐进式部署
- 先实现基础功能
- 逐步添加高级特性
- 提供详细的迁移指南

#### 3. 性能测试
- 与现有策略进行对比测试
- 验证内存使用合理性
- 确保稳定性

## 总结

双RocksDB范围分区存储策略提供了一个全面的解决方案，特别适用于大规模区块链数据存储。通过自适应缓存管理、内存压力感知和分层缓存策略，该设计能够在不同规模的数据集上提供优异的性能。

### 关键优势：
1. **智能内存管理**：基于内存压力的自适应缓存策略
2. **分层缓存**：L1热点 + L2范围 + L3被动缓存
3. **大规模优化**：支持20亿+KV数据集
4. **性能保障**：优先保证系统稳定性，避免内存溢出
5. **灵活配置**：丰富的命令行参数和配置文件支持

### 部署特性：
- **向后兼容**：与现有脚本系统完全兼容
- **渐进增强**：可选的高级参数，基础功能开箱即用
- **配置灵活**：支持命令行参数和JSON配置文件
- **易于测试**：提供专用脚本和详细的使用示例

该策略为区块链应用中的历史状态查询提供了一个高性能、可扩展的存储解决方案，同时保持了与现有系统的良好集成性。
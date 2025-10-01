#pragma once
#include "storage_strategy.hpp"
#include <memory>
#include <vector>
#include <string>

// 前向声明策略工厂
class StorageStrategyFactory;

// 现代化的基准配置 - 使用CLI11
struct BenchmarkConfig {
    std::string storage_strategy = "page_index";  // 存储策略
    std::string db_path = "./rocksdb_data";        // 数据库路径
    size_t initial_records = 100000000;           // 初始记录数
    size_t hotspot_updates = 10000000;            // 热点更新数
    // size_t query_interval = 500000;               // 查询间隔（暂时移除）
    bool enable_bloom_filter = true;               // 启用布隆过滤器
    bool clean_existing_data = false;              // 清理现有数据
    
    // 配置选项
    bool verbose = false;                         // 详细输出
    
    // DualRocksDB特定配置
    size_t dual_rocksdb_range_size = 10000;       // 范围大小
    size_t dual_rocksdb_cache_size = 1024 * 1024 * 1024; // 缓存大小 (1GB)
    double dual_rocksdb_hot_ratio = 0.01;         // 热缓存比例
    double dual_rocksdb_medium_ratio = 0.05;       // 中等缓存比例
    bool dual_rocksdb_compression = true;          // 启用压缩
    bool dual_rocksdb_bloom_filters = true;       // 启用布隆过滤器
    uint32_t dual_rocksdb_batch_size = 5;          // 每个WriteBatch写入的块数
    size_t dual_rocksdb_max_batch_bytes = 128 * 1024 * 1024; // 最大批次大小128MB
    
    // 静态方法
    static BenchmarkConfig from_args(int argc, char* argv[]);
    static BenchmarkConfig from_file(const std::string& config_path);
    static void print_help(const std::string& program_name);
    
    // 实例方法
    void print_config() const;
    void save_to_file(const std::string& config_path) const;
    
    // 验证配置
    bool validate() const;
    std::vector<std::string> get_validation_errors() const;
    
    // 获取策略特定配置
    std::string get_strategy_config() const;
};

// 配置解析错误
class ConfigError : public std::runtime_error {
public:
    ConfigError(const std::string& message) : std::runtime_error(message) {}
};
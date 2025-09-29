#pragma once
#include "storage_strategy.hpp"
#include <memory>
#include <vector>
#include <string>

// 前向声明策略类
class PageIndexStrategy;
class DirectVersionStrategy;
class DualRocksDBStrategy;

// 策略工厂 - 启动时创建指定的策略
class StorageStrategyFactory {
public:
    static std::unique_ptr<IStorageStrategy> create_strategy(
        const std::string& strategy_type);
    
    static std::vector<std::string> get_available_strategies();
    
    static void print_available_strategies();

private:
    static std::unique_ptr<IStorageStrategy> create_page_index_strategy();
    static std::unique_ptr<IStorageStrategy> create_direct_version_strategy();
    static std::unique_ptr<IStorageStrategy> create_dual_rocksdb_strategy();
};

// 基准配置 - 启动时选择策略
struct BenchmarkConfig {
    std::string storage_strategy = "page_index";  // 存储策略
    std::string db_path = "./rocksdb_data";        // 数据库路径
    size_t initial_records = 100000000;           // 初始记录数
    size_t hotspot_updates = 10000000;            // 热点更新数
    size_t query_interval = 500000;               // 查询间隔
    bool enable_bloom_filter = true;               // 启用布隆过滤器
    bool clean_existing_data = false;              // 清理现有数据
    
    static BenchmarkConfig from_args(int argc, char* argv[]);
    static BenchmarkConfig from_file(const std::string& config_path);
    void print_config() const;
    
    // 帮助信息
    static void print_help(const std::string& program_name);
};

// 配置解析错误
class ConfigError : public std::runtime_error {
public:
    ConfigError(const std::string& message) : std::runtime_error(message) {}
};
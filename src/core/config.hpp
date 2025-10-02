#pragma once
#include "storage_strategy.hpp"
#include <memory>
#include <vector>
#include <string>

// 前向声明策略工厂
class StorageStrategyFactory;

// 版本信息打印函数声明
void print_version_info();

// 历史版本查询测试配置 - 简化版本，专注于test.mdx需求
struct BenchmarkConfig {
    std::string storage_strategy = "direct_version";  // 存储策略，默认direct_version
    std::string db_path = "./rocksdb_data";        // 数据库路径
    size_t total_keys = 1000;                      // 总键数（小规模测试）
    size_t continuous_duration_minutes = 360;      // 连续运行时间（分钟，默认6小时）
    
    // 基本选项
    bool enable_bloom_filter = true;               // 启用布隆过滤器
    bool clean_existing_data = false;              // 清理现有数据
    bool verbose = false;                          // 详细输出
    bool version = false;                          // 显示版本信息
    
    // 策略特定配置（简化）
    size_t range_size = 5000;                      // DualRocksDB范围大小
    size_t cache_size = 128 * 1024 * 1024;         // 缓存大小（128MB）
    
    // Batch配置（用于所有策略）
    uint32_t batch_size_blocks = 5;                // 每个WriteBatch写入的块数（默认5个块）
    size_t max_batch_size_bytes = 4UL * 1024 * 1024 * 1024; // 最大批次大小（4GB）
    
    // 静态方法
    static BenchmarkConfig from_args(int argc, char* argv[]);
    static void print_help(const std::string& program_name);
    
    // 实例方法
    void print_config() const;
    
    // 验证配置
    bool validate() const;
    std::vector<std::string> get_validation_errors() const;
};

// 配置解析错误
class ConfigError : public std::runtime_error {
public:
    ConfigError(const std::string& message) : std::runtime_error(message) {}
};
#include "strategy_factory.hpp"
#include "page_index_strategy.hpp"
#include "direct_version_strategy.hpp"
#include "dual_rocksdb_strategy.hpp"
#include "../utils/logger.hpp"
#include <iostream>
#include <algorithm>
#include <cctype>

// 策略创建方法（简化版本，只支持direct_version和dual_rocksdb_adaptive）
std::unique_ptr<IStorageStrategy> StorageStrategyFactory::create_strategy(
    const std::string& strategy_type, const BenchmarkConfig& config) {
    
    std::string normalized_type = strategy_type;
    // 转换为小写以支持大小写不敏感
    std::transform(normalized_type.begin(), normalized_type.end(), 
                  normalized_type.begin(), ::tolower);
    
    if (normalized_type == "direct_version" || normalized_type == "directversion") {
        return create_direct_version_strategy(config);
    } else if (normalized_type == "dual_rocksdb_adaptive" || normalized_type == "dualrocksdbadaptive") {
        return create_dual_rocksdb_strategy(config);
    }
    
    throw std::runtime_error("Unknown storage strategy: " + strategy_type + 
                           ". Supported strategies: direct_version, dual_rocksdb_adaptive");
}

std::unique_ptr<IStorageStrategy> StorageStrategyFactory::create_direct_version_strategy(const BenchmarkConfig& config) {
    DirectVersionStrategy::Config strategy_config;
    
    // 从BenchmarkConfig中读取batch配置
    strategy_config.batch_size_blocks = config.batch_size_blocks;
    strategy_config.max_batch_size_bytes = config.max_batch_size_bytes;
    
    utils::log_info("Creating DirectVersionStrategy with config: batch_size_blocks={}, max_batch_size_bytes={}", 
                    strategy_config.batch_size_blocks, strategy_config.max_batch_size_bytes);
    
    return std::make_unique<DirectVersionStrategy>(strategy_config);
}

std::unique_ptr<IStorageStrategy> StorageStrategyFactory::create_dual_rocksdb_strategy(const BenchmarkConfig& benchmark_config) {
    DualRocksDBStrategy::Config config;
    
    // 从简化的 BenchmarkConfig 中读取 DualRocksDB 配置
    config.range_size = benchmark_config.range_size;
    config.max_cache_memory = benchmark_config.cache_size;
    config.hot_cache_ratio = 0.01;  // 1% hot cache
    config.medium_cache_ratio = 0.05;  // 5% medium cache
    config.enable_compression = false;  // 默认关闭压缩
    config.enable_bloom_filters = true;  // 强制启用布隆过滤器
    config.enable_dynamic_cache_optimization = false;  // 默认关闭动态缓存优化
    
    // 从BenchmarkConfig中读取batch配置
    config.batch_size_blocks = benchmark_config.batch_size_blocks;
    config.max_batch_size_bytes = benchmark_config.max_batch_size_bytes;
    
    utils::log_info("Creating DualRocksDB strategy with config:");
    utils::log_info("  Range Size: {}", config.range_size);
    utils::log_info("  Cache Memory: {} MB", config.max_cache_memory / (1024 * 1024));
    utils::log_info("  Hot Cache Ratio: {:.2f}%", config.hot_cache_ratio * 100);
    utils::log_info("  Medium Cache Ratio: {:.2f}%", config.medium_cache_ratio * 100);
    utils::log_info("  Compression: {}", config.enable_compression ? "enabled" : "disabled");
    utils::log_info("  Bloom Filters: enabled");
    
    return std::make_unique<DualRocksDBStrategy>(config);
}

std::vector<std::string> StorageStrategyFactory::get_available_strategies() {
    return {"direct_version", "dual_rocksdb_adaptive"};
}

void StorageStrategyFactory::print_available_strategies() {
    utils::log_info("Available storage strategies:");
    auto strategies = get_available_strategies();
    for (const auto& strategy : strategies) {
        utils::log_info("  - {}", strategy);
    }
}
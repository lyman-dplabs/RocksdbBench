#include "strategy_factory.hpp"
#include "page_index_strategy.hpp"
#include "direct_version_strategy.hpp"
#include "dual_rocksdb_strategy.hpp"
#include "../utils/logger.hpp"
#include <iostream>
#include <algorithm>
#include <cctype>

// 策略创建方法（统一使用配置）
std::unique_ptr<IStorageStrategy> StorageStrategyFactory::create_strategy(
    const std::string& strategy_type, const BenchmarkConfig& config) {
    
    std::string normalized_type = strategy_type;
    // 转换为小写以支持大小写不敏感
    std::transform(normalized_type.begin(), normalized_type.end(), 
                  normalized_type.begin(), ::tolower);
    
    if (normalized_type == "page_index" || normalized_type == "pageindex") {
        return create_page_index_strategy(config);
    } else if (normalized_type == "direct_version" || normalized_type == "directversion") {
        return create_direct_version_strategy(config);
    } else if (normalized_type == "dual_rocksdb_adaptive" || normalized_type == "dualrocksdbadaptive") {
        return create_dual_rocksdb_strategy(config);
    } else if (normalized_type == "simple_keyblock" || normalized_type == "simplekeyblock") {
        // TODO: 实现SimpleKeyBlockStrategy
        throw std::runtime_error("Strategy 'simple_keyblock' not yet implemented");
    } else if (normalized_type == "reduced_keyblock" || normalized_type == "reducedkeyblock") {
        // TODO: 实现ReducedKeyBlockStrategy
        throw std::runtime_error("Strategy 'reduced_keyblock' not yet implemented");
    }
    
    throw std::runtime_error("Unknown storage strategy: " + strategy_type);
}

std::unique_ptr<IStorageStrategy> StorageStrategyFactory::create_page_index_strategy(const BenchmarkConfig& config) {
    // 创建PageIndexStrategy，设置merge callback
    auto strategy = std::make_unique<PageIndexStrategy>(nullptr);
    return strategy;
}

std::unique_ptr<IStorageStrategy> StorageStrategyFactory::create_direct_version_strategy(const BenchmarkConfig& config) {
    DirectVersionStrategy::Config strategy_config;
    
    // 从 BenchmarkConfig 中读取 DirectVersion 特定配置
    strategy_config.batch_size_blocks = config.direct_version_batch_size;
    strategy_config.max_batch_size_bytes = config.direct_version_max_batch_bytes;
    
    utils::log_info("Creating DirectVersionStrategy with config: batch_size_blocks={}, max_batch_size_bytes={}", 
                    strategy_config.batch_size_blocks, strategy_config.max_batch_size_bytes);
    
    return std::make_unique<DirectVersionStrategy>(strategy_config);
}

std::unique_ptr<IStorageStrategy> StorageStrategyFactory::create_dual_rocksdb_strategy(const BenchmarkConfig& benchmark_config) {
    DualRocksDBStrategy::Config config;
    
    // 从 BenchmarkConfig 中读取 DualRocksDB 特定配置
    config.range_size = benchmark_config.dual_rocksdb_range_size;
    config.max_cache_memory = benchmark_config.dual_rocksdb_cache_size;
    config.hot_cache_ratio = benchmark_config.dual_rocksdb_hot_ratio;
    config.medium_cache_ratio = benchmark_config.dual_rocksdb_medium_ratio;
    config.enable_compression = benchmark_config.enable_compression;  // 使用全局压缩设置
    config.enable_bloom_filters = true;  // 强制启用布隆过滤器以获得最佳性能
    config.enable_dynamic_cache_optimization = benchmark_config.dual_rocksdb_dynamic_cache;  // 使用命令行配置
    config.batch_size_blocks = benchmark_config.dual_rocksdb_batch_size;
    config.max_batch_size_bytes = benchmark_config.dual_rocksdb_max_batch_bytes;
    
    utils::log_info("Creating DualRocksDB strategy with custom config:");
    utils::log_info("  Range Size: {}", config.range_size);
    utils::log_info("  Cache Memory: {} MB", config.max_cache_memory / (1024 * 1024));
    utils::log_info("  Hot Cache Ratio: {:.2f}%", config.hot_cache_ratio * 100);
    utils::log_info("  Medium Cache Ratio: {:.2f}%", config.medium_cache_ratio * 100);
    utils::log_info("  Compression: {}", config.enable_compression ? "enabled" : "disabled");
    utils::log_info("  Bloom Filters: always enabled (optimized)");
    utils::log_info("  Batch Size Blocks: {}", config.batch_size_blocks);
    utils::log_info("  Max Batch Size Bytes: {} MB", config.max_batch_size_bytes / (1024 * 1024));
    
    return std::make_unique<DualRocksDBStrategy>(config);
}

std::vector<std::string> StorageStrategyFactory::get_available_strategies() {
    return {"page_index", "direct_version", "dual_rocksdb_adaptive", "simple_keyblock", "reduced_keyblock"};
}

void StorageStrategyFactory::print_available_strategies() {
    utils::log_info("Available storage strategies:");
    auto strategies = get_available_strategies();
    for (const auto& strategy : strategies) {
        utils::log_info("  - {}", strategy);
    }
}
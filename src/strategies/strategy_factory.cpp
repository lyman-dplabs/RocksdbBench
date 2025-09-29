#include "strategy_factory.hpp"
#include "page_index_strategy.hpp"
#include "direct_version_strategy.hpp"
#include "dual_rocksdb_strategy.hpp"
#include "../utils/logger.hpp"
#include <iostream>
#include <algorithm>
#include <cctype>

// 策略工厂实现
std::unique_ptr<IStorageStrategy> StorageStrategyFactory::create_strategy(
    const std::string& strategy_type) {
    
    std::string normalized_type = strategy_type;
    // 转换为小写以支持大小写不敏感
    std::transform(normalized_type.begin(), normalized_type.end(), 
                  normalized_type.begin(), ::tolower);
    
    if (normalized_type == "page_index" || normalized_type == "pageindex") {
        return create_page_index_strategy();
    } else if (normalized_type == "direct_version" || normalized_type == "directversion") {
        return create_direct_version_strategy();
    } else if (normalized_type == "dual_rocksdb_adaptive" || normalized_type == "dualrocksdbadaptive") {
        return create_dual_rocksdb_strategy();
    } else if (normalized_type == "simple_keyblock" || normalized_type == "simplekeyblock") {
        // TODO: 实现SimpleKeyBlockStrategy
        throw std::runtime_error("Strategy 'simple_keyblock' not yet implemented");
    } else if (normalized_type == "reduced_keyblock" || normalized_type == "reducedkeyblock") {
        // TODO: 实现ReducedKeyBlockStrategy
        throw std::runtime_error("Strategy 'reduced_keyblock' not yet implemented");
    }
    
    throw std::runtime_error("Unknown storage strategy: " + strategy_type);
}

std::unique_ptr<IStorageStrategy> StorageStrategyFactory::create_page_index_strategy() {
    // 创建PageIndexStrategy，设置merge callback
    auto strategy = std::make_unique<PageIndexStrategy>(nullptr);
    return strategy;
}

std::unique_ptr<IStorageStrategy> StorageStrategyFactory::create_direct_version_strategy() {
    return std::make_unique<DirectVersionStrategy>();
}

std::unique_ptr<IStorageStrategy> StorageStrategyFactory::create_dual_rocksdb_strategy() {
    DualRocksDBStrategy::Config config;
    // 使用默认配置，可以根据需要从命令行参数或配置文件读取
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
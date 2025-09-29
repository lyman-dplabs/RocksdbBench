#pragma once
#include "../core/storage_strategy.hpp"
#include <memory>
#include <string>
#include <vector>

// 策略工厂类
class StorageStrategyFactory {
public:
    // 创建策略实例
    static std::unique_ptr<IStorageStrategy> create_strategy(const std::string& strategy_type);
    
    // 具体策略创建方法
    static std::unique_ptr<IStorageStrategy> create_page_index_strategy();
    static std::unique_ptr<IStorageStrategy> create_direct_version_strategy();
    static std::unique_ptr<IStorageStrategy> create_dual_rocksdb_strategy();
    
    // 获取可用策略列表
    static std::vector<std::string> get_available_strategies();
    
    // 打印可用策略
    static void print_available_strategies();
};
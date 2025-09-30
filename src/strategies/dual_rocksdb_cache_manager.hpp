#pragma once
#include <unordered_map>
#include <vector>
#include <string>
#include <optional>
#include <chrono>
#include <mutex>
#include <atomic>

// 缓存级别枚举
enum class CacheLevel {
    HOT,        // 热点数据：完整数据缓存
    MEDIUM,     // 中等数据：范围列表缓存
    PASSIVE     // 被动缓存：仅查询时缓存
};

// 访问统计信息
struct AccessStats {
    size_t access_count = 0;
    std::chrono::system_clock::time_point last_access;
    std::chrono::system_clock::time_point first_access;
};

// 缓存条目
struct CacheEntry {
    std::string value;
    std::chrono::system_clock::time_point last_access;
    std::chrono::system_clock::time_point created;
};

// 自适应缓存管理器 - 专门为DualRocksDBStrategy设计
class AdaptiveCacheManager {
private:
    // L1缓存：热点数据完整缓存
    std::unordered_map<std::string, CacheEntry> hot_cache_;
    
    // L2缓存：中等数据范围列表缓存
    std::unordered_map<std::string, std::vector<uint32_t>> range_cache_;
    
    // L3缓存：被动查询缓存
    std::unordered_map<std::string, std::string> passive_cache_;
    
    // 访问统计
    std::unordered_map<std::string, AccessStats> access_stats_;
    
    // 内存监控
    size_t current_memory_usage_ = 0;
    size_t max_memory_limit_;
    mutable std::mutex cache_mutex_;
    
    // 配置参数
    double hot_cache_ratio_ = 0.01;
    double medium_cache_ratio_ = 0.05;
    bool enable_memory_monitor_ = true;
    
  public:
    explicit AdaptiveCacheManager(size_t max_memory_bytes = 1024 * 1024 * 1024);
    
    // 缓存操作
    void cache_hot_data(const std::string& key, const std::string& value);
    void cache_range_list(const std::string& key, const std::vector<uint32_t>& ranges);
    void cache_passive_data(const std::string& key, const std::string& value);
    
    // 缓存查询
    std::optional<std::string> get_hot_data(const std::string& key);
    std::optional<std::vector<uint32_t>> get_range_list(const std::string& key);
    std::optional<std::string> get_passive_data(const std::string& key);
    
    // 访问模式更新
    void update_access_pattern(const std::string& key, bool is_write);
    CacheLevel determine_cache_level(const std::string& key) const;
    
    // 内存管理
    size_t get_memory_usage() const;
    void manage_memory_pressure();
    void evict_least_used();
    
    // 配置更新
    void set_config(double hot_ratio, double medium_ratio);
    
    // 清理操作
    void clear_expired();
    void clear_all();
    
private:
    bool has_memory_for_hot_data() const;
    bool has_memory_for_medium_data() const;
    void update_memory_usage(size_t added_bytes);
    size_t estimate_entry_size(const std::string& value) const;
};
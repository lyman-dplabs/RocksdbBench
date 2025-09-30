#include "dual_rocksdb_cache_manager.hpp"
#include "../utils/logger.hpp"
#include <algorithm>

AdaptiveCacheManager::AdaptiveCacheManager(size_t max_memory_bytes)
    : max_memory_limit_(max_memory_bytes) {
}

void AdaptiveCacheManager::cache_hot_data(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    size_t entry_size = estimate_entry_size(value);
    update_memory_usage(entry_size);
    
    hot_cache_[key] = {value, std::chrono::system_clock::now(), std::chrono::system_clock::now()};
    update_access_pattern(key, true);
}

void AdaptiveCacheManager::cache_range_list(const std::string& key, const std::vector<uint32_t>& ranges) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    size_t entry_size = ranges.size() * sizeof(uint32_t) + key.size();
    update_memory_usage(entry_size);
    
    range_cache_[key] = ranges;
    update_access_pattern(key, true);
}

void AdaptiveCacheManager::cache_passive_data(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    size_t entry_size = estimate_entry_size(value);
    update_memory_usage(entry_size);
    
    passive_cache_[key] = value;
    update_access_pattern(key, true);
}

std::optional<std::string> AdaptiveCacheManager::get_hot_data(const std::string& key) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    auto it = hot_cache_.find(key);
    if (it != hot_cache_.end()) {
        it->second.last_access = std::chrono::system_clock::now();
        update_access_pattern(key, false);
        return it->second.value;
    }
    return std::nullopt;
}

std::optional<std::vector<uint32_t>> AdaptiveCacheManager::get_range_list(const std::string& key) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    auto it = range_cache_.find(key);
    if (it != range_cache_.end()) {
        update_access_pattern(key, false);
        return it->second;
    }
    return std::nullopt;
}

std::optional<std::string> AdaptiveCacheManager::get_passive_data(const std::string& key) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    auto it = passive_cache_.find(key);
    if (it != passive_cache_.end()) {
        update_access_pattern(key, false);
        return it->second;
    }
    return std::nullopt;
}

void AdaptiveCacheManager::update_access_pattern(const std::string& key, bool is_write) {
    auto now = std::chrono::system_clock::now();
    auto& stats = access_stats_[key];
    
    stats.access_count++;
    stats.last_access = now;
    if (stats.first_access == std::chrono::system_clock::time_point{}) {
        stats.first_access = now;
    }
}

CacheLevel AdaptiveCacheManager::determine_cache_level(const std::string& key) const {
    auto stats_it = access_stats_.find(key);
    if (stats_it == access_stats_.end()) {
        return CacheLevel::PASSIVE;
    }
    
    const auto& stats = stats_it->second;
    auto now = std::chrono::system_clock::now();
    auto age = std::chrono::duration_cast<std::chrono::seconds>(now - stats.last_access);
    
    // 基于访问频率和内存压力动态判断
    double access_frequency = stats.access_count / std::max(1.0, static_cast<double>(age.count()));
    
    if (access_frequency > 100.0 && has_memory_for_hot_data()) {
        return CacheLevel::HOT;
    } else if (access_frequency > 10.0 && has_memory_for_medium_data()) {
        return CacheLevel::MEDIUM;
    } else {
        return CacheLevel::PASSIVE;
    }
}

size_t AdaptiveCacheManager::get_memory_usage() const {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    return current_memory_usage_;
}

void AdaptiveCacheManager::manage_memory_pressure() {
    if (!enable_memory_monitor_) return;
    
    if (current_memory_usage_ > max_memory_limit_ * 0.9) {
        evict_least_used();
    }
}

void AdaptiveCacheManager::evict_least_used() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    // 清理被动缓存
    if (current_memory_usage_ > max_memory_limit_ * 0.8) {
        // 简化清理逻辑，直接清理50%的缓存
        size_t to_remove = passive_cache_.size() / 2;
        auto it = passive_cache_.begin();
        for (size_t i = 0; i < to_remove && it != passive_cache_.end(); ++i) {
            it = passive_cache_.erase(it);
        }
    }
    
    // 清理中等缓存
    if (current_memory_usage_ > max_memory_limit_ * 0.6) {
        // 简化清理逻辑，直接清理30%的缓存
        size_t to_remove = range_cache_.size() * 0.3;
        auto it = range_cache_.begin();
        for (size_t i = 0; i < to_remove && it != range_cache_.end(); ++i) {
            it = range_cache_.erase(it);
        }
    }
    
    // 重新计算内存使用
    current_memory_usage_ = 0;
    for (const auto& [key, entry] : hot_cache_) {
        current_memory_usage_ += estimate_entry_size(entry.value);
    }
    for (const auto& [key, ranges] : range_cache_) {
        current_memory_usage_ += ranges.size() * sizeof(uint32_t) + key.size();
    }
    for (const auto& [key, value] : passive_cache_) {
        current_memory_usage_ += estimate_entry_size(value);
    }
}

void AdaptiveCacheManager::set_config(double hot_ratio, double medium_ratio) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    hot_cache_ratio_ = hot_ratio;
    medium_cache_ratio_ = medium_ratio;
}

void AdaptiveCacheManager::clear_expired() {
    // 实现TTL清理逻辑
    auto now = std::chrono::system_clock::now();
    
    // 清理1小时未访问的热点缓存
    for (auto it = hot_cache_.begin(); it != hot_cache_.end();) {
        auto age = std::chrono::duration_cast<std::chrono::seconds>(now - it->second.last_access);
        if (age.count() > 3600) {
            it = hot_cache_.erase(it);
        } else {
            ++it;
        }
    }
    
    // 清理30分钟未访问的中等缓存
    for (auto it = range_cache_.begin(); it != range_cache_.end();) {
        auto stats_it = access_stats_.find(it->first);
        if (stats_it != access_stats_.end()) {
            auto age = std::chrono::duration_cast<std::chrono::seconds>(now - stats_it->second.last_access);
            if (age.count() > 1800) {
                it = range_cache_.erase(it);
            } else {
                ++it;
            }
        } else {
            ++it;
        }
    }
}

void AdaptiveCacheManager::clear_all() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    hot_cache_.clear();
    range_cache_.clear();
    passive_cache_.clear();
    access_stats_.clear();
    current_memory_usage_ = 0;
}

bool AdaptiveCacheManager::has_memory_for_hot_data() const {
    return current_memory_usage_ < max_memory_limit_ * hot_cache_ratio_;
}

bool AdaptiveCacheManager::has_memory_for_medium_data() const {
    return current_memory_usage_ < max_memory_limit_ * medium_cache_ratio_;
}

void AdaptiveCacheManager::update_memory_usage(size_t added_bytes) {
    current_memory_usage_ += added_bytes;
    manage_memory_pressure();
}

size_t AdaptiveCacheManager::estimate_entry_size(const std::string& value) const {
    return value.size() + sizeof(CacheEntry) + 64; // 额外开销估算
}
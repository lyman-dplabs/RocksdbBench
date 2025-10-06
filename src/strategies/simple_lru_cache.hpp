#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <list>
#include <shared_mutex>
#include <mutex>
#include <memory>
#include <future>
#include <optional>
#include <chrono>

// 针对addr_slot格式优化的hash函数
inline size_t optimized_addr_hash(const std::string& addr_slot) {
    if (addr_slot.length() < 46) return std::hash<std::string>{}(addr_slot);

    // 提取slot数字部分用于更好的分布
    size_t slot_hash = 0;
    size_t hash_pos = addr_slot.find("#slot");
    if (hash_pos != std::string::npos && hash_pos + 5 < addr_slot.length()) {
        for (size_t i = hash_pos + 5; i < addr_slot.length(); ++i) {
            slot_hash = slot_hash * 10 + (addr_slot[i] - '0');
        }
    }

    // 组合地址部分和slot部分的hash
    size_t addr_hash = std::hash<std::string_view>{}(std::string_view(addr_slot.data(), hash_pos));
    return addr_hash ^ (slot_hash << 16);
}

// SingleFlight调用状态
struct CallState {
    std::vector<uint32_t> result;
    std::promise<std::vector<uint32_t>> promise;
    std::shared_future<std::vector<uint32_t>> future;
    std::chrono::system_clock::time_point start_time;

    CallState() : future(promise.get_future()), start_time(std::chrono::system_clock::now()) {}
};

// 简单的LRU缓存条目
struct LRUCacheEntry {
    std::vector<uint32_t> ranges;
    std::list<std::string>::iterator lru_it;

    LRUCacheEntry(std::vector<uint32_t> r) : ranges(std::move(r)) {}
};

// 简单的LRU缓存段 - 集成SingleFlight
class SimpleLRUSegment {
public:
    explicit SimpleLRUSegment(size_t max_size = 1000) : max_size_(max_size) {}
    ~SimpleLRUSegment() = default;

    // 非拷贝
    SimpleLRUSegment(const SimpleLRUSegment&) = delete;
    SimpleLRUSegment& operator=(const SimpleLRUSegment&) = delete;

    // 获取缓存条目 - 集成SingleFlight逻辑
    std::vector<uint32_t> get_or_load(
        const std::string& key,
        std::function<std::vector<uint32_t>()> loader
    );

    // 预热缓存
    void put(const std::string& key, std::vector<uint32_t> ranges);

    // 获取缓存大小
    size_t size() const;

    // 获取内存使用估算
    size_t memory_usage() const;

    // 获取活跃flight数量
    size_t active_flight_count() const;

    // 获取段统计
    struct SegmentStats {
        size_t cache_hits = 0;
        size_t cache_misses = 0;
        double hit_rate() const {
            size_t total = cache_hits + cache_misses;
            return total > 0 ? static_cast<double>(cache_hits) / total : 0.0;
        }
    };

    SegmentStats get_stats() const;

    // 清空缓存
    void clear();

private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, std::unique_ptr<LRUCacheEntry>> cache_;
    std::list<std::string> lru_list_;  // 最近使用的在前面
    size_t max_size_;

    // SingleFlight调用管理 - 每段独立
    std::unordered_map<std::string, std::shared_ptr<CallState>> active_flights_;

    // 统计信息 - 每段独立统计
    mutable size_t cache_hits_ = 0;
    mutable size_t cache_misses_ = 0;

    void update_lru(const std::string& key, LRUCacheEntry* entry);
    void cleanup_stale_flights(); // 清理超时的flight调用

    static constexpr auto FLIGHT_TIMEOUT = std::chrono::seconds(30);

    friend class SimpleSingleFlightCache; // 允许访问私有成员
};

// 简化的分段缓存管理器
class SimpleSingleFlightCache {
public:
    explicit SimpleSingleFlightCache(size_t segment_count = 16, size_t segment_size = 1000);
    ~SimpleSingleFlightCache() = default;

    // 非拷贝
    SimpleSingleFlightCache(const SimpleSingleFlightCache&) = delete;
    SimpleSingleFlightCache& operator=(const SimpleSingleFlightCache&) = delete;

    // 获取range列表 - 主要接口
    std::vector<uint32_t> get_ranges(
        const std::string& addr_slot,
        std::function<std::vector<uint32_t>()> loader
    );

    // 预热缓存
    void preload_ranges(
        const std::string& addr_slot,
        std::vector<uint32_t> ranges
    );

    // 获取缓存统计
    struct CacheStats {
        size_t total_entries;
        size_t total_memory_bytes;
        size_t active_flight_calls;
        double hit_rate;
        size_t total_accesses;
        size_t hits;
    };

    CacheStats get_stats() const;

    // 清空所有缓存
    void clear_all();

private:
    size_t get_segment_index(const std::string& key) const;

    std::vector<std::unique_ptr<SimpleLRUSegment>> segments_;
};
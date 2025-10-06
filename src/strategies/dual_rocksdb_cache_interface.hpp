#pragma once
#include "simple_lru_cache.hpp"
#include <rocksdb/db.h>
#include <functional>

// Range查询接口 - 封装与DualRocksDBStrategy的交互
class DualRocksDBCacheInterface {
public:
    explicit DualRocksDBCacheInterface(size_t segment_count = 16);
    ~DualRocksDBCacheInterface() = default;

    // 非拷贝
    DualRocksDBCacheInterface(const DualRocksDBCacheInterface&) = delete;
    DualRocksDBCacheInterface& operator=(const DualRocksDBCacheInterface&) = delete;

    // 设置数据库查询函数
    void set_query_function(std::function<std::vector<uint32_t>(const std::string&)> query_func);

    // 主要接口：获取某个地址的所有range列表
    std::vector<uint32_t> get_address_ranges(const std::string& addr_slot);

    // 预热缓存 - 批量添加已知的热点数据
    void preload_address_ranges(const std::unordered_map<std::string, std::vector<uint32_t>>& hot_data);

    // 查询统计信息
    struct QueryStats {
        size_t total_queries;
        size_t cache_hits;
        double hit_rate;
        size_t cache_entries;
        size_t cache_memory_bytes;
        size_t active_flight_calls;
    };

    QueryStats get_query_stats() const;

    // 缓存管理
    void clear_cache();

    // 性能调优接口
    void set_cleanup_interval(size_t interval);
    void set_expire_duration(std::chrono::minutes duration);

private:
    std::unique_ptr<SimpleSingleFlightCache> cache_;
    std::function<std::vector<uint32_t>(const std::string&)> query_function_;
};
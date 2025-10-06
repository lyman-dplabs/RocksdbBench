#include "dual_rocksdb_cache_interface.hpp"

DualRocksDBCacheInterface::DualRocksDBCacheInterface(size_t segment_count)
    : cache_(std::make_unique<SimpleSingleFlightCache>(segment_count, 1000)) {
}

void DualRocksDBCacheInterface::set_query_function(
    std::function<std::vector<uint32_t>(const std::string&)> query_func
) {
    query_function_ = std::move(query_func);
}

std::vector<uint32_t> DualRocksDBCacheInterface::get_address_ranges(const std::string& addr_slot) {
    // 使用缓存获取ranges，如果缓存未命中则调用query_function_
    return cache_->get_ranges(addr_slot, [this, &addr_slot]() -> std::vector<uint32_t> {
        return query_function_(addr_slot);
    });
}

void DualRocksDBCacheInterface::preload_address_ranges(
    const std::unordered_map<std::string, std::vector<uint32_t>>& hot_data
) {
    for (const auto& [addr_slot, ranges] : hot_data) {
        cache_->preload_ranges(addr_slot, ranges);
    }
}

DualRocksDBCacheInterface::QueryStats DualRocksDBCacheInterface::get_query_stats() const {
    auto cache_stats = cache_->get_stats();

    return QueryStats{
        cache_stats.total_accesses,
        cache_stats.hits,
        cache_stats.hit_rate,
        cache_stats.total_entries,
        cache_stats.total_memory_bytes,
        cache_stats.active_flight_calls
    };
}

void DualRocksDBCacheInterface::clear_cache() {
    cache_->clear_all();
}
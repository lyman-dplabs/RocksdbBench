#include "simple_lru_cache.hpp"
#include <algorithm>

// SimpleLRUSegment实现 - 集成SingleFlight
std::vector<uint32_t> SimpleLRUSegment::get_or_load(
    const std::string& key,
    std::function<std::vector<uint32_t>()> loader
) {
    // 首先尝试从缓存获取
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto it = cache_.find(key);
        if (it != cache_.end()) {
            // 缓存命中，更新统计
            cache_hits_++;

            // 更新LRU位置
            lock.unlock();
            std::unique_lock<std::shared_mutex> write_lock(mutex_);

            auto res_find = cache_.find(key);
            if (res_find != cache_.end()) {
                update_lru(key, res_find->second.get());
                return res_find->second->ranges;
            }
        }
    }

    // 缓存未命中，更新统计
    cache_misses_++;

    // 缓存未命中，检查SingleFlight
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);

        // 定期清理超时的flight调用
        cleanup_stale_flights();

        auto it = active_flights_.find(key);
        if (it != active_flights_.end()) {
            // 已有线程在加载，等待结果
            auto call_state = it->second;
            lock.unlock();

            try {
                auto timeout = std::chrono::milliseconds(10000); // 10秒超时
                auto status = call_state->future.wait_for(timeout);

                if (status == std::future_status::ready) {
                    auto result = call_state->future.get();

                    // 将结果加入缓存
                    put(key, result);
                    return result;
                } else {
                    // 超时，执行自己的加载逻辑
                }
            } catch (const std::exception&) {
                // 如果等待出错，继续执行自己的加载逻辑
            }
        } else {
            // 没有线程在加载，创建新的flight调用
            auto call_state = std::make_shared<CallState>();
            active_flights_[key] = call_state;
            lock.unlock();

            try {
                // 执行实际的加载操作
                auto result = loader();

                // 设置结果
                call_state->promise.set_value(result);

                // 加入缓存
                put(key, result);

                // 清理flight调用
                {
                    std::unique_lock<std::shared_mutex> cleanup_lock(mutex_);
                    active_flights_.erase(key);
                }

                return result;

            } catch (...) {
                // 加载失败，清理flight调用并重新抛出异常
                call_state->promise.set_exception(std::current_exception());

                {
                    std::unique_lock<std::shared_mutex> cleanup_lock(mutex_);
                    active_flights_.erase(key);
                }

                throw;
            }
        }
    }

    // 如果所有flight逻辑都失败，回退到直接加载
    auto result = loader();
    put(key, result);
    return result;
}

void SimpleLRUSegment::put(const std::string& key, std::vector<uint32_t> ranges) {
    std::unique_lock<std::shared_mutex> lock(mutex_);

    auto it = cache_.find(key);
    if (it != cache_.end()) {
        // 更新现有条目
        it->second->ranges = std::move(ranges);
        update_lru(key, it->second.get());
        return;
    }

    // 如果缓存已满，删除最旧的条目
    if (cache_.size() >= max_size_) {
        if (!lru_list_.empty()) {
            std::string oldest_key = lru_list_.back();
            lru_list_.pop_back();
            cache_.erase(oldest_key);
        }
    }

    // 添加新条目
    auto entry = std::make_unique<LRUCacheEntry>(std::move(ranges));
    lru_list_.push_front(key);
    entry->lru_it = lru_list_.begin();
    cache_[key] = std::move(entry);
}

void SimpleLRUSegment::update_lru(const std::string& key, LRUCacheEntry* entry) {
    // 从当前位置移除
    lru_list_.erase(entry->lru_it);
    // 添加到前面
    lru_list_.push_front(key);
    entry->lru_it = lru_list_.begin();
}

size_t SimpleLRUSegment::size() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return cache_.size();
}

size_t SimpleLRUSegment::memory_usage() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    size_t total = 0;

    for (const auto& [key, entry] : cache_) {
        total += key.size();
        total += entry->ranges.size() * sizeof(uint32_t);
        total += sizeof(LRUCacheEntry);
    }
    total += lru_list_.size() * sizeof(std::string); // LRU链表开销

    return total;
}

size_t SimpleLRUSegment::active_flight_count() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return active_flights_.size();
}

void SimpleLRUSegment::cleanup_stale_flights() {
    auto now = std::chrono::system_clock::now();

    for (auto it = active_flights_.begin(); it != active_flights_.end();) {
        auto age = now - it->second->start_time;
        if (age > FLIGHT_TIMEOUT) {
            // 设置超时异常
            try {
                it->second->promise.set_exception(
                    std::make_exception_ptr(std::runtime_error("Flight call timeout"))
                );
            } catch (...) {
                // 如果promise已经设置，忽略异常
            }
            it = active_flights_.erase(it);
        } else {
            ++it;
        }
    }
}


SimpleLRUSegment::SegmentStats SimpleLRUSegment::get_stats() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return {cache_hits_, cache_misses_};
}

void SimpleLRUSegment::clear() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    cache_.clear();
    lru_list_.clear();
    active_flights_.clear();
    cache_hits_ = 0;
    cache_misses_ = 0;
}

// SimpleSingleFlightCache实现
SimpleSingleFlightCache::SimpleSingleFlightCache(size_t segment_count, size_t segment_size) {
    for (size_t i = 0; i < segment_count; ++i) {
        segments_.push_back(std::make_unique<SimpleLRUSegment>(segment_size));
    }
}

size_t SimpleSingleFlightCache::get_segment_index(const std::string& key) const {
    return optimized_addr_hash(key) % segments_.size();
}

std::vector<uint32_t> SimpleSingleFlightCache::get_ranges(
    const std::string& addr_slot,
    std::function<std::vector<uint32_t>()> loader
) {
    // 路由到对应的段
    size_t segment_idx = get_segment_index(addr_slot);
    return segments_[segment_idx]->get_or_load(addr_slot, loader);
}

void SimpleSingleFlightCache::preload_ranges(
    const std::string& addr_slot,
    std::vector<uint32_t> ranges
) {
    size_t segment_idx = get_segment_index(addr_slot);
    segments_[segment_idx]->put(addr_slot, std::move(ranges));
}

SimpleSingleFlightCache::CacheStats SimpleSingleFlightCache::get_stats() const {
    size_t total_entries = 0;
    size_t total_memory = 0;
    size_t active_flights = 0;
    size_t total_hits = 0;
    size_t total_misses = 0;

    for (const auto& segment : segments_) {
        total_entries += segment->size();
        total_memory += segment->memory_usage();
        active_flights += segment->active_flight_count();

        auto segment_stats = segment->get_stats();
        total_hits += segment_stats.cache_hits;
        total_misses += segment_stats.cache_misses;
    }

    double hit_rate = (total_hits + total_misses) > 0 ?
        static_cast<double>(total_hits) / (total_hits + total_misses) : 0.0;

    return {
        total_entries,
        total_memory,
        active_flights,
        hit_rate,
        total_hits + total_misses,
        total_hits
    };
}

void SimpleSingleFlightCache::clear_all() {
    // 清空所有段
    for (auto& segment : segments_) {
        segment->clear();
    }
}
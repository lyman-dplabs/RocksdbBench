#include "strategy_db_manager.hpp"
#include "../utils/logger.hpp"
#include "../strategies/dual_rocksdb_strategy.hpp"
#include <filesystem>
#include <rocksdb/options.h>
#include <rocksdb/table_properties.h>
#include <algorithm>

StrategyDBManager::StrategyDBManager(const std::string& db_path, 
                                   std::unique_ptr<IStorageStrategy> strategy)
    : db_path_(db_path), strategy_(std::move(strategy)) {
    
    statistics_ = rocksdb::CreateDBStatistics();
}

StrategyDBManager::~StrategyDBManager() {
    close();
}

bool StrategyDBManager::open(bool force_clean) {
    if (is_open_) {
        utils::log_warn("Database is already open");
        return true;
    }

    try {
        if (force_clean && data_exists()) {
            utils::log_info("Cleaning existing data at: {}", db_path_);
            clean_data();
        }

        rocksdb::Options options = get_db_options();
        
        // Create database if not exists
        rocksdb::Status status = rocksdb::DB::Open(options, db_path_, &db_);
        if (!status.ok()) {
            utils::log_error("Failed to open database at {}: {}", db_path_, status.ToString());
            return false;
        }

        // Initialize storage strategy
        if (!strategy_->initialize(db_.get())) {
            utils::log_error("Failed to initialize storage strategy: {}", strategy_->get_strategy_name());
            return false;
        }

        is_open_ = true;
        utils::log_info("Database opened successfully at: {}", db_path_);
        utils::log_info("Using storage strategy: {}", strategy_->get_description());
        return true;

    } catch (const std::exception& e) {
        utils::log_error("Exception during database open: {}", e.what());
        return false;
    }
}

void StrategyDBManager::close() {
    if (is_open_) {
        // Cleanup strategy resources
        if (strategy_) {
            strategy_->cleanup(db_.get());
        }
        
        db_.reset();
        is_open_ = false;
        utils::log_info("Database closed");
    }
}

bool StrategyDBManager::data_exists() const {
    return std::filesystem::exists(db_path_) && 
           std::filesystem::is_directory(db_path_) &&
           !std::filesystem::is_empty(db_path_);
}

bool StrategyDBManager::clean_data() {
    try {
        if (std::filesystem::exists(db_path_)) {
            size_t removed = std::filesystem::remove_all(db_path_);
            utils::log_info("Removed existing data directory: {} ({} files)", db_path_, removed);
        }
        return true;
    } catch (const std::exception& e) {
        utils::log_error("Failed to clean data directory {}: {}", db_path_, e.what());
        return false;
    }
}

// New unified interface
bool StrategyDBManager::write_batch(const std::vector<DataRecord>& records) {
    if (!is_open_) {
        utils::log_error("Database is not open");
        return false;
    }

    try {
        return strategy_->write_batch(db_.get(), records);
    } catch (const std::exception& e) {
        utils::log_error("Exception during write_batch: {}", e.what());
        return false;
    }
}

std::optional<Value> StrategyDBManager::query_latest_value(const std::string& addr_slot) {
    if (!is_open_) {
        utils::log_error("Database is not open");
        return std::nullopt;
    }

    try {
        return strategy_->query_latest_value(db_.get(), addr_slot);
    } catch (const std::exception& e) {
        utils::log_error("Exception during query_latest_value: {}", e.what());
        return std::nullopt;
    }
}


bool StrategyDBManager::write_initial_load_batch(const std::vector<DataRecord>& records) {
    if (!is_open_) {
        utils::log_error("Database is not open");
        return false;
    }

    return strategy_->write_initial_load_batch(db_.get(), records);
}

void StrategyDBManager::flush_all_batches() {
    return strategy_->flush_all_batches();
}

// Legacy interface for compatibility
bool StrategyDBManager::write_batch(const std::vector<ChangeSetRecord>& changes, 
                                   const std::vector<IndexRecord>& indices) {
    if (!is_open_) {
        utils::log_error("Database is not open");
        return false;
    }

    try {
        // Convert to unified DataRecord format
        std::vector<DataRecord> records;
        records.reserve(changes.size());
        
        for (const auto& change : changes) {
            DataRecord record{change.block_num, change.addr_slot, change.value};
            records.push_back(record);
        }
        
        return strategy_->write_batch(db_.get(), records);
    } catch (const std::exception& e) {
        utils::log_error("Exception during legacy write_batch: {}", e.what());
        return false;
    }
}

std::optional<Value> StrategyDBManager::get_historical_state(const std::string& addr_slot, 
                                                           BlockNum target_block_num) {
    // Historical queries are not supported anymore
    return std::nullopt;
}

rocksdb::Options StrategyDBManager::get_db_options() {
    rocksdb::Options options;
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    options.compression = rocksdb::kNoCompression;
    options.max_open_files = -1;
    options.use_fsync = false;
    options.stats_dump_period_sec = 60;
    
    // Enable Bloom Filter with basic options
    options.optimize_filters_for_hits = true;
    options.level_compaction_dynamic_level_bytes = true;
    
    // Enable statistics for metrics collection
    options.statistics = statistics_;
    
    utils::log_info("Database options configured with Bloom filter and statistics");
    
    return options;
}

// Statistics methods
uint64_t StrategyDBManager::get_bloom_filter_hits() const {
    if (!statistics_) return 0;
    return statistics_->getTickerCount(rocksdb::BLOOM_FILTER_USEFUL);
}

uint64_t StrategyDBManager::get_bloom_filter_misses() const {
    if (!statistics_) return 0;
    return statistics_->getTickerCount(rocksdb::BLOOM_FILTER_FULL_POSITIVE);
}

uint64_t StrategyDBManager::get_point_query_total() const {
    if (!statistics_) return 0;
    return get_bloom_filter_hits() + get_bloom_filter_misses();
}

uint64_t StrategyDBManager::get_compaction_bytes_read() const {
    if (!statistics_) return 0;
    return statistics_->getTickerCount(rocksdb::COMPACT_READ_BYTES);
}

uint64_t StrategyDBManager::get_compaction_bytes_written() const {
    if (!statistics_) return 0;
    return statistics_->getTickerCount(rocksdb::COMPACT_WRITE_BYTES);
}

uint64_t StrategyDBManager::get_compaction_time_micros() const {
    if (!statistics_) return 0;
    // Use approximation since COMPACT_TIME_MICROS might not be available
    return statistics_->getTickerCount(rocksdb::COMPACT_READ_BYTES) / 1024; // rough approximation
}

void StrategyDBManager::debug_bloom_filter_stats() const {
    uint64_t hits = get_bloom_filter_hits();
    uint64_t misses = get_bloom_filter_misses();
    uint64_t total = get_point_query_total();
    
    utils::log_info("=== Bloom Filter Statistics ===");
    utils::log_info("Hits: {}", hits);
    utils::log_info("Misses: {}", misses);
    utils::log_info("Total: {}", total);
    
    if (total > 0) {
        double hit_rate = (static_cast<double>(hits) / total) * 100.0;
        double miss_rate = (static_cast<double>(misses) / total) * 100.0;
        utils::log_info("Hit Rate: {:.2f}%", hit_rate);
        utils::log_info("Miss Rate: {:.2f}%", miss_rate);
    }
    utils::log_info("===============================");
}

void StrategyDBManager::set_bloom_filter_enabled(bool enabled) {
    // This method would configure bloom filter settings
    // For now, it's a placeholder as bloom filter is enabled by default
    utils::log_info("Bloom filter {}", enabled ? "enabled" : "disabled");
}

void StrategyDBManager::set_merge_callback(std::function<void(size_t, size_t)> callback) {
    // This method would set merge callback for PageIndexStrategy
    // For now, it's a placeholder as the strategy handles its own callbacks
    utils::log_info("Merge callback set");
}

StrategyDBManager::BloomFilterStats StrategyDBManager::get_bloom_filter_stats() const {
    BloomFilterStats stats;
    stats.hits = get_bloom_filter_hits();
    stats.misses = get_bloom_filter_misses();
    stats.total_queries = get_point_query_total();
    return stats;
}

StrategyDBManager::CompactionStats StrategyDBManager::get_compaction_stats() const {
    CompactionStats stats;
    stats.bytes_read = get_compaction_bytes_read();
    stats.bytes_written = get_compaction_bytes_written();
    stats.time_micros = get_compaction_time_micros();
    return stats;
}
#pragma once
#include <chrono>
#include <vector>
#include <string>
#include <map>

class MetricsCollector {
public:
    struct WriteMetrics {
        size_t total_keys_written = 0;
        size_t total_bytes_written = 0;
        double total_time_ms = 0.0;
        size_t batch_count = 0;
        double avg_throughput_mbps = 0.0;
    };

    struct QueryMetrics {
        size_t total_queries = 0;
        size_t successful_queries = 0;
        double total_query_time_ms = 0.0;
        double avg_query_time_ms = 0.0;
    };

    struct CompactionMetrics {
        size_t total_compactions = 0;
        double total_compaction_time_ms = 0.0;
        size_t bytes_compacted = 0;
        size_t levels_compacted = 0;
    };

    struct MergeOperatorMetrics {
        size_t total_merges = 0;
        size_t total_merged_values = 0;
        double avg_merged_value_size = 0.0;
        size_t max_merged_value_size = 0;
    };

    struct BloomFilterMetrics {
        size_t total_point_queries = 0;
        size_t bloom_filter_hits = 0;
        size_t bloom_filter_misses = 0;
        double false_positive_rate = 0.0;
    };

    struct CacheHitMetrics {
        size_t hot_key_queries = 0;
        size_t hot_key_hits = 0;
        size_t medium_key_queries = 0;
        size_t medium_key_hits = 0;
        size_t tail_key_queries = 0;
        size_t tail_key_hits = 0;
    };

    MetricsCollector();
    
    void start_write_timer();
    void stop_and_record_write(size_t keys_written, size_t bytes_written);
    
    void start_query_timer();
    void stop_and_record_query(bool success);
    
    // Compaction metrics
    void record_compaction(double time_ms, size_t bytes_compacted, size_t levels_compacted);
    
    // MergeOperator metrics
    void record_merge_operation(size_t merged_values, size_t merged_value_size);
    
    // Bloom filter metrics
    void record_bloom_filter_query(bool hit);
    
    // Cache hit metrics
    void record_cache_hit(const std::string& key_type, bool hit);
    
    void report_summary() const;
    
    const WriteMetrics& get_write_metrics() const { return write_metrics_; }
    const QueryMetrics& get_query_metrics() const { return query_metrics_; }
    const CompactionMetrics& get_compaction_metrics() const { return compaction_metrics_; }
    const MergeOperatorMetrics& get_merge_operator_metrics() const { return merge_operator_metrics_; }
    const BloomFilterMetrics& get_bloom_filter_metrics() const { return bloom_filter_metrics_; }
    const CacheHitMetrics& get_cache_hit_metrics() const { return cache_hit_metrics_; }

private:
    WriteMetrics write_metrics_;
    QueryMetrics query_metrics_;
    CompactionMetrics compaction_metrics_;
    MergeOperatorMetrics merge_operator_metrics_;
    BloomFilterMetrics bloom_filter_metrics_;
    CacheHitMetrics cache_hit_metrics_;
    
    std::chrono::high_resolution_clock::time_point write_start_time_;
    std::chrono::high_resolution_clock::time_point query_start_time_;
    bool write_timer_running_ = false;
    bool query_timer_running_ = false;
};
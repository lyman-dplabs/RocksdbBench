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

    MetricsCollector();
    
    void start_write_timer();
    void stop_and_record_write(size_t keys_written, size_t bytes_written);
    
    void start_query_timer();
    void stop_and_record_query(bool success);
    
    void report_summary() const;
    
    const WriteMetrics& get_write_metrics() const { return write_metrics_; }
    const QueryMetrics& get_query_metrics() const { return query_metrics_; }

private:
    WriteMetrics write_metrics_;
    QueryMetrics query_metrics_;
    
    std::chrono::high_resolution_clock::time_point write_start_time_;
    std::chrono::high_resolution_clock::time_point query_start_time_;
    bool write_timer_running_ = false;
    bool query_timer_running_ = false;
};
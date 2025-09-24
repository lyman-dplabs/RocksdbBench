#include "metrics_collector.hpp"
#include <iostream>
#include <iomanip>

MetricsCollector::MetricsCollector() = default;

void MetricsCollector::start_write_timer() {
    if (write_timer_running_) return;
    write_start_time_ = std::chrono::high_resolution_clock::now();
    write_timer_running_ = true;
}

void MetricsCollector::stop_and_record_write(size_t keys_written, size_t bytes_written) {
    if (!write_timer_running_) return;
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - write_start_time_);
    double time_ms = duration.count();
    
    write_metrics_.total_keys_written += keys_written;
    write_metrics_.total_bytes_written += bytes_written;
    write_metrics_.total_time_ms += time_ms;
    write_metrics_.batch_count++;
    
    if (time_ms > 0) {
        double throughput_mbps = (bytes_written / (1024.0 * 1024.0)) / (time_ms / 1000.0);
        write_metrics_.avg_throughput_mbps = 
            (write_metrics_.avg_throughput_mbps * (write_metrics_.batch_count - 1) + throughput_mbps) / 
            write_metrics_.batch_count;
    }
    
    write_timer_running_ = false;
}

void MetricsCollector::start_query_timer() {
    if (query_timer_running_) return;
    query_start_time_ = std::chrono::high_resolution_clock::now();
    query_timer_running_ = true;
}

void MetricsCollector::stop_and_record_query(bool success) {
    if (!query_timer_running_) return;
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - query_start_time_);
    double time_ms = duration.count() / 1000.0;
    
    query_metrics_.total_queries++;
    if (success) {
        query_metrics_.successful_queries++;
    }
    query_metrics_.total_query_time_ms += time_ms;
    query_metrics_.avg_query_time_ms = query_metrics_.total_query_time_ms / query_metrics_.total_queries;
    
    query_timer_running_ = false;
}

void MetricsCollector::report_summary() const {
    std::cout << "\n=== Performance Metrics Summary ===\n";
    
    std::cout << "\nWrite Metrics:\n";
    std::cout << "  Total keys written: " << write_metrics_.total_keys_written << "\n";
    std::cout << "  Total bytes written: " << write_metrics_.total_bytes_written << "\n";
    std::cout << "  Total write time: " << std::fixed << std::setprecision(2) 
              << write_metrics_.total_time_ms << " ms\n";
    std::cout << "  Write batches: " << write_metrics_.batch_count << "\n";
    
    if (write_metrics_.total_time_ms > 0) {
        double avg_throughput = (write_metrics_.total_bytes_written / (1024.0 * 1024.0)) / 
                               (write_metrics_.total_time_ms / 1000.0);
        std::cout << "  Average write throughput: " << std::fixed << std::setprecision(2) 
                  << avg_throughput << " MB/s\n";
    }
    
    std::cout << "\nQuery Metrics:\n";
    std::cout << "  Total queries: " << query_metrics_.total_queries << "\n";
    std::cout << "  Successful queries: " << query_metrics_.successful_queries << "\n";
    std::cout << "  Query success rate: " << std::fixed << std::setprecision(2) 
              << (query_metrics_.total_queries > 0 ? 
                 (query_metrics_.successful_queries * 100.0 / query_metrics_.total_queries) : 0.0) << "%\n";
    std::cout << "  Average query time: " << std::fixed << std::setprecision(3) 
              << query_metrics_.avg_query_time_ms << " ms\n";
    
    std::cout << "===================================\n";
}
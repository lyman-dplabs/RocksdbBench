#include "data_generator.hpp"
#include <random>
#include <sstream>
#include <iomanip>
#include <thread>
#include <vector>
#include <mutex>
#include <cstring>

DataGenerator::DataGenerator(const Config& config) : config_(config), rng_(std::random_device{}()) {
    generate_initial_keys_parallel();
}

void DataGenerator::generate_initial_keys_parallel() {
    all_keys_.resize(config_.total_keys);
    
    const size_t num_threads = std::thread::hardware_concurrency();
    const size_t keys_per_thread = (config_.total_keys + num_threads - 1) / num_threads;
    
    std::vector<std::thread> threads;
    std::mutex rng_mutex;
    
    auto worker = [&](size_t start_idx, size_t end_idx) {
        std::mt19937 local_rng(std::random_device{}());
        std::uniform_int_distribution<uint8_t> hex_dist(0, 15);
        std::uniform_int_distribution<uint32_t> slot_dist(0, 999999);
        
        for (size_t i = start_idx; i < end_idx && i < config_.total_keys; ++i) {
            std::string addr = "0x";
            for (int j = 0; j < 40; ++j) {
                addr += "0123456789abcdef"[hex_dist(local_rng)];
            }
            
            std::string slot = "slot" + std::to_string(slot_dist(local_rng));
            all_keys_[i] = addr + "#" + slot;
        }
    };
    
    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * keys_per_thread;
        size_t end = start + keys_per_thread;
        threads.emplace_back(worker, start, end);
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
}

std::vector<size_t> DataGenerator::generate_hotspot_update_indices(size_t batch_size) {
    std::vector<size_t> indices;
    indices.reserve(batch_size);
    
    size_t hotspot_count = static_cast<size_t>(batch_size * 0.8);
    size_t medium_count = static_cast<size_t>(batch_size * 0.1);
    size_t tail_count = batch_size - hotspot_count - medium_count;
    
    std::uniform_int_distribution<size_t> hotspot_dist(0, config_.hotspot_count - 1);
    std::uniform_int_distribution<size_t> medium_dist(config_.hotspot_count, 
                                                     config_.hotspot_count + config_.medium_count - 1);
    std::uniform_int_distribution<size_t> tail_dist(config_.hotspot_count + config_.medium_count, 
                                                   config_.total_keys - 1);
    
    for (size_t i = 0; i < hotspot_count; ++i) {
        indices.push_back(hotspot_dist(rng_));
    }
    
    for (size_t i = 0; i < medium_count; ++i) {
        indices.push_back(medium_dist(rng_));
    }
    
    for (size_t i = 0; i < tail_count; ++i) {
        indices.push_back(tail_dist(rng_));
    }
    
    std::shuffle(indices.begin(), indices.end(), rng_);
    return indices;
}

std::string DataGenerator::generate_unique_random_value(uint64_t index) {
    std::string value;
    value.resize(32);
    
    // 基于唯一索引生成确定性"随机"数据
    // 使用多个不同的hash函数和黄金比例常数保证分布均匀
    uint64_t base_index = index + 0x9E3779B97F4A7C15ULL; // 黄金比例常数
    uint64_t hash1 = std::hash<uint64_t>{}(base_index);
    uint64_t hash2 = std::hash<uint64_t>{}(base_index ^ 0x87654321FEDCBA98ULL);
    uint64_t hash3 = std::hash<uint64_t>{}(base_index ^ 0x123456789ABCDEFULL);
    uint64_t hash4 = std::hash<uint64_t>{}(base_index ^ 0xFEDCBA9876543210ULL);
    
    // 进一步打乱，避免hash函数的线性特性
    hash1 = (hash1 ^ (hash1 >> 30)) * 0xBF58476D1CE4E5B9ULL;
    hash2 = (hash2 ^ (hash2 >> 30)) * 0xBF58476D1CE4E5B9ULL;
    hash3 = (hash3 ^ (hash3 >> 30)) * 0xBF58476D1CE4E5B9ULL;
    hash4 = (hash4 ^ (hash4 >> 30)) * 0xBF58476D1CE4E5B9ULL;
    
    hash1 = (hash1 ^ (hash1 >> 27)) * 0x94D049BB133111EBULL;
    hash2 = (hash2 ^ (hash2 >> 27)) * 0x94D049BB133111EBULL;
    hash3 = (hash3 ^ (hash3 >> 27)) * 0x94D049BB133111EBULL;
    hash4 = (hash4 ^ (hash4 >> 27)) * 0x94D049BB133111EBULL;
    
    hash1 = hash1 ^ (hash1 >> 31);
    hash2 = hash2 ^ (hash2 >> 31);
    hash3 = hash3 ^ (hash3 >> 31);
    hash4 = hash4 ^ (hash4 >> 31);
    
    memcpy(&value[0], &hash1, 8);
    memcpy(&value[8], &hash2, 8);
    memcpy(&value[16], &hash3, 8);
    memcpy(&value[24], &hash4, 8);
    
    return value;
}

std::string DataGenerator::generate_random_value() {
    uint64_t current_index = global_random_value_count_.fetch_add(1, std::memory_order_relaxed);
    return generate_unique_random_value(current_index);
}

// 优化后的批量随机值生成 - 使用hash方法保证唯一性和高性能
std::vector<std::string> DataGenerator::generate_random_values(size_t count) {
    std::vector<std::string> values;
    values.reserve(count);
    
    // 批量获取唯一索引范围，保证全局唯一性
    uint64_t start_index = global_random_value_count_.fetch_add(count, std::memory_order_relaxed);
    
    for (size_t i = 0; i < count; ++i) {
        values.push_back(generate_unique_random_value(start_index + i));
    }
    
    return values;
}

std::string DataGenerator::generate_address() {
    std::ostringstream oss;
    oss << "0x";
    
    std::uniform_int_distribution<uint8_t> dist(0, 15);
    for (int i = 0; i < 40; ++i) {
        oss << std::hex << dist(rng_);
    }
    
    return oss.str();
}

std::string DataGenerator::generate_slot() {
    std::ostringstream oss;
    oss << "slot";
    
    std::uniform_int_distribution<uint32_t> dist(0, 999999);
    oss << dist(rng_);
    
    return oss.str();
}

std::string DataGenerator::create_addr_slot(const std::string& addr, const std::string& slot) {
    return addr + "#" + slot;
}
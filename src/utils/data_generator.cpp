#include "data_generator.hpp"
#include <random>
#include <sstream>
#include <iomanip>

DataGenerator::DataGenerator(const Config& config) : config_(config), rng_(std::random_device{}()) {
    all_keys_ = generate_initial_keys();
}

std::vector<std::string> DataGenerator::generate_initial_keys() {
    std::vector<std::string> keys;
    keys.reserve(config_.total_keys);
    
    for (size_t i = 0; i < config_.total_keys; ++i) {
        std::string addr = generate_address();
        std::string slot = generate_slot();
        keys.push_back(create_addr_slot(addr, slot));
    }
    
    return keys;
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

std::string DataGenerator::generate_random_value() {
    std::string value;
    value.resize(32);
    
    std::uniform_int_distribution<uint8_t> dist(1, 255);
    
    for (size_t i = 0; i < 32; ++i) {
        value[i] = static_cast<char>(dist(rng_));
    }
    
    return value;
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
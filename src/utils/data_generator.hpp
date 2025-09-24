#pragma once
#include <vector>
#include <string>
#include <random>
#include <algorithm>
#include "../core/types.hpp"

class DataGenerator {
public:
    struct Config {
        size_t total_keys = 100000000;
        double hotspot_ratio = 0.8;
        size_t hotspot_count = 10000000;
        size_t medium_count = 20000000;
        size_t tail_count = 70000000;
    };

    explicit DataGenerator(const Config& config);
    
    const std::vector<std::string>& get_all_keys() const { return all_keys_; }
    std::vector<size_t> generate_hotspot_update_indices(size_t batch_size);
    std::string generate_random_value();
    std::vector<std::string> generate_random_values(size_t count);
    
private:
    Config config_;
    std::mt19937 rng_;
    std::vector<std::string> all_keys_;
    
    void generate_initial_keys_parallel();
    std::string generate_address();
    std::string generate_slot();
    std::string create_addr_slot(const std::string& addr, const std::string& slot);
};
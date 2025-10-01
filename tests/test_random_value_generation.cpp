#include "../src/utils/data_generator.hpp"
#include <chrono>
#include <iostream>
#include <unordered_set>

int main() {
    DataGenerator::Config config;
    config.total_keys = 100000;
    DataGenerator generator(config);
    
    std::cout << "Testing random value generation performance...\n";
    
    // Test 1: Single value generation
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < 10000; ++i) {
        auto value = generator.generate_random_value();
        (void)value; // Avoid optimization
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration1 = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    std::cout << "Single value generation (10k calls): " << duration1.count() << " μs\n";
    std::cout << "Average per call: " << duration1.count() / 10000.0 << " μs\n";
    
    // Test 2: Batch value generation
    start = std::chrono::high_resolution_clock::now();
    auto values = generator.generate_random_values(10000);
    end = std::chrono::high_resolution_clock::now();
    auto duration2 = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    std::cout << "Batch value generation (10k values): " << duration2.count() << " μs\n";
    std::cout << "Average per value: " << duration2.count() / 10000.0 << " μs\n";
    
    // Test 3: Uniqueness test
    start = std::chrono::high_resolution_clock::now();
    auto values2 = generator.generate_random_values(1000);
    auto values3 = generator.generate_random_values(1000);
    end = std::chrono::high_resolution_clock::now();
    
    std::unordered_set<std::string> unique_values;
    for (const auto& v : values2) {
        unique_values.insert(v);
    }
    for (const auto& v : values3) {
        unique_values.insert(v);
    }
    
    std::cout << "Uniqueness test: " << unique_values.size() << " unique values out of 2000\n";
    std::cout << "Uniqueness rate: " << (unique_values.size() * 100.0 / 2000) << "%\n";
    
    // Test 4: Sample values to show they look random
    std::cout << "\nSample values (first 3):\n";
    for (size_t i = 0; i < std::min(size_t(3), values.size()); ++i) {
        std::cout << "Value " << i << ": ";
        for (int j = 0; j < std::min(8, (int)values[i].size()); ++j) {
            printf("%02x", (unsigned char)values[i][j]);
        }
        std::cout << "...\n";
    }
    
    return 0;
}
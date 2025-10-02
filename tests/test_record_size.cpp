#include "../src/core/types.hpp"
#include "../src/utils/logger.hpp"
#include "../src/utils/data_generator.hpp"
#include "../src/strategies/dual_rocksdb_strategy.hpp"
#include <iostream>
#include <iomanip>

int main() {
    utils::init_logger("test_record_size");
    
    // 创建数据生成器
    DataGenerator::Config config;
    config.total_keys = 1000;
    config.hotspot_count = 100;
    config.medium_count = 200;
    config.tail_count = 700;
    
    DataGenerator generator(config);
    
    // 生成一些测试数据
    auto test_indices = generator.generate_hotspot_update_indices(10);
    auto test_values = generator.generate_random_values(10);
    const auto& all_keys = generator.get_all_keys();
    
    std::cout << "=== Record Size Analysis ===" << std::endl;
    std::cout << std::fixed << std::setprecision(2);
    
    for (size_t i = 0; i < 10; ++i) {
        DataRecord record{
            static_cast<BlockNum>(i),                    // block_num
            all_keys[test_indices[i]],                   // addr_slot (string)
            test_values[i]                               // value (string)
        };
        
        // 实际测量各个部分的大小
        size_t value_size = record.value.size();
        size_t key_size = record.addr_slot.size();
        size_t block_num_size = sizeof(record.block_num);
        
        // 模拟dual_rocksdb_strategy.cpp中的计算
        size_t calculated_size = value_size + key_size + block_num_size + 100;
        
        // 计算实际存储的key大小 (dual rocksdb会构造新的key)
        // range_num (4 bytes) + addr_slot + block_num (8 bytes) + separator
        size_t actual_data_key_size = 4 + key_size + 8 + 1; // 假设1个separator
        
        std::cout << "\nRecord " << i << ":" << std::endl;
        std::cout << "  Value size: " << value_size << " bytes" << std::endl;
        std::cout << "  Key (addr_slot) size: " << key_size << " bytes" << std::endl;
        std::cout << "  BlockNum size: " << block_num_size << " bytes" << std::endl;
        std::cout << "  Estimated data key size: " << actual_data_key_size << " bytes" << std::endl;
        std::cout << "  Calculated batch size: " << calculated_size << " bytes" << std::endl;
        std::cout << "  Key content: '" << record.addr_slot << "'" << std::endl;
        std::cout << "  Value content: '" << record.value << "'" << std::endl;
    }
    
    // 分析range index的存储大小
    std::cout << "\n=== Range Index Analysis ===" << std::endl;
    std::cout << "Range index stores: vector<uint32_t> for each address" << std::endl;
    std::cout << "  uint32_t size: " << sizeof(uint32_t) << " bytes" << std::endl;
    std::cout << "  Typical ranges per address: 1-3" << std::endl;
    std::cout << "  Range index entry size: " << sizeof(uint32_t) * 2 << " bytes (average)" << std::endl;
    
    // 分析WriteBatch开销
    std::cout << "\n=== WriteBatch Overhead Analysis ===" << std::endl;
    std::cout << "The '+100' bytes in calculation likely covers:" << std::endl;
    std::cout << "  - WriteBatch internal overhead (~20-50 bytes per operation)" << std::endl;
    std::cout << "  - RocksDB key-value metadata (~10-20 bytes)" << std::endl;
    std::cout << "  - String allocation overhead (~10-20 bytes)" << std::endl;
    std::cout << "  - Alignment and padding" << std::endl;
    
    return 0;
}
#include "core/strategy_db_manager.hpp"
#include "strategies/dual_rocksdb_strategy.hpp"
#include <filesystem>
#include <vector>
#include <iostream>
#include <cassert>

class DualBlockSizeTest {
public:
    void run_test() {
        std::cout << "=== DualRocksDBStrategy Block Size 测试 ===" << std::endl;
        
        // 配置：模拟你的设置
        DualRocksDBStrategy::Config config;
        config.batch_size_blocks = 75000;  // 75000个block后flush
        config.max_batch_size_bytes = 300UL * 1024 * 1024 * 1024;  // 300GB
        
        std::cout << "配置：" << std::endl;
        std::cout << "  batch_size_blocks: " << config.batch_size_blocks << " blocks" << std::endl;
        std::cout << "  max_batch_size_bytes: " << config.max_batch_size_bytes / (1024*1024*1024) << " GB" << std::endl;
        
        test_db_path_ = std::filesystem::temp_directory_path() / "dual_block_size_test";
        std::filesystem::remove_all(test_db_path_);
        
        auto strategy = std::make_unique<DualRocksDBStrategy>(config);
        auto db_manager = std::make_unique<StrategyDBManager>(test_db_path_.string(), std::move(strategy));
        
        if (!db_manager->open(true)) {
            throw std::runtime_error("Failed to open test database");
        }
        
        std::cout << "\n=== 测试1：写入100个blocks，每个10,000个records ===" << std::endl;
        
        // 创建测试数据：100个blocks，每个10,000个records
        for (int block_num = 0; block_num < 100; ++block_num) {
            std::vector<DataRecord> records;
            records.reserve(10000);  // 预留10,000个records
            
            for (int i = 0; i < 10000; ++i) {
                DataRecord record;
                record.block_num = block_num;
                record.addr_slot = "addr_" + std::to_string(block_num) + "_" + std::to_string(i);
                record.value = "value_" + std::to_string(i) + "_some_longer_value_to_increase_size";
                records.push_back(record);
            }
            
            std::cout << "写入block " << block_num << " (10,000 records)..." << std::endl;
            db_manager->write_initial_load_batch(records);
        }
        
        std::cout << "\n=== 测试2：计算预期的数据大小 ===" << std::endl;
        // 计算一个record的大小
        DataRecord sample_record;
        sample_record.block_num = 0;
        sample_record.addr_slot = "addr_0_0";
        sample_record.value = "value_0_some_longer_value_to_increase_size";
        
        size_t record_size = sample_record.addr_slot.size() + sample_record.value.size() + sizeof(sample_record.block_num) + 100;
        size_t expected_block_size = 10000 * record_size;
        size_t expected_total_size = 100 * expected_block_size;
        
        std::cout << "单个record估算大小: " << record_size << " bytes" << std::endl;
        std::cout << "单个block预期大小: " << expected_block_size / (1024*1024) << " MB" << std::endl;
        std::cout << "100个blocks预期总大小: " << expected_total_size / (1024*1024) << " MB" << std::endl;
        
        std::cout << "\n=== 关闭数据库，查看实际的flush日志 ===" << std::endl;
        db_manager->close();
        std::filesystem::remove_all(test_db_path_);
        
        std::cout << "\n=== 对比分析 ===" << std::endl;
        std::cout << "请查看上面的flush日志，对比：" << std::endl;
        std::cout << "1. Block数量应该是100" << std::endl;
        std::cout << "2. Byte数量应该接近 " << expected_total_size / (1024*1024) << " MB" << std::endl;
        std::cout << "3. 如果bytes远小于预期，说明数据有问题" << std::endl;
    }
    
private:
    std::filesystem::path test_db_path_;
};

int main() {
    try {
        DualBlockSizeTest tester;
        tester.run_test();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "测试失败: " << e.what() << std::endl;
        return 1;
    }
}
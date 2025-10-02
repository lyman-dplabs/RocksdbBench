#include "core/strategy_db_manager.hpp"
#include "strategies/dual_rocksdb_strategy.hpp"
#include <filesystem>
#include <vector>
#include <iostream>
#include <cassert>

class DualBatchTest {
public:
    void run_test() {
        std::cout << "=== DualRocksDBStrategy 按block攒批测试 ===" << std::endl;
        
        // 配置：75000个block
        DualRocksDBStrategy::Config config;
        config.batch_size_blocks = 75000;  // 75000个block后flush
        config.max_batch_size_bytes = 4UL * 1024 * 1024 * 1024;  // 4GB，应该不会先达到
        
        std::cout << "配置：" << std::endl;
        std::cout << "  batch_size_blocks: " << config.batch_size_blocks << " blocks" << std::endl;
        std::cout << "  max_batch_size_bytes: " << config.max_batch_size_bytes / (1024*1024) << " MB" << std::endl;
        
        test_db_path_ = std::filesystem::temp_directory_path() / "dual_batch_test";
        std::filesystem::remove_all(test_db_path_);
        
        auto strategy = std::make_unique<DualRocksDBStrategy>(config);
        auto db_manager = std::make_unique<StrategyDBManager>(test_db_path_.string(), std::move(strategy));
        
        if (!db_manager->open(true)) {
            throw std::runtime_error("Failed to open test database");
        }
        
        // 创建测试数据：每个block包含不同数量的records
        std::vector<std::vector<DataRecord>> test_blocks;
        
        // 生成100个blocks，每个block有100条记录
        for (int i = 0; i < 100; ++i) {
            std::vector<DataRecord> block;
            for (int j = 0; j < 100; ++j) {
                DataRecord record;
                record.block_num = i;
                record.addr_slot = "addr_" + std::to_string(i) + "_" + std::to_string(j);
                record.value = "value_" + std::to_string(j);
                block.push_back(record);
            }
            test_blocks.push_back(std::move(block));
        }
        
        std::cout << "\n=== 测试：写入100个blocks ===" << std::endl;
        std::cout << "预期：应该积累所有100个blocks，因为远少于75000个blocks的限制" << std::endl;
        
        // 使用initial load模式写入
        for (int i = 0; i < test_blocks.size(); ++i) {
            std::cout << "写入第 " << (i+1) << " 个block (包含 " << test_blocks[i].size() << " 条records)..." << std::endl;
            db_manager->write_initial_load_batch(test_blocks[i]);
        }
        
        std::cout << "\n=== 验证：检查是否真的攒批了 ===" << std::endl;
        std::cout << "如果攒批正确，应该看到只有1次flush日志" << std::endl;
        std::cout << "如果每个block都立即写入，应该看到100次flush日志" << std::endl;
        
        // 关闭数据库，触发cleanup flush
        db_manager->close();
        std::filesystem::remove_all(test_db_path_);
        
        std::cout << "\n=== 测试完成 ===" << std::endl;
        std::cout << "请查看上面的日志输出，确认flush次数！" << std::endl;
    }
    
private:
    std::filesystem::path test_db_path_;
};

int main() {
    try {
        DualBatchTest tester;
        tester.run_test();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "测试失败: " << e.what() << std::endl;
        return 1;
    }
}
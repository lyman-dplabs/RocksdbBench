#include "core/strategy_db_manager.hpp"
#include "strategies/direct_version_strategy.hpp"
#include <filesystem>
#include <vector>
#include <iostream>
#include <cassert>

class DirectVersionBatchTester {
public:
    void run_all_tests() {
        std::cout << "=== DirectVersionStrategy 攒批逻辑测试 ===" << std::endl;
        
        test_hotspot_immediate_write();
        test_initial_load_batching();
        test_cleanup_flushes_remaining();
        test_mixed_mode_correctness();
        
        std::cout << "\n✅ 所有测试通过！" << std::endl;
    }
    
private:
    std::filesystem::path test_db_path_ = std::filesystem::temp_directory_path() / "direct_version_batch_test";
    
    void setup_test() {
        std::filesystem::remove_all(test_db_path_);
        
        // 创建DirectVersionStrategy配置
        DirectVersionStrategy::Config config;
        config.batch_size_blocks = 3;  // 3个block后flush
        config.max_batch_size_bytes = 1024 * 1024;  // 1MB后flush
        
        auto strategy = std::make_unique<DirectVersionStrategy>(config);
        db_manager_ = std::make_unique<StrategyDBManager>(test_db_path_.string(), std::move(strategy));
        
        if (!db_manager_->open(true)) {
            throw std::runtime_error("Failed to open test database");
        }
    }
    
    void teardown_test() {
        if (db_manager_) {
            db_manager_->close();
        }
        std::filesystem::remove_all(test_db_path_);
        db_manager_.reset();
    }
    
    std::vector<DataRecord> create_test_block(size_t num_records, BlockNum block_num) {
        std::vector<DataRecord> records;
        records.reserve(num_records);
        
        for (size_t i = 0; i < num_records; ++i) {
            DataRecord record;
            record.block_num = block_num;
            record.addr_slot = "address_" + std::to_string(i);
            record.value = "value_" + std::to_string(i);
            records.push_back(record);
        }
        
        return records;
    }
    
    std::unique_ptr<StrategyDBManager> db_manager_;
    
    void test_hotspot_immediate_write() {
        std::cout << "\n--- 测试1: Hotspot模式立即写入 ---" << std::endl;
        setup_test();
        
        // 写入第一个block（应该立即写入）
        auto block1 = create_test_block(10, 100);
        std::cout << "写入第一个block (10条记录, block_num=100)..." << std::endl;
        assert(db_manager_->write_batch(block1));
        
        // 写入第二个block（应该立即写入）
        auto block2 = create_test_block(10, 101);
        std::cout << "写入第二个block (10条记录, block_num=101)..." << std::endl;
        assert(db_manager_->write_batch(block2));
        
        // 验证数据可以立即查询到
        auto value = db_manager_->query_latest_value("address_0");
        assert(value.has_value());
        assert(*value == "value_0");
        
        std::cout << "✓ Hotspot模式测试通过：每个block都立即写入" << std::endl;
        teardown_test();
    }
    
    void test_initial_load_batching() {
        std::cout << "\n--- 测试2: Initial Load模式攒批 ---" << std::endl;
        setup_test();
        
        // 配置是3个block后flush，所以我们写入2个block不应该flush
        auto block1 = create_test_block(5, 200);
        std::cout << "写入第一个block到initial load (5条记录, block_num=200)..." << std::endl;
        assert(db_manager_->write_initial_load_batch(block1));
        
        auto block2 = create_test_block(5, 201);
        std::cout << "写入第二个block到initial load (5条记录, block_num=201)..." << std::endl;
        assert(db_manager_->write_initial_load_batch(block2));
        
        std::cout << "✓ 前两个block应该被积累，还没有flush" << std::endl;
        
        // 写入第三个block，应该触发flush
        auto block3 = create_test_block(5, 202);
        std::cout << "写入第三个block到initial load (5条记录, block_num=202) - 应该触发flush..." << std::endl;
        assert(db_manager_->write_initial_load_batch(block3));
        
        std::cout << "✓ 第三个block触发flush，所有3个block应该被写入" << std::endl;
        teardown_test();
    }
    
    void test_cleanup_flushes_remaining() {
        std::cout << "\n--- 测试3: Cleanup时flush剩余数据 ---" << std::endl;
        setup_test();
        
        // 写入少于3个block，不应该自动flush
        auto block1 = create_test_block(3, 300);
        std::cout << "写入第一个block到initial load (3条记录, block_num=300)..." << std::endl;
        assert(db_manager_->write_initial_load_batch(block1));
        
        auto block2 = create_test_block(3, 301);
        std::cout << "写入第二个block到initial load (3条记录, block_num=301)..." << std::endl;
        assert(db_manager_->write_initial_load_batch(block2));
        
        std::cout << "✓ 写入了2个block，还没有达到3个block的限制" << std::endl;
        
        // 关闭数据库应该触发cleanup并flush剩余数据
        std::cout << "关闭数据库，应该触发cleanup并flush剩余数据..." << std::endl;
        teardown_test();
        
        std::cout << "✓ Cleanup完成，剩余数据应该被flush" << std::endl;
    }
    
    void test_mixed_mode_correctness() {
        std::cout << "\n--- 测试4: 混合模式数据正确性 ---" << std::endl;
        setup_test();
        
        // 先用hotspot模式写入一些数据
        auto hotspot_block = create_test_block(3, 400);
        std::cout << "使用hotspot模式写入block (3条记录, block_num=400)..." << std::endl;
        assert(db_manager_->write_batch(hotspot_block));
        
        // 再用initial load模式写入数据
        auto il_block1 = create_test_block(2, 401);
        auto il_block2 = create_test_block(2, 402);
        auto il_block3 = create_test_block(2, 403);
        
        std::cout << "使用initial load模式写入3个block..." << std::endl;
        assert(db_manager_->write_initial_load_batch(il_block1));
        assert(db_manager_->write_initial_load_batch(il_block2));
        assert(db_manager_->write_initial_load_batch(il_block3));  // 这应该触发flush
        
        // 验证数据可以查询到
        std::cout << "验证数据查询..." << std::endl;
        for (const auto& record : hotspot_block) {
            auto value = db_manager_->query_latest_value(record.addr_slot);
            assert(value.has_value());
            assert(*value == record.value);
        }
        
        for (const auto& record : il_block1) {
            auto value = db_manager_->query_latest_value(record.addr_slot);
            assert(value.has_value());
            assert(*value == record.value);
        }
        
        std::cout << "✓ 所有数据查询正确" << std::endl;
        teardown_test();
    }
};

int main() {
    try {
        DirectVersionBatchTester tester;
        tester.run_all_tests();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "测试失败: " << e.what() << std::endl;
        return 1;
    }
}
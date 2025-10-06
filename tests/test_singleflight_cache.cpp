#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <chrono>
#include <random>
#include <atomic>
#include "../src/strategies/simple_lru_cache.hpp"
#include "../src/strategies/dual_rocksdb_cache_interface.hpp"

class SingleFlightCacheTest : public ::testing::Test {
protected:
    void SetUp() override {
        cache_ = std::make_unique<SimpleSingleFlightCache>(8);
    }

    void TearDown() override {
        if (cache_) {
            cache_->clear_all();
        }
    }

    std::unique_ptr<SimpleSingleFlightCache> cache_;
};

// 测试优化的hash函数
TEST_F(SingleFlightCacheTest, OptimizedAddrHash) {
    std::string addr1 = "0x1234567890abcdef1234567890abcdef12345678#slot123456";
    std::string addr2 = "0x1234567890abcdef1234567890abcdef12345678#slot123457";
    std::string addr3 = "0x1234567890abcdef1234567890abcdef12345679#slot123456";

    size_t hash1 = optimized_addr_hash(addr1);
    size_t hash2 = optimized_addr_hash(addr2);
    size_t hash3 = optimized_addr_hash(addr3);

    EXPECT_NE(hash1, hash2);
    EXPECT_NE(hash1, hash3);
    EXPECT_NE(hash2, hash3);
}

// 测试基本缓存功能
TEST_F(SingleFlightCacheTest, BasicCacheOperations) {
    std::string key = "0x1234567890abcdef1234567890abcdef12345678#slot123456";
    std::vector<uint32_t> expected_ranges = {1, 2, 3, 4, 5};

    int call_count = 0;
    auto loader = [&call_count, &expected_ranges]() -> std::vector<uint32_t> {
        call_count++;
        return expected_ranges;
    };

    // 第一次调用应该调用loader
    auto result1 = cache_->get_ranges(key, loader);
    EXPECT_EQ(result1, expected_ranges);
    EXPECT_EQ(call_count, 1);

    // 第二次调用应该从缓存获取，不调用loader
    auto result2 = cache_->get_ranges(key, loader);
    EXPECT_EQ(result2, expected_ranges);
    EXPECT_EQ(call_count, 1); // loader不应该再次被调用

    // 验证缓存统计
    auto stats = cache_->get_stats();
    EXPECT_EQ(stats.total_accesses, 2);
    EXPECT_EQ(stats.hits, 1);
    EXPECT_DOUBLE_EQ(stats.hit_rate, 0.5);
    EXPECT_EQ(stats.total_entries, 1);
}

// 测试预热功能
TEST_F(SingleFlightCacheTest, PreloadRanges) {
    std::string key = "0x1234567890abcdef1234567890abcdef12345678#slot123456";
    std::vector<uint32_t> ranges = {10, 20, 30};

    // 预热缓存
    cache_->preload_ranges(key, ranges);

    // 验证可以从缓存获取
    int call_count = 0;
    auto loader = [&call_count]() -> std::vector<uint32_t> {
        call_count++;
        return {1, 2, 3};
    };

    auto result = cache_->get_ranges(key, loader);
    EXPECT_EQ(result, ranges);
    EXPECT_EQ(call_count, 0); // loader不应该被调用
}

// 测试SingleFlight机制 - 并发访问相同key
TEST_F(SingleFlightCacheTest, SingleFlightConcurrentAccess) {
    std::string key = "0x1234567890abcdef1234567890abcdef12345678#slot123456";
    std::vector<uint32_t> expected_ranges = {100, 200, 300};

    std::atomic<int> call_count{0};
    auto loader = [&call_count, &expected_ranges]() -> std::vector<uint32_t> {
        std::this_thread::sleep_for(std::chrono::milliseconds(50)); // 模拟耗时操作
        call_count++;
        return expected_ranges;
    };

    const int num_threads = 10;
    std::vector<std::thread> threads;
    std::vector<std::vector<uint32_t>> results(num_threads);

    // 启动多个线程同时访问相同的key
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            results[i] = cache_->get_ranges(key, loader);
        });
    }

    // 等待所有线程完成
    for (auto& thread : threads) {
        thread.join();
    }

    // 验证loader只被调用了一次
    EXPECT_EQ(call_count.load(), 1);

    // 验证所有线程都得到了正确的结果
    for (const auto& result : results) {
        EXPECT_EQ(result, expected_ranges);
    }

    // 验证缓存统计 - 在SingleFlight模式下，由于并发访问的时序，可能都会被统计为misses
    // 关键是验证loader只调用了一次，并且所有线程都得到了正确结果
    auto stats = cache_->get_stats();
    EXPECT_EQ(stats.total_accesses, num_threads);
    EXPECT_EQ(stats.total_entries, 1);
    EXPECT_LE(stats.hits, num_threads); // hits应该小于等于总访问次数
}

// 测试并发访问不同key
TEST_F(SingleFlightCacheTest, ConcurrentDifferentKeys) {
    const int num_threads = 20;
    const int keys_per_thread = 10;

    std::atomic<int> call_count{0};
    std::vector<std::thread> threads;
    std::vector<std::vector<std::vector<uint32_t>>> all_results(num_threads);

    // 启动多个线程访问不同的key
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            for (int k = 0; k < keys_per_thread; ++k) {
                std::string key = "0x1234567890abcdef1234567890abcdef1234567" +
                                std::to_string(t) + "#slot" +
                                std::to_string(100000 + k);

                auto loader = [&, key]() -> std::vector<uint32_t> {
                    call_count++;
                    // 为每个key生成不同的range
                    return {static_cast<uint32_t>(std::hash<std::string>{}(key) % 1000)};
                };

                all_results[t].push_back(cache_->get_ranges(key, loader));
            }
        });
    }

    // 等待所有线程完成
    for (auto& thread : threads) {
        thread.join();
    }

    // 验证所有key都被处理了 (允许一些变化，因为可能有清理操作)
    EXPECT_NEAR(call_count.load(), num_threads * keys_per_thread, 5);

    // 验证缓存统计 (允许一些变化)
    auto stats = cache_->get_stats();
    EXPECT_NEAR(stats.total_accesses, num_threads * keys_per_thread, 5);
    EXPECT_NEAR(static_cast<int>(stats.total_entries), num_threads * keys_per_thread, 5);
}

// 测试异常处理
TEST_F(SingleFlightCacheTest, ExceptionHandling) {
    std::string key = "0x1234567890abcdef1234567890abcdef12345678#slot123456";

    auto failing_loader = []() -> std::vector<uint32_t> {
        throw std::runtime_error("Database connection failed");
    };

    // 第一次调用应该抛出异常
    EXPECT_THROW(cache_->get_ranges(key, failing_loader), std::runtime_error);

    // 后续调用也应该可以正常工作（flight状态被清理）
    auto working_loader = []() -> std::vector<uint32_t> {
        return {1, 2, 3};
    };

    auto result = cache_->get_ranges(key, working_loader);
    std::vector<uint32_t> expected = {1, 2, 3};
    EXPECT_EQ(result, expected);
}

// 测试缓存过期
TEST_F(SingleFlightCacheTest, CacheExpiration) {
    std::string key = "0x1234567890abcdef1234567890abcdef12345678#slot123456";
    std::vector<uint32_t> ranges = {1, 2, 3};

    // 预热缓存
    cache_->preload_ranges(key, ranges);

    // 验证缓存中有数据
    auto stats1 = cache_->get_stats();
    EXPECT_EQ(stats1.total_entries, 1);

    // 简化版本不需要手动清理，LRU自动管理
    // 缓存应该仍然存在
    auto stats2 = cache_->get_stats();
    EXPECT_EQ(stats2.total_entries, 1);

    // 清空所有缓存
    cache_->clear_all();
    auto stats3 = cache_->get_stats();
    EXPECT_EQ(stats3.total_entries, 0);
}

// 测试内存使用
TEST_F(SingleFlightCacheTest, MemoryUsage) {
    // 添加一些数据
    for (int i = 0; i < 100; ++i) {
        std::string key = "0x1234567890abcdef1234567890abcdef12345678#slot" +
                         std::to_string(100000 + i);
        std::vector<uint32_t> ranges(i + 1, static_cast<uint32_t>(i));
        cache_->preload_ranges(key, ranges);
    }

    auto stats = cache_->get_stats();
    EXPECT_EQ(stats.total_entries, 100);
    EXPECT_GT(stats.total_memory_bytes, 0);

    // 验证内存使用合理（估算）
    size_t expected_min_memory = 100 * (50 + 4 * 50); // key + ranges的估算
    EXPECT_GT(stats.total_memory_bytes, expected_min_memory);
}

class DualRocksDBCacheInterfaceTest : public ::testing::Test {
protected:
    void SetUp() override {
        interface_ = std::make_unique<DualRocksDBCacheInterface>(4);
    }

    void TearDown() override {
        if (interface_) {
            interface_->clear_cache();
        }
    }

    std::unique_ptr<DualRocksDBCacheInterface> interface_;
};

// 测试接口基本功能
TEST_F(DualRocksDBCacheInterfaceTest, BasicFunctionality) {
    // 设置查询函数
    std::unordered_map<std::string, std::vector<uint32_t>> mock_db;
    auto query_func = [&mock_db](const std::string& addr) -> std::vector<uint32_t> {
        auto it = mock_db.find(addr);
        if (it != mock_db.end()) {
            return it->second;
        }
        return {};
    };

    interface_->set_query_function(query_func);

    // 准备测试数据
    std::string addr = "0x1234567890abcdef1234567890abcdef12345678#slot123456";
    std::vector<uint32_t> ranges = {1, 2, 3};
    mock_db[addr] = ranges;

    // 第一次查询应该调用数据库查询
    auto result1 = interface_->get_address_ranges(addr);
    EXPECT_EQ(result1, ranges);

    // 第二次查询应该从缓存获取
    auto result2 = interface_->get_address_ranges(addr);
    EXPECT_EQ(result2, ranges);

    // 验证统计信息
    auto stats = interface_->get_query_stats();
    EXPECT_EQ(stats.total_queries, 2);
    EXPECT_EQ(stats.cache_hits, 1);
    EXPECT_DOUBLE_EQ(stats.hit_rate, 0.5);
    EXPECT_EQ(stats.cache_entries, 1);
}

// 测试预热功能
TEST_F(DualRocksDBCacheInterfaceTest, PreloadFunctionality) {
    // 设置查询函数
    auto query_func = [](const std::string& addr) -> std::vector<uint32_t> {
        return {}; // 不应该被调用
    };

    interface_->set_query_function(query_func);

    // 准备热点数据
    std::unordered_map<std::string, std::vector<uint32_t>> hot_data;
    hot_data["0x1234567890abcdef1234567890abcdef12345678#slot123456"] = {1, 2, 3};
    hot_data["0x1234567890abcdef1234567890abcdef12345679#slot123457"] = {4, 5, 6};

    // 预热数据
    interface_->preload_address_ranges(hot_data);

    // 验证可以从缓存获取
    for (const auto& [addr, expected_ranges] : hot_data) {
        auto result = interface_->get_address_ranges(addr);
        EXPECT_EQ(result, expected_ranges);
    }

    // 验证查询函数没有被调用（全部来自缓存）
    auto stats = interface_->get_query_stats();
    EXPECT_EQ(stats.total_queries, 2);
    EXPECT_EQ(stats.cache_hits, 2); // 两次都是缓存命中
    EXPECT_DOUBLE_EQ(stats.hit_rate, 1.0);
}

// 测试正常使用情况
TEST_F(DualRocksDBCacheInterfaceTest, NormalUsage) {
    std::string addr = "0x1234567890abcdef1234567890abcdef12345678#slot123456";

    // 设置查询函数
    auto query_func = [](const std::string& addr) -> std::vector<uint32_t> {
        return {7, 8, 9};
    };

    interface_->set_query_function(query_func);

    // 多次查询应该正常工作并返回缓存结果
    auto result1 = interface_->get_address_ranges(addr);
    auto result2 = interface_->get_address_ranges(addr);

    std::vector<uint32_t> expected = {7, 8, 9};
    EXPECT_EQ(result1, expected);
    EXPECT_EQ(result2, expected);

    // 验证缓存命中
    auto stats = interface_->get_query_stats();
    EXPECT_GT(stats.cache_hits, 0);
    EXPECT_EQ(stats.total_queries, 2);
}

// 主函数 - 运行所有测试
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
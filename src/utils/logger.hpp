#pragma once
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <fmt/core.h>
#include <memory>
#include <string>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <mutex>

namespace utils {

// 全局日志器初始化 - 支持动态命名
inline void init_logger(const std::string& strategy_name = "rocksdb_bench") {
    static bool initialized = false;
    if (!initialized) {
        // 创建logs目录
        system("mkdir -p logs 2>/dev/null");
        
        // 生成时间戳
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()) % 1000;
        
        std::ostringstream oss;
        oss << std::put_time(std::localtime(&time_t), "%Y%m%d_%H%M%S");
        oss << "_" << std::setfill('0') << std::setw(3) << ms.count();
        
        // 创建带时间戳的日志文件名
        std::string log_filename = "logs/" + strategy_name + "_" + oss.str() + ".log";
        
        // 创建控制台输出日志器
        auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        console_sink->set_pattern("%^[%Y-%m-%d %H:%M:%S.%e] [%n] [%l]%$ %v%$");
        
        // 创建文件输出日志器
        auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(log_filename, true);
        file_sink->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%n] [%l] %v");
        
        // 组合多个输出目标
        std::vector<spdlog::sink_ptr> sinks = {console_sink, file_sink};
        auto logger = std::make_shared<spdlog::logger>("rocksdb_logger", sinks.begin(), sinks.end());
        
        // 设置日志级别
        logger->set_level(spdlog::level::debug);
        logger->flush_on(spdlog::level::err); // 只在错误级别立即刷新
        
        // 注册为默认日志器
        spdlog::set_default_logger(logger);
        initialized = true;
        
        // 注册退出时自动刷新
        static std::once_flag cleanup_flag;
        std::call_once(cleanup_flag, []() {
            std::atexit([]() {
                if (auto logger = spdlog::default_logger()) {
                    logger->flush();
                }
            });
        });
    }
}

// 强制立即刷新缓冲区
inline void flush_logger() {
    if (auto logger = spdlog::default_logger()) {
        logger->flush();
    }
}

// 包装 spdlog 日志函数，支持 fmt 格式
template<typename... Args>
void log_info(fmt::format_string<Args...> fmt_str, Args&&... args) {
    spdlog::info(fmt_str, std::forward<Args>(args)...);
}

template<typename... Args>
void log_error(fmt::format_string<Args...> fmt_str, Args&&... args) {
    spdlog::error(fmt_str, std::forward<Args>(args)...);
}

template<typename... Args>
void log_debug(fmt::format_string<Args...> fmt_str, Args&&... args) {
    spdlog::debug(fmt_str, std::forward<Args>(args)...);
}

template<typename... Args>
void log_warn(fmt::format_string<Args...> fmt_str, Args&&... args) {
    spdlog::warn(fmt_str, std::forward<Args>(args)...);
}

// 性能关键的日志（带立即刷新）
template<typename... Args>
void log_info_flush(fmt::format_string<Args...> fmt_str, Args&&... args) {
    spdlog::info(fmt_str, std::forward<Args>(args)...);
    flush_logger();
}

template<typename... Args>
void log_error_flush(fmt::format_string<Args...> fmt_str, Args&&... args) {
    spdlog::error(fmt_str, std::forward<Args>(args)...);
    flush_logger();
}

}
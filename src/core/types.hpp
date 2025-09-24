#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <optional>
#include <sstream>
#include <iomanip>

using BlockNum = uint64_t;
using PageNum = uint64_t;
using Key = std::string;
using Value = std::string;

struct ChangeSetRecord {
    BlockNum block_num;
    std::string addr_slot;
    Value value;

    Key to_key() const {
        std::ostringstream oss;
        oss << std::setw(8) << std::setfill('0') << std::hex << block_num << addr_slot;
        return oss.str();
    }
};

struct IndexRecord {
    PageNum page_num;
    std::string addr_slot;
    std::vector<BlockNum> block_history;

    Key to_key() const {
        std::ostringstream oss;
        oss << std::setw(6) << std::setfill('0') << std::hex << page_num << addr_slot;
        return oss.str();
    }
};

inline PageNum block_to_page(BlockNum block_num) {
    return block_num / 1000;
}
[**📊 RocksDB Benchmark 方案：模拟分页索引与 Changeset 存储**](https://www.notion.so/RocksDB-Benchmark-Changeset-2758ec314f758011b03ce880adf655b7?pvs=21) 

# 数据结构设计

模拟以下两张表：
ChangeSet 表：

```jsx
Key: block_num_be || addr#slot (例如: "0000007A0000/0xabc#slot123")
Value: 32-byte new value(尽量是随机的 不要每一位都是0)
```

Index 表（分页，每1万区块一页）：

```jsx
Key: page_be || addr#slot（例如: "000001/0xabc#slot123"）
Value: MergeOperator记录变更过的block number列表（初步使用定长8字节；后续可压缩为VarInt）
```

**数据规模设定**

Hash（seed + Salt）

| **参数** | **说明** |
| --- | --- |
| **初始 Key 总量** | 1 亿个（写入一次性完成，每个 key 的 value 为 32 字节随机） |
| 初始化新key | 总共写入 1 亿条变更记录，每次写1w个 |
| 更新旧key | 模拟每个新区块，变更 1w 个 key |
| **key 构成** | 热点 key: 0.1 亿（80% 概率写入）中等活跃: 0.2 亿（10%）
长尾 key: 0.7 亿（10%） |

相当于模拟每次都写入一个block 一个是1万条kv？

先写入1亿个kv 每次按照1个block 1万kv的方式 这部分是纯写入测试

然后做更新测试 更新时使用hotspot模式的key分布

### **🚀 Benchmark 场景设计**

### **✅ 热点模式（Hotspot Mode）**

- 每区块写入中：
    - 从 0.1 亿热点 key 中随机选取 8000 个
    - 从 0.2 亿中等活跃 key 中选取 1000 个
    - 从 0.7 亿长尾 key 中选取 1000 个
- 每条写入同时更新：
    - ChangeSet 表：append block-specific delta
    - Index 表：通过 merge 操作追加当前 block height
- 每 50 万区块（即约 50w 轮）后：
    - **随机读取一批 key** 的某个历史区块状态（模拟 eth_call）
    - 测试读取是否能 **快速定位分页** 并解析 Index + ChangeSet

### **🧪 测试指标**

| **指标** | **描述** |
| --- | --- |
| **写入吞吐** | 每轮写入 10000 个 key 的时间与吞吐量（MB/s） |
| **SST 合并效率** |  |
| **MergeOperator 聚合大小** |  |
| **Bloom Filter 准确率** |  |
| **历史查询性能** |  |
| **冷热 key 命中分析** |  |
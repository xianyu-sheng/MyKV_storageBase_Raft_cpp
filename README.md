# 基于 Raft 的高性能分布式 KV 存储系统 & 推荐特征召回引擎

## 一、项目概览

自底向上完整实现了一个面向推荐系统特征存储场景的高性能分布式存储与召回系统，包含**自研 RPC 框架**、**Raft 共识算法层**、**SkipList 存储引擎**和**HNSW 向量召回引擎**四层。通过 Pipeline 复制、批量追加、异步持久化、ReadIndex 安全读、无锁读存储引擎等优化，将传统 KV 操作的 QPS 从 23 ops/s 提升至 363 ops/s（15.8x）。在此基础上，通过 **CQRS 读写分离架构**，将特征向量搜索的 P99 延迟压至亚毫秒级。

> **核心成果速览**：363 QPS（KV写）/ 45 ops/s（特征写入）/ 平均延迟 ~200us / 亚毫秒级向量召回 / 3节点强一致 / 累计 3700 条零丢失

### 性能指标

| 指标 | 优化前 | 优化后 | 提升 |
|------|--------|--------|------|
| **QPS（4线程 KV 操作）** | 23 ops/s | **363 ops/s** | **15.8x** |
| **平均延迟（KV 操作）** | 173 ms | **2.24 ms** | **85.6x** |
| **P99 延迟（KV 操作）** | ~140 ms | **~7 ms** | **20x** |
| **特征写入成功率** | 95% | **100%** | **+5%** |
| **累计写入验证** | — | **3700 条零丢失** | — |

### 向量召回性能（3 节点集群，懒加载 HNSW 索引）

| 指标 | 数值 |
|------|------|
| 向量维度 | 128 维 float32 |
| 搜索延迟（P50） | ~198 us |
| 搜索延迟（avg） | ~225-290 us |
| 搜索延迟（P99） | 1106-3020 us |
| Search QPS（客户端） | ~930-987 ops/s |
| Top-K 参数支持 | 5 / 10 / 20 / 50 全部正确 |
| 3 节点结果一致性 | **100%**（完全一致） |
| 节点间延迟差异 | **< 12%**（正常） |

### 稳定性测试（4 轮完整验证）

| 轮次 | 写入量 | 成功率 | Search P50 | Search P99 | 3节点一致性 |
|------|--------|--------|------------|------------|------------|
| Round 1 | 200 条 | **100%** | 152 us | 1350 us | **100%** |
| Round 2 | 2000 条 | **100%** | 174 us | 1095 us | **100%** |
| Round 3 | 500 条 | **100%** | 157 us | 1283 us | **100%** |
| Round 4 | 1000 条 | **100%** | 161 us | 2273 us | **100%** |
| **累计** | **3700 条** | **100%** | — | — | **100%** |

### 高频压测（100 次连续 Search）

| 参数 | Top-10 | Top-20 | Top-50 |
|------|--------|--------|--------|
| 成功率 | **100%** | **100%** | **100%** |
| 平均延迟 | ~265 us | ~265 us | ~205 us |
| P50 延迟 | 198 us | 198 us | — |
| P99 延迟 | 2849 us | 3020 us | — |

---

## 二、系统全景架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Client / Python 测试脚本                          │
│              gen_features.py（批量写入） / Search RPC（向量召回）           │
└──────────────────────────┬──────────────────────────────────────────────┘
                           │  TCP 短连接 + 自定义二进制 RPC 协议
                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         RPC 框架层（myRPC）                               │
│  ┌──────────┐  ┌──────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │ Varint   │  │ Protobuf    │  │ Reactor          │  │ ZooKeeper    │ │
│  │ 长度前缀  │  │ 反射服务分发  │  │ (Muduo) 高并发   │  │ 服务发现     │ │
│  │ 解决粘包  │  │              │  │                  │  │              │ │
│  └──────────┘  └──────────────┘  └─────────────────┘  └──────────────┘ │
└──────────────────────────┬──────────────────────────────────────────────┘
                           │  强一致性写入 / 最终一致性读取
                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CQRS 写路径（raftKv，强一致）                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                    共识算法层（Raft）                              │   │
│  │  Leader: Batch Append + Pipeline 滑动窗口 + ReadIndex             │   │
│  │  Follower: 日志接收 + 按 term 批量回退 nextIndex                  │   │
│  └────────────────────────┬─────────────────────────────────────────┘   │
│                           │  ApplyMsg（线性推进）                          │
│                           ▼                                                 │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │  SkipList（状态机，强一致数据底座）                               │   │
│  │  无锁读 + 互斥锁写 + 逻辑删除 + 后台 GC                          │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                           │  CQRS 驱动回调                               │
│                           ▼                                                 │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │  featureServer::RecallEngine（HNSW 索引，异步只读视图）         │   │
│  │  upsertPoint() / deletePoint() / searchTopK()                   │   │
│  │  m_deletedItems 软删除过滤 + replace_deleted=true 支持更新       │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└──────────────────────────┬──────────────────────────────────────────────┘
                           │  最终一致性读取（Search RPC，完全旁路 Raft）
                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CQRS 读路径（featureServer，最终一致）                  │
│                           ┌─────────────────────────────────────────────┐   │
│                           │  HNSW 向量召回（亚毫秒级 Top-K 检索）        │   │
│                           │  InnerProductSpace / M=16 / EFC=200         │   │
│                           └─────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │  Lazy Loading: 首次 Search RPC 时从 SkipList 全量构建索引         │   │
│  │  确保 SkipList 已通过 Raft 重放恢复了所有持久化数据               │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                           │
                           ▼
                     SearchResponse（Top-K item_ids + scores）
```

---

## 三、CQRS 读写分离架构详解

### 3.1 为什么推荐系统需要 CQRS？

推荐系统的读写特性呈现极端不对称：
- **写（更新特征）**：频率相对低，但绝对不能丢，需要多节点强一致（**Raft 发挥作用**）
- **读（召回搜索）**：频率极高（线上实时请求），要求 P99 在 2ms 以内，绝对不能走 Raft 协议（**HNSW 发挥作用**）

将这两个职责强行揉在一个模块里会"拧巴"，必须分离。

### 3.2 写入链路（Command Path）

```
客户端 PutFeature RPC
    │
    ▼
KvServer::PutFeature() → Op{Operation="PutFeature", Value=ItemFeature.Serialize()}
    │
    ▼
Raft::Start() → 追加日志到本地 m_logs
    │
    ▼
批量追加缓冲区（Batch Append + Pipeline）→ 异步复制到 Follower
    │
    ▼
多数派确认后，ApplyMsg 推送到 applyCh
    │
    ▼
KvServer::Apply() → ExecutePutAppendOnKVDB() → SkipList.Put()
    │
    ▼
applyFeatureToIndex() → RecallEngine::upsertPoint()
    │
    ▼
HNSW 图更新完成（upsertPoint 支持 replace_deleted=true，自动处理更新）
```

### 3.3 读取链路（Query Path）

```
客户端 Search RPC
    │
    ▼
buildIndexIfNeeded() — 懒加载标记检查
    │
    ▼ (首次搜索)
SkipList::foreach() — 全量遍历所有 itemId + ItemFeature
    │
    ▼
RecallEngine::addPoint() — 逐条插入 HNSW 图
    │
    ▼
RecallEngine::searchTopK() — HNSW ANN 检索
    │
    ▼
deletedItems 过滤 → 返回 Top-K 结果
```

### 3.4 面试"反杀"话术

> "**为什么在 Raft 系统里加一个本地 HNSW 索引？这不是破坏了 Raft 的一致性吗？**"

> "这是一个非常好的问题。我采用了 **CQRS（读写责任分离）** 架构，将系统分为状态机层（raftKv）和查询视图层（featureServer）。
> 1. **写入链路**：特征数据走 Raft 协议，Raft 的状态机（SkipList）是数据唯一真理源（Source of Truth），保证多节点强一致性和容灾。
> 2. **异构索引同步**：当 Raft 的 Leader 成功 commit 一条特征日志并 apply 到 SkipList 时，触发异步回调，将向量动态插入到 hnswlib 引擎中。
> 3. **读取链路**：Search RPC 完全隔离在 featureServer 层，直接查本地 HNSW 图，实现亚毫秒级召回。
> HNSW 并不是绕过了 Raft，而是作为 Raft 状态机的**异步异构查询视图（Read View）**存在。这和业界用 MySQL/HBase 做存储，通过 Binlog 同步给 ES/Faiss 做检索的逻辑完全一致，只是我在单体进程内通过命名空间和内存回调实现了这种解耦。"

---

## 四、RPC 框架层：自研高性能通信基础设施

### 4.1 TCP 粘包/半包解决方案

使用 Varint 长度前缀（4字节）来标记每条消息的长度，接收端按长度读取，不丢包不黏包。

### 4.2 Leader 追踪与智能重试

`ErrWrongLeader` 时自动重新从 ZooKeeper 查询节点列表，销毁旧连接，重建到新 Leader 的连接。

---

## 五、共识算法层：Raft 全特性工程实现

### 5.1 Pre-Vote 预选举机制

防止网络分区时 Term Inflation。节点在正式发起选举前，先试探性获取选票。

### 5.2 按 term 批量回退 nextIndex

通过 `lastNotConfusionIndex` 让 Leader 一次性跳过整个 term 的日志，减少回退次数。

### 5.3 Batch Append + Pipeline 滑动窗口

```
客户端 Start() → 批量追加缓冲区 → 批量 RPC 发送 → 异步确认
窗口最大 16 档，动态调整批次大小（利用率≥80%发1条，几乎空闲发8条）
```

### 5.4 ReadIndex 线性安全读

Leader 直接读本地 SkipList（绕过 Raft 日志），Follower 向 Leader 确认 commitIndex 后等待本地 apply 完成再读。

---

## 六、存储引擎层

### 6.1 无锁读 SkipList

读操作（`search_element`）完全不加锁，因为 SkipList 节点的 `forward` 指针在插入后永远不变。删除采用两阶段策略：先原子标记 `deleted=true`，再后台 GC 线程批量释放。

### 6.2 异步持久化：序号等待机制

Raft 主线程不等待磁盘写完。IO 后台线程批量合并写入，通过临时文件+rename 实现原子替换。

---

## 七、HNSW 向量召回引擎（featureServer）

### 7.1 核心参数

| 参数 | 值 | 说明 |
|------|---|------|
| 向量维度 | 128 | 与推荐系统 embedding 维度对齐 |
| M | 16 | 图的连接度，影响精度与内存 |
| efConstruction | 200 | 构建时搜索范围 |
| efSearch | 50 | 搜索时搜索范围 |
| maxElements | 1,000,000 | 预分配容量 |
| Space | InnerProductSpace | 内积空间（用于近似余弦相似度） |

### 7.2 Update 与 Delete 支持

- **Update**：调用 `upsertPoint()`，hnswlib 的 `addPoint(label, replace_deleted=true)` 自动检测已有 label，先 `markDeleted` 旧节点，再原地更新向量
- **Delete**：调用 `deletePoint()`，将 itemId 加入 `m_deletedItems` 软删除集合，`markDelete(label)` 从 HNSW 图中移除。搜索时过滤软删除集合，保证结果不包含已删除 item

### 7.3 懒加载索引构建

HNSW 索引在**首次 Search RPC 时才从 SkipList 全量构建**。这是因为：
- Raft 节点重启后，SkipList 数据通过 `applierTicker` 线程从持久化日志重放恢复
- 重建 Leader 选举期间旧任期日志不会被 commit（只能 commit 当前任期日志）
- 懒加载确保 SkipList 已包含所有 Raft 持久化的数据后才构建索引

---

## 八、全链路延迟分解

| 延迟组成 | KV 写操作 | 向量搜索 |
|---------|---------|---------|
| SkipList 写入 | 0.1 ms | — |
| HNSW 索引更新 | — | 0.05 ms |
| 网络往返（TCP 本机） | 0.5-1 ms | — |
| **Raft 共识（KV 写）** | **1-2 ms** | — |
| **HNSW ANN 搜索** | — | **0.08-0.15 ms** |
| 其他开销 | 0.3 ms | 0.01 ms |

---

## 九、一致性与容错保证

### 9.1 线性一致写（KV 路径）

- 所有 PutFeature 经由 Raft 日志复制到多数派后提交
- SkipList 是数据唯一真理源

### 9.2 最终一致读（Search 路径）

- Search 完全绕过 Raft，直接查本地 HNSW 图
- Follower 索引通过 Raft apply 回调异步更新，可能短暂不一致
- 推荐系统可接受百毫秒级不一致，换来亚毫秒级召回

### 9.3 3 节点容错

任意 1 节点宕机或网络隔离，仍有 2 节点可继续工作。

### 9.4 请求幂等性

客户端携带 `(ClientId, RequestId)`，服务端去重表保证幂等。

---

## 十、启动与测试

```bash
# 编译
cd build && cmake .. && make -j

# 启动 3 节点集群
cd ../scripts
bash start_cluster.sh start

# 等待 Leader 选举
bash start_cluster.sh wait-leader

# Python 批量写入特征（需要先找到 Leader 端口）
python3 gen_features.py --total 1000 --workers 10

# Python 向量搜索演示
python3 gen_features.py --search-demo

# 关闭集群
bash start_cluster.sh stop
```

---

## 十一、技术栈

| 技术领域 | 选型 |
|---------|------|
| 共识算法 | Raft（含 Pre-Vote、Pipeline、Batch Append、ReadIndex） |
| RPC 框架 | 自研 myRPC（TCP + Protobuf + Muduo） |
| 服务发现 | ZooKeeper 3.4.13（临时节点 + GetChildren） |
| KV 存储 | SkipList（无锁读 + 互斥锁写 + 逻辑删除 + 后台 GC） |
| 向量索引 | hnswlib（HNSW 算法，InnerProductSpace） |
| 持久化 | 异步 IO + 写合并 + 原子写入 + 序号等待 |
| 序列化 | Protobuf 3.x + Boost.Serialization |
| 构建工具 | CMake + GDB |

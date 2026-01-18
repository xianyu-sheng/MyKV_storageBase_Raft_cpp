# MyKV_storageBase_Raft_cpp

## 一、项目简介

MyKV_storageBase_Raft_cpp 是一个基于 Raft 共识算法的分布式 KV 存储系统，实现了：

- **多副本容错**：通过 Raft 选举与日志复制提供主从一致性，允许少数节点故障。
- **线性一致的读写**：客户端所有写操作都经由 Leader 串行提交，读操作在已提交日志上执行。
- **自研 RPC 框架（myRPC）**：基于 protobuf 实现的轻量级 RPC 通讯层，支持 ZooKeeper 服务发现。
- **服务发现与多副本路由**：使用 ZooKeeper 维护 KvServer 实例列表，客户端能向多个节点发起请求。
- **持久化与恢复**：利用 Boost.Serialization 等机制持久化 Raft 状态和日志，为后续快照与容错打基础。
- **典型三节点集群**：默认配置为 3 个 Raft 节点（kvserver 进程），组成一个小型分布式存储集群。

该项目适合作为简历上的“分布式存储系统”项目，体现对 Raft、一致性协议、RPC 框架和 ZooKeeper 的理解与工程实践能力。

---

## 二、项目模块

- **myRPC 通讯层**
  - 封装 TCP + protobuf 的 RPC 调用过程。
  - 包含：
    - `KrpcApplication`：RPC 框架入口，读取配置。
    - [KrpcProvider](cci:1://file:///home/xianyu-sheng/MyKV_storageBase_Raft_cpp/myRPC/Server/Krpcprovider.cc:14:0-19:1)：服务端注册与请求分发，将 protobuf Service 暴露为 RPC 服务。
    - [KrpcChannel](cci:2://file:///home/xianyu-sheng/MyKV_storageBase_Raft_cpp/myRPC/User/KrpcChannel.h:8:0-33:1)：客户端通道，负责序列化请求、连接远端、发送与接收。
    - `KrpcController`：记录每次调用的状态（失败原因等）。
  - 支持两种寻址方式：
    - 通过 ZooKeeper 按 `service/method` 查询可用服务地址。
    - 直接使用 `ip:port` 进行点对点连接（主要用于 Raft 节点间通信）。

- **ZooKeeper 集成（服务发现）**
  - [Zkclient](cci:2://file:///home/xianyu-sheng/MyKV_storageBase_Raft_cpp/myRPC/ZooKeeper/ZooKeeper.h:9:0-24:1)：封装 ZooKeeper C 接口。
  - 功能：
    - 在 `/ServiceName/MethodName` 路径下创建**持久节点**描述服务和方法。
    - 在方法节点下为每个服务实例创建一个以端口命名的**临时子节点**（如 `/kvServerRpc/PutAppend/8000`），节点内数据为 `ip:port`。
    - 客户端通过 [GetChildren](cci:1://file:///home/xianyu-sheng/MyKV_storageBase_Raft_cpp/myRPC/ZooKeeper/ZooKeeper.cc:102:0-118:1) 获取所有可用实例，并支持简单的轮询/随机选择，实现多副本路由。

- **Raft 共识模块**
  - 实现 Raft 的主要状态机逻辑，包括：
    - Leader 选举：Follower 超时发起选举，RequestVote RPC。
    - 日志复制：Leader 周期性发送 AppendEntries 心跳与日志条目。
    - 提交与应用：在多数节点复制成功后推进 `commitIndex`，并将日志应用到状态机（KV 存储）。
  - 参数：
    - 心跳间隔：约 25ms。
    - 选举超时：300–500ms 随机。
  - 提供给上层的接口：
    - [Start](cci:1://file:///home/xianyu-sheng/MyKV_storageBase_Raft_cpp/Raft/Raft.cpp:1124:0-1157:1)：客户端（KvServer）提交一个命令，由 Raft 负责复制和提交。

- **KvServer（分布式 KV 服务）**
  - 每个进程同时包含：
    - 一个 Raft 实例（参与共识）。
    - 一个本地 KV 存储（如跳表 Skip List）。
  - 对外暴露的 RPC：
    - [Get](cci:1://file:///home/xianyu-sheng/MyKV_storageBase_Raft_cpp/Clerk/clerk.cpp:45:0-80:1)：读取键值。
    - [PutAppend](cci:1://file:///home/xianyu-sheng/MyKV_storageBase_Raft_cpp/KvServer/KvServer.cpp:142:0-152:20)：写入或追加值（包含 [Put](cci:1://file:///home/xianyu-sheng/MyKV_storageBase_Raft_cpp/Clerk/clerk.cpp:6:0-43:1) / `Append` 两种操作）。
  - 关键逻辑：
    - 将客户端请求封装成命令，通过 [Raft::Start](cci:1://file:///home/xianyu-sheng/MyKV_storageBase_Raft_cpp/Raft/Raft.cpp:1124:0-1157:1) 提交。
    - 等待对应日志条目在本节点被提交（带超时，例如 `CONSENSUS_TIMEOUT=500ms`）。
    - 日志提交后在本地 KV 中应用，并返回结果。
    - 包含去重逻辑，避免客户端重试导致重复写入。

- **Clerk / 客户端组件**
  - 封装客户端对分布式 KV 集群的访问：
    - 对外提供 [Get(key)](cci:1://file:///home/xianyu-sheng/MyKV_storageBase_Raft_cpp/Clerk/clerk.cpp:45:0-80:1)、[Put(key, value)](cci:1://file:///home/xianyu-sheng/MyKV_storageBase_Raft_cpp/Clerk/clerk.cpp:6:0-43:1)、`Append(key, value)` 等接口。
    - 内部通过 myRPC + ZooKeeper 发现可用 KvServer，支持在遇到 `ErrWrongLeader` 或网络错误时重试其他节点。
  - 可以作为测试程序或 SDK 使用。

- **配置与脚本**
  - 多份 myRPC 配置文件（如 [myrpc_0.conf](cci:7://file:///home/xianyu-sheng/MyKV_storageBase_Raft_cpp/myRPC/conf/myrpc_0.conf:0:0-0:0), [myrpc_1.conf](cci:7://file:///home/xianyu-sheng/MyKV_storageBase_Raft_cpp/myRPC/conf/myrpc_1.conf:0:0-0:0), [myrpc_2.conf](cci:7://file:///home/xianyu-sheng/MyKV_storageBase_Raft_cpp/myRPC/conf/myrpc_2.conf:0:0-0:0), [myrpc.conf](cci:7://file:///home/xianyu-sheng/MyKV_storageBase_Raft_cpp/myRPC/conf/myrpc.conf:0:0-0:0)），分别对应不同节点和客户端。
  - 指定：
    - 本地 RPC 监听地址（`rpcserverip`, `rpcserverport`）。
    - ZooKeeper 地址（`zookeeperip`, `zookeeperport`）。

---

## 三、框架图（预留）

> **提示：此处留给你后续补充架构图**。可以在这里加上一句话，例如：
>
> - TODO：在此插入 MyKV_storageBase_Raft_cpp 的总体架构图。

### 建议的框架图内容（你画图时可以参考）

- **整体结构层次**
  - 上层：多个客户端（Clerk / kvclient），通过 myRPC 调用 [KvServer](cci:2://file:///home/xianyu-sheng/MyKV_storageBase_Raft_cpp/KvServer/kvServer.h:29:0-70:1) 的 `Get/PutAppend` 接口。
  - 中间层：若干 [KvServer](cci:2://file:///home/xianyu-sheng/MyKV_storageBase_Raft_cpp/KvServer/kvServer.h:29:0-70:1) 实例，每个包含：
    - Raft 模块（Leader/Follower 状态机）。
    - 本地 KV 存储。
  - 底层：ZooKeeper 集群，用于：
    - 记录 [KvServer](cci:2://file:///home/xianyu-sheng/MyKV_storageBase_Raft_cpp/KvServer/kvServer.h:29:0-70:1) 的服务节点。
    - 为 RpcChannel 提供服务发现能力。

- **Raft 内部结构**
  - 三个 Raft 节点之间构成一个完全互联的 RPC 网络（RequestVote / AppendEntries / InstallSnapshot）。
  - Leader 对外服务客户端写请求，Follower 仅接收复制。

- **数据流示意**
  - 客户端发起 [Put](cci:1://file:///home/xianyu-sheng/MyKV_storageBase_Raft_cpp/Clerk/clerk.cpp:6:0-43:1)：
    1. 通过 myRPC + ZooKeeper 连接到某个 KvServer。
    2. KvServer 将命令提交给本地 Raft。
    3. Raft Leader 将日志复制到其他节点。
    4. 达到多数派后提交并应用到本地 KV。
    5. KvServer 返回成功给客户端。
  - 你可以用箭头标出以上 5 步数据流。

---

## 四、测试与运行命令（示例）

下面的命令是**示例**，你可以根据自己项目实际的可执行文件名 / 构建方式做适当修改。

### 1. 环境准备

- 已安装：
  - C++17 编译器（如 g++ / clang）。
  - CMake。
  - protobuf 及其 C++ 库。
  - ZooKeeper 及 C 语言客户端库。
- 已启动 ZooKeeper（假设为本机默认端口 `127.0.0.1:2181`）：
  - 例如：
    ```bash
    # 示例命令，按你本机 ZooKeeper 安装路径调整
    zkServer.sh start
    ```

### 2. 编译项目

```bash
mkdir -p build
cd build
cmake ..
make -j

### 3.测试
终端 1：节点 0
cd /home/your_user/MyKV_storageBase_Raft_cpp/build
RAFT_ME=0 ./kvserver

终端 2：节点 1
cd /home/your_user/MyKV_storageBase_Raft_cpp/build
RAFT_ME=1 ./kvserver

终端 3：节点 2
cd /home/your_user/MyKV_storageBase_Raft_cpp/build
RAFT_ME=2 ./kvserver

终端 4：客户端
cd /home/your_user/MyKV_storageBase_Raft_cpp/build
RAFT_ME=3 ./kvclient

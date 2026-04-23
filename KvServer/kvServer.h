#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <map>
#include <memory>
#include <mutex>
#include <functional>
#include "../Raft/raft.h"
#include "../Skiplist-CPP/skiplist.h"
#include "../Proto/raftKVRpcProtoc/KvServerRPC.pb.h"
#include "../Proto/raftRpcProtoc/raftRPC.pb.h"
#include "../Raft/ApplyMsg.h"
#include <google/protobuf/service.h>

// 日志命令结构（扩展支持 PutFeature 操作）
struct Op{
    std::string Operation;  // "Get" | "Put" | "Append" | "PutFeature" | "DeleteFeature"
    std::string Key;       // 对 PutFeature/DeleteFeature：item_id
    std::string Value;      // 对 PutFeature：ItemFeature.SerializeAsString()
    std::string ClientId;
    int RequestId;
};

static const std::string OK ="OK";
static const std::string ErrNoKey="ErrNoKey";
static const std::string ErrWrongLeader="ErrWrongLeader";

// CQRS 架构：读写分离
namespace featureServer { class RecallEngine; }

class KvServer:public raftKVRpcProtoc::kvServerRpc{
    public:
    KvServer(std::shared_ptr<Raft> raftNode);

    // 原有 RPC（proto 生成）
    void Get(::google::protobuf::RpcController* controller,
             const raftKVRpcProtoc::GetArgs* request,
             raftKVRpcProtoc::GetReply* response,
             ::google::protobuf::Closure* done) override;
    void PutAppend(::google::protobuf::RpcController* controller,
                   const raftKVRpcProtoc::PutAppendArgs* request,
                   raftKVRpcProtoc::PutAppendReply* response,
                   ::google::protobuf::Closure* done) override;
    // 新增 RPC（proto 生成）
    void PutFeature(::google::protobuf::RpcController* controller,
                    const raftKVRpcProtoc::PutFeatureArgs* request,
                    raftKVRpcProtoc::PutFeatureReply* response,
                    ::google::protobuf::Closure* done) override;
    void Search(::google::protobuf::RpcController* controller,
                 const raftKVRpcProtoc::SearchRequest* request,
                 raftKVRpcProtoc::SearchResponse* response,
                 ::google::protobuf::Closure* done) override;

    // 业务逻辑版本（不带 controller/done）
    void Get(const raftKVRpcProtoc::GetArgs* args, raftKVRpcProtoc::GetReply* reply);
    void PutAppend(const raftKVRpcProtoc::PutAppendArgs* args, raftKVRpcProtoc::PutAppendReply* reply);
    void PutFeature(const raftKVRpcProtoc::PutFeatureArgs* args, raftKVRpcProtoc::PutFeatureReply* reply);
    void Search(const raftKVRpcProtoc::SearchRequest* args, raftKVRpcProtoc::SearchResponse* reply);

    // Raft 状态机_apply_入口
    void Apply(const ApplyMsg& msg);

    // ReadIndex
    void SetLeaderInfo(int leaderId, const std::string& leaderIp, int leaderPort);
    int GetCommitIndex() { return m_raftNode->GetCommitIndex(); }
    void setRaftNode(std::shared_ptr<Raft> raftNode) { m_raftNode = raftNode; }

    // 启动时全量构建 HNSW 索引（懒加载：首次 Search RPC 时自动构建）
    void buildRecallIndex();

    // 懒加载：Search RPC 首次调用时构建索引（确保 SkipList 已恢复所有数据）
    void buildIndexIfNeeded();

    private:
    void ExecuteGetOpOnKVDB(const Op& op, std::string* value, bool* exist);
    void ExecutePutAppendOnKVDB(const Op& op);
    bool ifRequestDuplicate(const std::string& clientId, int requestId);
    void recordRequestResult(const Op& op, const std::string& lastValue);
    bool WaitForCommitIndex(int targetIndex, int timeoutMs);
    bool QueryLeaderForReadIndex(int* commitIndex);

    // 写入 KV 后，驱动 RecallEngine 增量索引（CQRS 写路径）
    void applyFeatureToIndex(const raftKVRpcProtoc::ItemFeature& feat);

    // 软删除：从 SkipList 逻辑删除 + 从 RecallEngine 索引中移除
    void deleteFeature(const std::string& itemId);

    private:
    std::shared_ptr<Raft> m_raftNode;
    SkipList<std::string, std::string> m_kvdb;

    // ========== CQRS: 异步只读查询视图 ==========
    std::unique_ptr<featureServer::RecallEngine> m_recallEngine;
    bool m_indexBuilt = false;  // 懒加载标记
    // ========== CQRS End ==========

    std::map<int, std::shared_ptr<LockQueue<Op>>> waitApplyCh;
    std::mutex m_mtx;

    struct RequestRecord {
        int lastRequestId;
        std::string lastValue;
    };
    std::unordered_map<std::string, RequestRecord> m_lastRequests;
    std::mutex m_reqMtx;  // 保护 m_lastRequests 的并发读写

    std::map<int, std::shared_ptr<LockQueue<ApplyMsg>>> m_commitIndexCh;
    std::mutex m_commitMtx;
    std::string m_leaderIp;
    int m_leaderPort;
    int m_leaderId;
    std::mutex m_leaderMtx;
};

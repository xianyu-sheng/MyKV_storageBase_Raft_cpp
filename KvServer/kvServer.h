#pragma once
#include <string>
#include <unordered_map>
#include <map>
#include <memory>
#include <mutex>
#include "../Raft/raft.h"//RAft类
#include "../Skiplist-CPP/skiplist.h"//SkipList类
#include "../Proto/raftKVRpcProtoc/KvServerRPC.pb.h"
#include "../Proto/raftRpcProtoc/raftRPC.pb.h"
#include "../Raft/ApplyMsg.h"
#include <google/protobuf/service.h>

//日志命令结构
struct Op{
    std::string Operation;//"Get"或"Put"
    std::string Key;
    std::string Value;
    std::string  ClientId;
    int RequestId;
};


//一般错误码可以直接用字符串常量
static const std::string OK ="OK";
static const std::string ErrNoKey="ErrNoKey";
static const std::string ErrWrongLeader="ErrWrongLeader";

//Kvserver负责实现kvserverRPC定义RPC接口
class KvServer:public raftKVRpcProtoc::kvServerRpc{
    public:
    //构造函数：传入raft节点指针等
    KvServer(std::shared_ptr<Raft> raftNode);
    //Rpc接口(proto里定义的两个RPC接口)
    void Get(::google::protobuf::RpcController* controller,
             const raftKVRpcProtoc::GetArgs* request,
             raftKVRpcProtoc::GetReply* response,
             ::google::protobuf::Closure* done) override;
    void PutAppend(::google::protobuf::RpcController* controller,
                   const raftKVRpcProtoc::PutAppendArgs* request,
                   raftKVRpcProtoc::PutAppendReply* response,
                   ::google::protobuf::Closure* done) override;
    // 供上面包装函数调用的实际业务逻辑版本（不带 controller/done）
    void Get(const raftKVRpcProtoc::GetArgs* args,
             raftKVRpcProtoc::GetReply* reply);
    void PutAppend(const raftKVRpcProtoc::PutAppendArgs* args,
                   raftKVRpcProtoc::PutAppendReply* reply);
    //提供给Raft的入口，Raft在apply日志时，调用这个接口吧ApplyMsg推给KvServer
    void Apply(const ApplyMsg& msg);

    private:
    //内部的辅助函数
    void ExecuteGetOpOnKVDB(const Op& op,std::string* value,bool* exist);
    void ExecutePutAppendOnKVDB(const Op& op);
    bool ifRequestDuplicate(const std::string& clientId,int requestId);
    void recordRequestResult(const Op& op,const std::string& lastValue);
    private:
    // === 和 Raft 通信 ===
    std::shared_ptr<Raft> m_raftNode;
    // === 真正的 KV 数据库：用跳表存储 key/value ===
    SkipList<std::string, std::string> m_kvdb;
    // === 用于 RPC 线程等待 Raft 提交日志 ===
    std::map<int, std::shared_ptr<LockQueue<Op>>> waitApplyCh;  // key: raft log index
    std::mutex m_mtx;
    // === 去重表：保证幂等性（按照 clientId + requestId 去重） ===
    struct RequestRecord {
        int         lastRequestId;
        std::string lastValue;  // 对 Get/Put/Append 的上次返回值
    };
    std::unordered_map<std::string, RequestRecord> m_lastRequests; // key: ClientId
};
//在保证线性一致性之前我们如何读KV
// 核心函数：处理Get操作（保证线性一致性）
//函数理解链接：https://www.doubao.com/thread/wa43737e2bbd8fc16

#include "op_coder.h"
namespace{
    constexpr int debugMul=1;
    constexpr int CONSENSUS_TIMEOUT=500*debugMul;
}
KvServer::KvServer(std::shared_ptr<Raft> raftNode)
    : m_raftNode(std::move(raftNode)),
      m_kvdb(/* max_level = */ 12)  // 跳表最大层数自己定，一个常见值是 12 或 16
{
}

void KvServer::ExecuteGetOpOnKVDB(const Op& op, std::string* Value, bool* exist) {
    std::string out;
    bool found = m_kvdb.search_element(op.Key, &out);  // 调用你新加的接口

    if (found) {
        *exist = true;
        *Value = out;
    } else {
        *exist = false;
        Value->clear();
    }
}
void KvServer::ExecutePutAppendOnKVDB(const Op& op) {
    if (op.Operation == "Put") {
        // Put 语义：直接覆盖
        m_kvdb.insert_element(op.Key, op.Value);
    } else if (op.Operation == "Append") {
        // Append 语义：取出旧值 + 拼接
        std::string oldVal;
        bool exist = m_kvdb.search_element(op.Key, &oldVal);

        std::string newVal = exist ? (oldVal + op.Value) : op.Value;
        m_kvdb.insert_element(op.Key, newVal);
    }
}
bool KvServer::ifRequestDuplicate(const std::string& ClientId, int RequestId) {
    auto it = m_lastRequests.find(ClientId);
    if (it == m_lastRequests.end()) {
        return false;//表示是新客户端
    }
    return RequestId <= it->second.lastRequestId;
}

//将完成的请求信息记录起来，以供后面的重复性查询
void KvServer::recordRequestResult(const Op& op, const std::string& lastValue) {
    auto& rec = m_lastRequests[op.ClientId];
    rec.lastRequestId = op.RequestId;
    rec.lastValue     = lastValue;  // 对 Put/Append 可以根据需要留空或记录新值
}
void KvServer::Get(::google::protobuf::RpcController* controller,
                    const raftKVRpcProtoc::GetArgs* request,
                    raftKVRpcProtoc::GetReply* response,
                    ::google::protobuf::Closure* done){
                        (void)controller;//如果暂时不用 可以忽略此参数
                        // 调用不带 controller/done 的业务逻辑版本
                        KvServer::Get(request,response);
                        if(done)    done->Run();//按Protobuf规范，最后吊用回调
                    }
void KvServer::Get(const raftKVRpcProtoc::GetArgs* args,raftKVRpcProtoc::GetReply* reply){
    Op op;
    op.Operation="Get";
    op.Key=args->key();
    op.Value="";
    op.ClientId=args->clientid();
    op.RequestId=args->requestid();

    int raftindex=-1;
    bool isLeader=false;
    m_raftNode->Start(op,&raftindex,&isLeader);//raftindex，raft预计的logIndex,虽然是预计，但是正确情况下，是准确的，op的具体内容对raft来说，是隔离的

    if(!isLeader){
        reply->set_err(ErrWrongLeader);
        return;
    }

    //create waitForch
    m_mtx.lock();
    if(waitApplyCh.find(raftindex)==waitApplyCh.end()){
        waitApplyCh.insert(std::make_pair(raftindex,new LockQueue<Op>()));
    }

    auto chForRaftIndex = waitApplyCh[raftindex];

    m_mtx.unlock();

    //超时了怎么做
    Op raftCommitOp;

     // 尝试从Raft提交通道中带超时获取已提交的日志
        if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {
            // 分支1：Raft共识超时（未在超时时间内拿到提交确认）
            int raftState = -1;  // 占位，Raft状态（follower/candidate/leader）
            bool isLeader = false;
            // 获取当前节点的Raft状态：判断是否是Leader
            m_raftNode->GetState(&raftState, &isLeader);

            // 关键判断：请求是重复的 + 当前节点是Leader
            if (ifRequestDuplicate(op.ClientId, op.RequestId) && isLeader) {
                // 逻辑：超时不代表日志没提交，只是集群没及时响应
                // 但如果是重复的Get请求（已处理过），可安全重读（不违反线性一致性）
                std::string Value;
                bool exist = false;
                // 直接在本地KV执行Get
                ExecuteGetOpOnKVDB(op, &Value, &exist);
                if (exist) {
                    reply->set_err(OK);
                    reply->set_value(Value);
                } else {
                    reply->set_err(ErrNoKey);
                    reply->set_value("");
                }
            } else {
                // 非重复请求/非Leader：返回WrongLeader，让客户端换节点重试
                reply->set_err(ErrWrongLeader);
            }
        } else {
            // 分支2：Raft日志已成功提交（超时前拿到了提交确认）
            // 验证：确保提交的日志就是当前请求的日志（防串请求）
            if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId) {
                std::string Value;
                bool exist = false;
                // 执行Get操作（此时日志已提交，线性一致性有保障）
                ExecuteGetOpOnKVDB(op, &Value, &exist);
                if (exist) {
                    reply->set_err(OK);
                    reply->set_value(Value);
                } else {
                    reply->set_err(ErrNoKey);
                    reply->set_value("");
                }
            } else {
                // 异常情况：提交的日志和当前请求不匹配（理论上不会发生）
                reply->set_err(ErrWrongLeader);
            }
        }
}

void KvServer::PutAppend(::google::protobuf::RpcController* controller,
                   const raftKVRpcProtoc::PutAppendArgs* request,
                   raftKVRpcProtoc::PutAppendReply* response,
                   ::google::protobuf::Closure* done){
                    (void)controller;
                    // 调用不带 controller/done 的业务逻辑版本
                    KvServer::PutAppend(request,response);
                    if(done){
                        done->Run();
                    }    
                   }
void KvServer::PutAppend(const raftKVRpcProtoc::PutAppendArgs* args,
                         raftKVRpcProtoc::PutAppendReply* reply) {
    Op op;
    op.Operation = args->op();        // "Put" or "Append"
    op.Key       = args->key();
    op.Value     = args->value();
    op.ClientId  = args->clientid();
    op.RequestId = args->requestid();

    int index = -1;
    bool isLeader = false;
    m_raftNode->Start(op, &index, &isLeader);

    if (!isLeader) {
        reply->set_err(ErrWrongLeader);
        return;
    }

    // 和 Get 一样：用 waitApplyCh[index] 等待 Raft 提交结果
    std::shared_ptr<LockQueue<Op>> ch;
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        if (waitApplyCh.count(index) == 0) {
            waitApplyCh[index] = std::make_shared<LockQueue<Op>>();
        }
        ch = waitApplyCh[index];
    }

    Op raftCommitOp;
    if (!ch->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {
        // 超时：检查是否是重复请求
        if (ifRequestDuplicate(op.ClientId, op.RequestId)) {
            reply->set_err(OK);
        } else {
            reply->set_err(ErrWrongLeader);
        }
    } else {
        // 拿到提交的 Op，验证是不是本次请求
        if (raftCommitOp.ClientId == op.ClientId &&
            raftCommitOp.RequestId == op.RequestId) {
            reply->set_err(OK);
        } else {
            reply->set_err(ErrWrongLeader);
        }
    }
}

void KvServer::Apply(const ApplyMsg& msg) {
    Op op = decodeOp(msg.command);   // 按你序列化 Op 的方式反序列化
    int index = msg.index;

    // 去重 + 写入跳表
    if (!ifRequestDuplicate(op.ClientId, op.RequestId)) {
        if (op.Operation == "Get") {
            // Get 日志一般不改数据，你可以选择不做任何 KV 更新
            std::string Value;
            bool exist;
            ExecuteGetOpOnKVDB(op, &Value, &exist);
            recordRequestResult(op, exist ? Value : "");
        } else {
            ExecutePutAppendOnKVDB(op);
            recordRequestResult(op, /* 可选：记录最新 Value */ "");
        }
    }

    // 唤醒等待该 index 的 RPC
    std::shared_ptr<LockQueue<Op>> ch;
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        auto it = waitApplyCh.find(index);
        if (it != waitApplyCh.end()) {
            ch = it->second;
        }
    }
    if (ch) {
        ch->push(op);
    }
}
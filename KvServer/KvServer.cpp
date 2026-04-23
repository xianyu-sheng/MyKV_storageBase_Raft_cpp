// KvServer 实现：CQRS 架构
// 写路径（raftKv）走 Raft 共识 + SkipList + RecallEngine 驱动
// 读路径（Search）走本地 HNSW 索引，完全绕过 Raft

#include "op_coder.h"
#include "kvServer.h"
#include "RecallEngine.h"
#include <iostream>

namespace{
    constexpr int debugMul = 1;
    // Raft 共识超时：本地 TCP 1-2 个 RTT 约 300-500ms
    // 但高并发写入时需要容忍一些排队延迟，设 1500ms
    constexpr int CONSENSUS_TIMEOUT = 1500 * debugMul;
}

KvServer::KvServer(std::shared_ptr<Raft> raftNode)
    : m_raftNode(std::move(raftNode)),
      m_kvdb(/* max_level = */ 12)
{
    // 初始化 RecallEngine（CQRS 读视图）
    // 从 SkipList 当前数据量估算最大容量，预留一定余量
    int currentSize = m_kvdb.element_count();
    size_t initCapacity = static_cast<size_t>(std::max(currentSize * 2, 10000));
    m_recallEngine.reset(new featureServer::RecallEngine(initCapacity));
    std::cout << "[KvServer] RecallEngine initialized, capacity=" << initCapacity << std::endl;
}

void KvServer::buildIndexIfNeeded() {
    if (m_indexBuilt) return;
    std::lock_guard<std::mutex> lk(m_mtx);
    if (m_indexBuilt) return;
    std::cout << "[KvServer] Lazy building HNSW index from SkipList ("
              << m_kvdb.element_count() << " items)..." << std::endl;
    int count = 0;
    m_kvdb.foreach([this, &count](const std::string& itemId,
                                   const std::string& value) {
        raftKVRpcProtoc::ItemFeature feat;
        if (feat.ParseFromString(value)) {
            if (feat.embedding_size() == featureServer::HNSW_DIM) {
                std::vector<float> emb;
                emb.reserve(featureServer::HNSW_DIM);
                for (int i = 0; i < featureServer::HNSW_DIM; ++i) {
                    emb.push_back(feat.embedding(i));
                }
                m_recallEngine->addPoint(itemId, emb);
                ++count;
            }
        }
    });
    std::cout << "[KvServer] HNSW index built, " << count << " vectors loaded" << std::endl;
    m_indexBuilt = true;
}

// buildRecallIndex: 空实现（索引通过懒加载构建）
void KvServer::buildRecallIndex() {
    // HNSW 索引在首次 Search RPC 时通过 buildIndexIfNeeded() 懒加载构建
    // 这确保 SkipList 已经包含了所有持久化的数据
}

void KvServer::applyFeatureToIndex(const raftKVRpcProtoc::ItemFeature& feat) {
    if (!m_recallEngine || feat.item_id().empty()) return;
    if (feat.embedding_size() != featureServer::HNSW_DIM) {
        std::cerr << "[KvServer] Skip invalid embedding for item_id="
                  << feat.item_id() << ", dim=" << feat.embedding_size() << std::endl;
        return;
    }
    std::vector<float> emb;
    emb.reserve(featureServer::HNSW_DIM);
    for (int i = 0; i < featureServer::HNSW_DIM; ++i) {
        emb.push_back(feat.embedding(i));
    }
    // upsertPoint 自动处理插入和更新（replace_deleted=true）
    m_recallEngine->upsertPoint(feat.item_id(), emb);
    std::cout << "[KvServer] Recall index updated: item_id=" << feat.item_id()
              << ", total=" << m_recallEngine->size()
              << ", deleted=" << m_recallEngine->deletedCount() << std::endl;
}

void KvServer::deleteFeature(const std::string& itemId) {
    if (!m_recallEngine || itemId.empty()) return;
    m_recallEngine->deletePoint(itemId);
    std::cout << "[KvServer] Recall index deleted: item_id=" << itemId
              << ", total=" << m_recallEngine->size()
              << ", deleted=" << m_recallEngine->deletedCount() << std::endl;
}

void KvServer::ExecuteGetOpOnKVDB(const Op& op, std::string* value, bool* exist) {
    std::string out;
    bool found = m_kvdb.search_element(op.Key, &out);
    if (found) {
        *exist = true;
        *value = out;
    } else {
        *exist = false;
        value->clear();
    }
}

void KvServer::ExecutePutAppendOnKVDB(const Op& op) {
    if (op.Operation == "Put") {
        m_kvdb.insert_element(op.Key, op.Value);
    } else if (op.Operation == "Append") {
        std::string oldVal;
        bool exist = m_kvdb.search_element(op.Key, &oldVal);
        std::string newVal = exist ? (oldVal + op.Value) : op.Value;
        m_kvdb.insert_element(op.Key, newVal);
    }
}

bool KvServer::ifRequestDuplicate(const std::string& ClientId, int RequestId) {
    std::lock_guard<std::mutex> lk(m_reqMtx);
    auto it = m_lastRequests.find(ClientId);
    if (it == m_lastRequests.end()) return false;
    return RequestId <= it->second.lastRequestId;
}

void KvServer::recordRequestResult(const Op& op, const std::string& lastValue) {
    std::lock_guard<std::mutex> lk(m_reqMtx);
    auto& rec = m_lastRequests[op.ClientId];
    rec.lastRequestId = op.RequestId;
    rec.lastValue = lastValue;
}

// ========== RPC 入口包装（proto 规范要求） ==========

void KvServer::Get(::google::protobuf::RpcController*,
                   const raftKVRpcProtoc::GetArgs* request,
                   raftKVRpcProtoc::GetReply* response,
                   ::google::protobuf::Closure* done) {
    KvServer::Get(request, response);
    if (done) done->Run();
}

void KvServer::PutAppend(::google::protobuf::RpcController*,
                         const raftKVRpcProtoc::PutAppendArgs* request,
                         raftKVRpcProtoc::PutAppendReply* response,
                         ::google::protobuf::Closure* done) {
    KvServer::PutAppend(request, response);
    if (done) done->Run();
}

void KvServer::PutFeature(::google::protobuf::RpcController*,
                          const raftKVRpcProtoc::PutFeatureArgs* request,
                          raftKVRpcProtoc::PutFeatureReply* response,
                          ::google::protobuf::Closure* done) {
    KvServer::PutFeature(request, response);
    if (done) done->Run();
}

void KvServer::Search(::google::protobuf::RpcController*,
                      const raftKVRpcProtoc::SearchRequest* request,
                      raftKVRpcProtoc::SearchResponse* response,
                      ::google::protobuf::Closure* done) {
    KvServer::Search(request, response);
    if (done) done->Run();
}

// ========== 业务逻辑实现 ==========

void KvServer::Get(const raftKVRpcProtoc::GetArgs* args,
                   raftKVRpcProtoc::GetReply* reply) {
    Op op;
    op.Operation = "Get";
    op.Key = args->key();
    op.Value = "";
    op.ClientId = args->clientid();
    op.RequestId = args->requestid();

    int raftState = -1;
    bool isLeader = false;
    m_raftNode->GetState(&raftState, &isLeader);

    if (!isLeader) {
        if (ifRequestDuplicate(op.ClientId, op.RequestId)) {
            std::string cachedValue;
            {
                std::lock_guard<std::mutex> lk(m_reqMtx);
                auto it = m_lastRequests.find(op.ClientId);
                if (it != m_lastRequests.end()) cachedValue = it->second.lastValue;
            }
            reply->set_err(OK);
            reply->set_value(cachedValue);
            return;
        }
        int currentCommitIndex = m_raftNode->GetCommitIndex();
        if (currentCommitIndex > 0) {
            std::string value;
            bool exist = false;
            ExecuteGetOpOnKVDB(op, &value, &exist);
            if (exist) {
                reply->set_err(OK);
                reply->set_value(value);
                recordRequestResult(op, value);
                return;
            } else {
                reply->set_err(ErrNoKey);
                reply->set_value("");
                recordRequestResult(op, "");
                return;
            }
        }
        reply->set_err(ErrWrongLeader);
        return;
    }

    if (ifRequestDuplicate(op.ClientId, op.RequestId)) {
        std::string cachedValue;
        {
            std::lock_guard<std::mutex> lk(m_reqMtx);
            auto it = m_lastRequests.find(op.ClientId);
            if (it != m_lastRequests.end()) cachedValue = it->second.lastValue;
        }
        reply->set_err(OK);
        reply->set_value(cachedValue);
        return;
    }

    std::string value;
    bool exist = false;
    ExecuteGetOpOnKVDB(op, &value, &exist);
    if (exist) {
        reply->set_err(OK);
        reply->set_value(value);
        recordRequestResult(op, value);
    } else {
        reply->set_err(ErrNoKey);
        reply->set_value("");
        recordRequestResult(op, "");
    }
}

void KvServer::PutAppend(const raftKVRpcProtoc::PutAppendArgs* args,
                         raftKVRpcProtoc::PutAppendReply* reply) {
    Op op;
    op.Operation = args->op();
    op.Key = args->key();
    op.Value = args->value();
    op.ClientId = args->clientid();
    op.RequestId = args->requestid();

    int index = -1;
    bool isLeader = false;
    m_raftNode->Start(op, &index, &isLeader);

    if (!isLeader) {
        reply->set_err(ErrWrongLeader);
        return;
    }

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
        bool dup = ifRequestDuplicate(op.ClientId, op.RequestId);
        reply->set_err(dup ? OK : ErrWrongLeader);
    } else {
        if (raftCommitOp.ClientId == op.ClientId &&
            raftCommitOp.RequestId == op.RequestId) {
            reply->set_err(OK);
        } else {
            reply->set_err(ErrWrongLeader);
        }
    }
}

void KvServer::PutFeature(const raftKVRpcProtoc::PutFeatureArgs* args,
                          raftKVRpcProtoc::PutFeatureReply* reply) {
    const raftKVRpcProtoc::ItemFeature& feat = args->feature();
    Op op;
    op.Operation = "PutFeature";
    op.Key = feat.item_id();
    op.Value = feat.SerializeAsString();
    op.ClientId = args->clientid();
    op.RequestId = args->requestid();

    int index = -1;
    bool isLeader = false;
    m_raftNode->Start(op, &index, &isLeader);

    if (!isLeader) {
        reply->set_err(ErrWrongLeader);
        return;
    }

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
        bool dup = ifRequestDuplicate(op.ClientId, op.RequestId);
        std::cout << "[PutFeature] timeout, dup=" << dup << std::endl;
        reply->set_err(dup ? OK : ErrWrongLeader);
    } else {
        std::cout << "[PutFeature] committed, item_id=" << feat.item_id() << std::endl;
        if (raftCommitOp.ClientId == op.ClientId &&
            raftCommitOp.RequestId == op.RequestId) {
            reply->set_err(OK);
        } else {
            reply->set_err(ErrWrongLeader);
        }
    }
}

void KvServer::Search(const raftKVRpcProtoc::SearchRequest* args,
                      raftKVRpcProtoc::SearchResponse* reply) {
    // 懒加载：首次 Search 时构建 HNSW 索引
    // 此时 SkipList 已通过 Raft 重放恢复了所有持久化数据
    buildIndexIfNeeded();

    if (!m_recallEngine || m_recallEngine->size() == 0) {
        reply->set_search_time_us(0);
        return;
    }

    const auto& queryVec = args->query_vector();
    int topK = args->top_k();
    if (topK <= 0) topK = 10;
    if (topK > 100) topK = 100;

    std::vector<float> query(queryVec.begin(), queryVec.end());
    int64_t searchTimeUs = 0;

    auto result = m_recallEngine->searchTopK(query, topK, &searchTimeUs);

    for (const auto& itemId : result.first) {
        reply->add_item_ids(itemId);
    }
    for (const auto& score : result.second) {
        reply->add_scores(score);
    }
    reply->set_search_time_us(searchTimeUs);

    std::cout << "[Search] topK=" << topK
              << ", returned=" << result.first.size()
              << ", time_us=" << searchTimeUs << std::endl;
}

// ========== Raft Apply 回调（CQRS 写路径核心） ==========

void KvServer::Apply(const ApplyMsg& msg) {
    Op op = decodeOp(msg.command);
    int index = msg.index;

    // 去重
    if (!ifRequestDuplicate(op.ClientId, op.RequestId)) {
        if (op.Operation == "Get") {
            std::string value;
            bool exist;
            ExecuteGetOpOnKVDB(op, &value, &exist);
            recordRequestResult(op, exist ? value : "");
        } else if (op.Operation == "PutFeature") {
            // 写入 SkipList
            ExecutePutAppendOnKVDB(op);
            recordRequestResult(op, "");

            // CQRS 核心：驱动 RecallEngine 异步构建索引视图
            raftKVRpcProtoc::ItemFeature feat;
            if (feat.ParseFromString(op.Value)) {
                applyFeatureToIndex(feat);
            }
        } else if (op.Operation == "DeleteFeature") {
            // 从 SkipList 逻辑删除
            ExecutePutAppendOnKVDB(op);
            recordRequestResult(op, "");

            // CQRS：从 RecallEngine 中软删除（不影响 HNSW 图结构）
            deleteFeature(op.Key);
        } else {
            ExecutePutAppendOnKVDB(op);
            recordRequestResult(op, "");
        }
    } else {
        std::cout << "[Apply] duplicate op, client=" << op.ClientId
                  << " req=" << op.RequestId << std::endl;
    }

    // 唤醒等待该 index 的 RPC
    std::shared_ptr<LockQueue<Op>> ch;
    {
        std::lock_guard<std::mutex> lk(m_mtx);
        auto it = waitApplyCh.find(index);
        if (it != waitApplyCh.end()) ch = it->second;
    }
    if (ch) ch->push(op);

    {
        std::lock_guard<std::mutex> lk(m_commitMtx);
        auto it = m_commitIndexCh.find(index);
        if (it != m_commitIndexCh.end()) it->second->push(msg);
    }
}

// ========== ReadIndex ==========

void KvServer::SetLeaderInfo(int leaderId, const std::string& leaderIp, int leaderPort) {
    std::lock_guard<std::mutex> lk(m_leaderMtx);
    m_leaderId = leaderId;
    m_leaderIp = leaderIp;
    m_leaderPort = leaderPort;
}

bool KvServer::WaitForCommitIndex(int targetIndex, int timeoutMs) {
    std::shared_ptr<LockQueue<ApplyMsg>> ch;
    {
        std::lock_guard<std::mutex> lk(m_commitMtx);
        if (m_commitIndexCh.count(targetIndex) == 0) {
            m_commitIndexCh[targetIndex] = std::make_shared<LockQueue<ApplyMsg>>();
        }
        ch = m_commitIndexCh[targetIndex];
    }
    ApplyMsg msg;
    bool success = ch->timeOutPop(timeoutMs, &msg);
    {
        std::lock_guard<std::mutex> lk(m_commitMtx);
        m_commitIndexCh.erase(targetIndex);
    }
    return success;
}

bool KvServer::QueryLeaderForReadIndex(int* commitIndex) {
    int leaderId = -1;
    m_raftNode->GetLeaderInfo(&leaderId, nullptr, nullptr);
    if (leaderId < 0) return false;

    auto request = std::make_shared<raftRpcProtoc::ReadIndexRequest>();
    request->set_serverid(m_leaderId);
    auto response = std::make_shared<raftRpcProtoc::ReadIndexResponse>();

    if (m_raftNode->sendReadIndex(leaderId, request, response)) {
        if (response->isleader()) {
            *commitIndex = response->commitindex();
            return true;
        }
    }
    return false;
}

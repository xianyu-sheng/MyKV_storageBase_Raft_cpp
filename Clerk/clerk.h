#pragma once
#include <random>
#include <string>
#include <vector>
#include <memory>
#include "../myRPC/User/KrpcChannel.h"
#include "../myRPC/Server/KrpcController.h"
#include "../Proto/raftKVRpcProtoc/KvServerRPC.pb.h"
#include "../Proto/raftRpcProtoc/raftRPC.pb.h"

class Clerk {
public:
    void Init(const std::string& configFile="");
    void Put(const std::string& key, const std::string& value);
    std::string Get(const std::string& key);

    // 新增：写入商品特征（走 Raft 强一致路径）
    void PutFeature(const raftKVRpcProtoc::ItemFeature& feature);

    // 新增：向量召回搜索（走 CQRS 只读视图，Leader/Follower 均可）
    struct SearchResult {
        std::vector<std::string> item_ids;
        std::vector<float> scores;
        int64_t search_time_us = 0;
    };
    SearchResult Search(const std::vector<float>& query_vector, int topK = 10);

    void Close();

private:
    void InitStub();
    int RequestId_;
    int ClientId_;
    std::shared_ptr<KrpcChannel> channel_;
    std::shared_ptr<raftKVRpcProtoc::kvServerRpc_Stub> stub_;
    std::string m_leaderIp;
    int m_leaderPort;
};

// ... random_key 和 random_value 函数保持不变 ...

inline std::string random_key(int keySpace,std::mt19937 &gen){
    std::uniform_int_distribution<int> dist(0,keySpace-1);
    int id=dist(gen);
    return "key"+std::to_string(id);
}

inline std::string random_value(int valueSize,std::mt19937 &gen){
    static const char charset[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    
    std::uniform_int_distribution<int> dist(0,(int)sizeof(charset)-2);
    std::string v;
    v.reserve(valueSize);
    for(int i=0;i<valueSize;i++){
        v.push_back(charset[dist(gen)]);
    }
    return v;
}
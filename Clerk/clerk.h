#pragma once
#include <string>
#include <vector>
#include <memory>
#include <random> // 用于生成随机ID
#include "rpc/mprpcchannel.h"
#include "raftRpcPro/kvServerRPC.pb.h"

class Clerk {
public:
    void Init(const std::string& configFile);
    void Put(const std::string& key, const std::string& value);
    std::string Get(const std::string& key);

private:
    std::vector<std::shared_ptr<MprpcChannel>> channels_;
    int currentLeaderIndex_;
    int requestId_;
    int clientId_; // 新增：客户端唯一标识
};
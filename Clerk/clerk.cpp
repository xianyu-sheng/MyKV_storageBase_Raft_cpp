#include "clerk.h"
#include <fstream>
#include <iostream>
#include <unistd.h> // for usleep

void Clerk::Init(const std::string& configFile) {
    std::ifstream file(configFile);
    if (!file.is_open()) {
        std::cerr << "Config file error!" << std::endl;
        return;
    }
    std::string line;
    while (std::getline(file, line)) {
        if (line.empty()) continue;
        size_t colon = line.find(':');
        std::string ip = line.substr(0, colon);
        short port = std::stoi(line.substr(colon + 1));
        // 使用 make_shared
        channels_.emplace_back(std::make_shared<MprpcChannel>(ip, port, true));
    }
    currentLeaderIndex_ = 0;
    requestId_ = 0;
    
    // 生成随机 ClientID (简单实现，实际生产可能需要更复杂的UUID)
    std::srand(std::time(nullptr));
    clientId_ = std::rand(); 
}


void Clerk::Put(const std::string& key, const std::string& value) {
    requestId_++; 
    kvserverrpc::PutRequest request;
    request.set_key(key);
    request.set_value(value);
    request.set_clientid(clientId_); // 务必在 proto 中添加此字段
    request.set_requestid(requestId_);

    while (true) {
        kvserverrpc::PutResponse response;
        MprpcController controller;
        
        auto& channel = channels_[currentLeaderIndex_];
        kvserverrpc::kvServerRpc_Stub stub(channel.get());
        
        // 发起 RPC
        stub.Put(&controller, &request, &response, nullptr);

        if (!controller.Failed()) {
            if (response.issuccess()) {
                return; // 成功直接返回
            } 
            // 如果 server 告诉我们要去连谁，更新 index 并 continue
            if (response.has_leaderhint()) {
                currentLeaderIndex_ = response.leaderhint();
                // ！！！关键修改：收到 hint 后直接重试该节点，不要往下走去 +1
                usleep(10000); // 稍微停顿一下防止死循环
                continue; 
            }
        }

        // 只有在网络失败、或者对方不是 Leader 且没有给出 Hint 时，才轮询
        currentLeaderIndex_ = (currentLeaderIndex_ + 1) % channels_.size();
        
        // 增加睡眠时间，避免选举期间风暴攻击 (100ms)
        usleep(100000); 
    }
}

void Clerk::Put(const std::string& key, const std::string& value) {
    requestId_++; 
    kvserverrpc::PutRequest request;
    request.set_key(key);
    request.set_value(value);
    request.set_clientid(clientId_); // 务必在 proto 中添加此字段
    request.set_requestid(requestId_);

    while (true) {
        kvserverrpc::PutResponse response;
        MprpcController controller;
        
        auto& channel = channels_[currentLeaderIndex_];
        kvserverrpc::kvServerRpc_Stub stub(channel.get());
        
        // 发起 RPC
        stub.Put(&controller, &request, &response, nullptr);

        if (!controller.Failed()) {
            if (response.issuccess()) {
                return; // 成功直接返回
            } 
            // 如果 server 告诉我们要去连谁，更新 index 并 continue
            if (response.has_leaderhint()) {
                currentLeaderIndex_ = response.leaderhint();
                // ！！！关键修改：收到 hint 后直接重试该节点，不要往下走去 +1
                usleep(10000); // 稍微停顿一下防止死循环
                continue; 
            }
        }

        // 只有在网络失败、或者对方不是 Leader 且没有给出 Hint 时，才轮询
        currentLeaderIndex_ = (currentLeaderIndex_ + 1) % channels_.size();
        
        // 增加睡眠时间，避免选举期间风暴攻击 (100ms)
        usleep(100000); 
    }
}
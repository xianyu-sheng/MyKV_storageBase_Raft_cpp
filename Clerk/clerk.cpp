#include "clerk.h"

//实现clerk类
//初始化客户端
// clerk.cpp
void Clerk::Init(const std::string& configFile) {
    // 读取配置文件（格式示例：每行一个节点，如 "127.0.0.1:8080"）
    std::ifstream file(configFile);
    std::string line;
    while (std::getline(file, line)) {
        if (line.empty()) continue;
        // 解析 IP 和端口
        size_t colon = line.find(':');
        std::string ip = line.substr(0, colon);
        short port = std::stoi(line.substr(colon + 1));
        // 创建 RPC 通道
        channels_.emplace_back(new MprpcChannel(ip, port, true));
    }
    currentLeaderIndex_ = 0; // 初始从第一个节点开始尝试
    requestId_ = 0;
}


// clerk.cpp
void Clerk::Put(const std::string& key, const std::string& value) {
    requestId_++; // 递增请求 ID，确保幂等性
    kvserverrpc::PutRequest request;
    kvserverrpc::PutResponse response;
    MprpcController controller;

    request.set_key(key);
    request.set_value(value);
    request.set_requestid(requestId_);

    while (true) {
        // 向当前节点发送 Put 请求
        auto& channel = channels_[currentLeaderIndex_];
        kvserverrpc::kvServerRpc_Stub stub(channel.get());
        stub.Put(&controller, &request, &response, nullptr);

        if (!controller.Failed()) {
            // 若成功，检查是否为 Leader 处理
            if (response.issuccess()) {
                break; // 写入成功
            } else if (response.has_leaderhint()) {
                // 若返回新 Leader 地址，更新索引并重试
                currentLeaderIndex_ = response.leaderhint();
            }
        }

        // 失败或非 Leader，轮询下一个节点
        currentLeaderIndex_ = (currentLeaderIndex_ + 1) % channels_.size();
        controller.Reset(); // 重置控制器
        usleep(1000); // 短暂休眠避免频繁重试
    }
}

std::string Clerk::Get(const std::string& key) {
    requestId_++;
    kvserverrpc::GetRequest request;
    kvserverrpc::GetResponse response;
    MprpcController controller;

    request.set_key(key);
    request.set_requestid(requestId_);

    while (true) {
        auto& channel = channels_[currentLeaderIndex_];
        kvserverrpc::kvServerRpc_Stub stub(channel.get());
        stub.Get(&controller, &request, &response, nullptr);

        if (!controller.Failed()) {
            if (response.issuccess()) {
                return response.value(); // 返回读取到的值
            } else if (response.has_leaderhint()) {
                currentLeaderIndex_ = response.leaderhint();
            }
        }

        currentLeaderIndex_ = (currentLeaderIndex_ + 1) % channels_.size();
        controller.Reset();
        usleep(1000);
    }
}
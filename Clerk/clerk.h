#pragma once
#include <random>
#include <string>
#include <memory> // 必须包含这个
#include "../myRPC/User/KrpcChannel.h"
#include "../myRPC/Server/KrpcController.h"
#include "../Proto/raftKVRpcProtoc/KvServerRPC.pb.h"
#include "../Proto/raftRpcProtoc/raftRPC.pb.h"

class Clerk {
public:
    void Init(const std::string& configFile="");
    void Put(const std::string& key, const std::string& value);
    std::string Get(const std::string& key);

    // 关闭连接（析构时调用）
    void Close();

private:
    int RequestId_;
    int ClientId_;

    // 【关键优化】：用来复用连接的成员变量
    std::shared_ptr<KrpcChannel> channel_;
    std::shared_ptr<raftKVRpcProtoc::kvServerRpc_Stub> stub_;

    // 辅助函数：初始化或重置连接
    void InitStub();

    // 记录当前连接的 Leader 地址（用于重连判断）
    std::string m_leaderIp;
    int m_leaderPort;

    // 连接池单例引用
    // static ConnectionPool& getPool() { return ConnectionPool::getInstance(); }
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
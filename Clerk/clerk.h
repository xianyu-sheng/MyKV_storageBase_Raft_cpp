#pragma once
#include <random>//用于生成随机ID
#include <string>
#include "../myRPC/User/KrpcChannel.h"
#include "../myRPC/Server/KrpcController.h"
#include "../Proto/raftKVRpcProtoc/KvServerRPC.pb.h"
#include "../Proto/raftRpcProtoc/raftRPC.pb.h"


class Clerk {
public:
    //初始化版本clerk（生成ClientID，重置requestID）
    //注意：myRPC框架自身的INIT在main里通过KrpcApplication::init完成
    void Init(const std::string& configFile="");
    void Put(const std::string& key, const std::string& value);
    std::string Get(const std::string& key);
private:
    int RequestId_;
    int ClientId_; // 新增：客户端唯一标识
    std::unique_ptr<KrpcChannel> channel_;
    std::unique_ptr<raftKVRpcProtoc::kvServerRpc_Stub> stub_;
};

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
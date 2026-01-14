#pragma once
#include <random>//用于生成随机ID
#include <string>
#include "../myRPC/User/KrpcChannel.h"
#include "../myRPC/Server/KrpcController.h"
#include "../raftKVRpcProtoc/raftKCProtoc.pb.h"


class Clerk {
public:
    //初始化版本clerk（生成ClientID，重置requestID）
    //注意：myRPC框架自身的INIT在main里通过KrpcApplication::init完成
    void Init(const std::string& configFile="");

private:
    int RequestId_;
    int ClientId_; // 新增：客户端唯一标识
};
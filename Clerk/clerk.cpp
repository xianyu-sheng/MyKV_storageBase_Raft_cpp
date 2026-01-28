#include "clerk.h"
#include <fstream>
#include <iostream>
#include <unistd.h> 
#include <glog/logging.h>
#include <sys/stat.h>

// 辅助函数：负责建立连接
void Clerk::InitStub() {
    // 使用长连接模式 (keep_alive=true)
    channel_ = std::make_shared<KrpcChannel>(true);
    stub_ = std::make_shared<raftKVRpcProtoc::kvServerRpc_Stub>(channel_.get());
}

void Clerk::Init(const std::string& configFile){
    RequestId_ = 0;
    std::srand(std::time(nullptr));
    ClientId_ = std::rand();
    // 刚开始不需要立即连接，等发请求时懒加载
    channel_ = nullptr;
    stub_ = nullptr;
}

void Clerk::Put(const std::string& key, const std::string& value) {
    RequestId_++; 
    raftKVRpcProtoc::PutAppendArgs request;
    request.set_key(key);
    request.set_value(value);
    request.set_clientid(std::to_string(ClientId_)); 
    request.set_requestid(RequestId_);
    request.set_op("Put");
    
    int retry_count = 0;
    const int kMaxRetry = 100;  // 增加重试次数

    while (true) {
        if (!stub_) {
            InitStub();
        }

        KrpcController controller;
        controller.SetTimeout(5000);  // 增加到 5 秒
        raftKVRpcProtoc::PutAppendReply response;
        
        stub_->PutAppend(&controller, &request, &response, nullptr);

        if(controller.Failed()){
            // 网络错误才销毁连接
            stub_ = nullptr;
            channel_ = nullptr;
            retry_count++;
            if (retry_count > kMaxRetry) {
                 LOG(ERROR) << "Put RPC failed too many times!";
                 return; 
            }
            usleep(50000);  // 减少到 50ms
            continue;
        }
        
        retry_count = 0;
        const std::string& err = response.err();
        if(err == "OK"){
            return;
        }
        if(err == "ErrWrongLeader"){
            // 遇到非 Leader，销毁连接重新查询 ZK
            stub_ = nullptr;
            channel_ = nullptr;
            usleep(50000);  // 等待50ms后重试
            continue;
        }
        return;
    }
}
std::string Clerk::Get(const std::string& key){
    ++RequestId_;
    raftKVRpcProtoc::GetArgs request;
    request.set_key(key);
    request.set_clientid(std::to_string(ClientId_));
    request.set_requestid(RequestId_);
    
    int retry_count = 0;
    const int kMaxRetry = 100;  // 增加重试次数

    while(true){
        // 1. 检查连接
        if (!stub_) {
            InitStub();
        }
        
        // 2. 发送 Get 请求
        KrpcController controller;
        controller.SetTimeout(5000);  // 增加到 5 秒
        raftKVRpcProtoc::GetReply response;
        stub_->Get(&controller, &request, &response, nullptr);
        
        // 3. 错误处理
        if(controller.Failed()){
            // 网络/RPC错误 -> 销毁连接
            stub_ = nullptr;
            channel_ = nullptr;
            
            retry_count++;
            if (retry_count > kMaxRetry) {
                LOG(ERROR) << "Get RPC failed too many times!";
                return "";
            }
            usleep(50000); // 减少到 50ms
            continue;
        }
        
        retry_count = 0;
        const std::string& err = response.err();
        if(err == "OK"){
            return response.value();
        }
        if(err == "ErrNoKey"){
            return "";
        }
        if(err == "ErrWrongLeader"){
            // 遇到非 Leader，销毁连接重新查询 ZK
            stub_ = nullptr;
            channel_ = nullptr;
            usleep(50000);
            continue;
        }
        return "";
    }
}
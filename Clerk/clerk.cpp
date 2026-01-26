#include "clerk.h"
#include <fstream>
#include <iostream>
#include <unistd.h> 

// 辅助函数：负责建立连接
// 这里我们依然传 false (短连接模式)，但因为我们把对象存下来了，它实际上就是长连接！
// 这样做可以避开你之前遇到的 keep_alive=true 的那个 bug。
void Clerk::InitStub() {
    // 每次创建时，让它去 ZK 查一次
    channel_ = std::make_shared<KrpcChannel>(false); 
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
    const int kMaxRetry = 50; // 最大重试次数

    while (true) {
        // 1. 【懒汉式连接】：如果 stub 为空，说明是第一次或者是上一次出错了，需要重连
        if (!stub_) {
            InitStub();
        }

        KrpcController controller;
        controller.SetTimeout(3000);
        raftKVRpcProtoc::PutAppendReply response;
        
        // 2. 使用成员变量 stub_ 发送请求
        stub_->PutAppend(&controller, &request, &response, nullptr);

        // 3. 处理框架级错误（网络断了、ZK连不上、Server挂了）
        if(controller.Failed()){
            // 【核心逻辑】：一旦网络出错，认为当前连接已废，直接置空！
            // 这样下一次循环就会自动触发 InitStub() 重新去 ZK 找节点
            stub_ = nullptr; 
            
            retry_count++;
            if (retry_count > kMaxRetry) {
                 std::cerr << "Put RPC failed too many times! Give up." << std::endl;
                 return; 
            }
            // 失败后休息一下，防止雪崩
            usleep(200000); // 200ms
            continue;
        }
        
        retry_count = 0; // 网络通了，重置计数

        // 4. 处理业务错误
        const std::string& err = response.err();
        if(err == "OK"){
            return;
        }
        if(err == "ErrWrongLeader"){
            // 找错 Leader 了。
            // 策略：虽然网络是通的，但为了保险起见，我们也可以重置连接，
            // 强制让 Channel 去 ZK 刷新一下最新的 Leader 是谁。
            stub_ = nullptr; 
            usleep(50000); // 50ms 等待选举
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
    const int kMaxRetry = 50;

    while(true){
        // 1. 检查连接
        if (!stub_) {
            InitStub();
            
            // --- 预热阶段 ---
            {
                KrpcController dummy_controller;
                // 【修复1】：给预热请求也加上超时！
                // 预热只是为了通连接，时间可以设短一点，比如 1000ms
                dummy_controller.SetTimeout(1000); 

                raftKVRpcProtoc::PutAppendArgs dummy_args;
                dummy_args.set_key("ping");
                dummy_args.set_value("warmup");
                dummy_args.set_op("Put"); // 注意大小写，你之前写的 "put" 最好统一规范
                dummy_args.set_clientid(std::to_string(ClientId_));
                dummy_args.set_requestid(-1);

                raftKVRpcProtoc::PutAppendReply dummy_reply;
                stub_->PutAppend(&dummy_controller, &dummy_args, &dummy_reply, nullptr);

                // 【修复2】：如果预热都超时/失败了，说明连接质量很差，不要继续发 Get 了
                if (dummy_controller.Failed()) {
                    // std::cerr << "Warmup failed: " << dummy_controller.ErrorText() << std::endl;
                    stub_ = nullptr; // 销毁连接
                    usleep(50000);   // 稍微休息
                    continue;        // 直接进入下一次大循环重试
                }
            }
        }
        
        // --- 正式请求阶段 ---
        KrpcController controller;
        controller.SetTimeout(3000); // 这里你已经设置了，很好
        raftKVRpcProtoc::GetReply response;
        
        // 2. 发送
        stub_->Get(&controller, &request, &response, nullptr);
        
        // 3. 错误处理
        if(controller.Failed()){
            // 网络/RPC错误 -> 销毁连接，下次重连
            stub_ = nullptr; 
            
            retry_count++;
            if (retry_count > kMaxRetry) {
                std::cerr << "Get RPC failed too many times! Give up." << std::endl;
                return "";
            }
            // 多线程下建议睡久一点，避开拥堵
            usleep(100000); // 100ms
            continue;
        }
        
        retry_count = 0;
        const std::string& err = response.err();
        if(err == "OK"){
            return response.value();
        }
        if(err == "ErrNoKey"){
            return "no found,no Key"; 
        }
        if(err == "ErrWrongLeader"){
            // 选错 Leader -> 销毁连接，重新去 ZK 查
            stub_ = nullptr;
            usleep(100000);
            continue;
        }
        return "";
    }
}
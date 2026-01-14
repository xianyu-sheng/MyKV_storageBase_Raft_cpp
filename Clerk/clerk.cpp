#include "clerk.h"
#include <fstream>
#include <iostream>
#include <unistd.h> // for usleep


void Clerk::Put(const std::string& key, const std::string& value) {
    RequestId_++; 
    raftKVRpcProtoc::PutAppendArgs request;
    request.set_key(key);
    request.set_value(value);
    request.set_clientid(ClientId_); // 务必在 proto 中添加此字段
    request.set_requestid(RequestId_);
    request.set_op("Put");
    while (true) {
        //2.每次请求构造一个KrpcChannel(内部会通过ZooKeeper找到一个服务节点)
        KrpcChannel channel(false);
        raftKVRpcProtoc::kvServerRpc_Stub stub(&channel);//设置代理
        KrpcController controller;//设计一个控制器 返回错误信息
        raftKVRpcProtoc::PutAppendReply response;
        
        //3.发起RPC
        stub.PutAppend(&controller,&request,&response,nullptr);

        //4.处理myRpc 层面的错误（网络/超时/反序列化等）
        //框架级错误
        if(controller.Failed()){
            //打印日志 并重试
            std::cerr<<"RPC failed:"<<controller.ErrorText()<<std::endl;
            continue;
        }
        //5.业务层面的错误 和 KvServer的reply->set_err()对应
        const std::string& err=response.err();
        if(err=="OK"){
            return;//成功
        }
        if(err=="ErrWrongLeader"){
            //当前节点不是Leader节点 重试
            usleep(100000);
            continue;
        }
        return;//其他错误 直接返回
    }
}

std::string Clerk::Get(const std::string& key){
    ++RequestId_;
    raftKVRpcProctoc::GetArgs request;
    request.set_key(key);
    request.set_clientid(ClientId_);
    request.set_requestid(RequestId_);

    while(true){
        KrpcChannel channel(false);
        raftKVRpcProctoc::kvServerRpc_Stub stub(&channel);
        KrpcController controller;
        raftKVRpcProctoc::GetReply response;
        stub.Get(&controller,&request,&response,nullptr);
        //如果是框架级错误
        if(controller.Failed()){
            //框架级错误：重试
            continue;
        }
        const std::string& err=response.err();
        if(err=="OK"){
            //找到了 放回
            return response.value();
        }
        if(err=="ErrNoKey"){
            //语义：key不存在  一般我们返回空字符串或者是抛出异常
            return "no found,no Key";
        }
    }
}
void Clerk::Put(const std::string& key, const std::string& value) {
    RequestId_++; 
    kvserverrpc::PutRequest request;
    request.set_key(key);
    request.set_value(value);
    request.set_clientid(ClientId_); // 务必在 proto 中添加此字段
    request.set_requestid(RequestId_);

    while (true) {
        kvserverrpc::PutResponse response;
        MprpcController controller;
        
        auto& channel = channels_[currentLeaderIndex_];
        kvserverrpc::kvServerRpc_Stub stub(channel.get());
        
        // 发起 RPC
        stub.Put(&controller, &request, &response, nullptr);

        //4.处理myRpc 层面的错误（网络/超时/反序列化等）
        //框架级错误
        if(controller.Failed()){
            //打印日志 并重试
            std::cerr<<"RPC failed:"<<controller.ErrorText()<<std::endl;
            continue;
        }
        //5.业务层面的错误 和 KvServer的reply->set_err()对应
        const std::string& err=response.err();
        if(err=="OK"){
            return;//成功
        }
        if(err=="ErrWrongLeader"){
            //当前节点不是Leader节点 重试
            usleep(100000);
            continue;
        }
        return;//其他错误 直接返回
    }        if (!controller.Failed()) {
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

//新init 只做本地初始化  不在读  config文件
void Clerk::Init(const std::string& configFile){
    RequestId_=0;
    std::srand(std::time(nullptr));
    ClientId_=std::rand();
}
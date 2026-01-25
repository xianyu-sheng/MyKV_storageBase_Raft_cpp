#include "clerk.h"
#include <fstream>
#include <iostream>
#include <unistd.h> // for usleep


void Clerk::Put(const std::string& key, const std::string& value) {
    RequestId_++; 
    raftKVRpcProtoc::PutAppendArgs request;
    request.set_key(key);
    request.set_value(value);
    request.set_clientid(std::to_string(ClientId_)); // clientid 是 bytes/string，用字符串表示
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
            usleep(10000);
            continue;
        }
        return;//其他错误 直接返回
    }
}

std::string Clerk::Get(const std::string& key){
    ++RequestId_;
    raftKVRpcProtoc::GetArgs request;
    request.set_key(key);
    request.set_clientid(std::to_string(ClientId_));
    request.set_requestid(RequestId_);

    while(true){
        KrpcChannel channel(false);
        raftKVRpcProtoc::kvServerRpc_Stub stub(&channel);
        KrpcController controller;
        raftKVRpcProtoc::GetReply response;
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
        if(err=="ErrWrongLeader"){
            //同Put：换节点重试
            usleep(10000);
            continue;
        }
        //其他业务错误
        return "";
    }
}

//新init 只做本地初始化  不再读 config 文件
void Clerk::Init(const std::string& configFile){
    RequestId_=0;
    std::srand(std::time(nullptr));
    ClientId_=std::rand();
}
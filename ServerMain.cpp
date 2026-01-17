#include "myRPC/Server/Krpcapplication.h"
#include "myRPC/Server/Krpcprovider.h"

#include "KvServer/kvServer.h"
#include "Raft/raft.h"

#include <memory>
#include <thread>
#include <iostream>

int main(int argc,char** argv){
    //1.初始化myRPC框架，（读取-i conf/myrpc.conf）
    KrpcApplication::Init(argc,argv);
    //2.创建RAft相关组件
    int me=0;//单节点，ID就写0
    if(const char* env=std::getenv("RAFT_ME")){
        me=std::atoi(env);
    }
    const int kTotalServers = 3;
    const int kBasePort=8000;
    //2.1持久化组件（会自动创建./raft_persist 目录并加载状态）
    auto persister=std::make_shared<Persister>(me);
    //2.2Raft->KvServer的apply通道
    auto applyCh=std::make_shared<LockQueue<ApplyMsg>>();
    //2.3 peers：单节点版本，长度为1 占位即可
    std::vector<std::shared_ptr<RaftRpcUtil>> peers(kTotalServers);
    for(int i=0;i<kTotalServers;i++){
        if(i==me){
            peers[i]=nullptr;
        }else{
            peers[i]=std::make_shared<KrpcRaftRpcClient>(
                "127.0.0.1",
                kBasePort+i
            );
        }
    }
    //对于单节点,peers[0]不会被真正用到 可以保持为空
    //2.4创建并初始化节点
    auto raftNode=std::make_shared<Raft>();
    raftNode->init(peers,me,persister,applyCh);
    //3.创建KvServer 并启动Apply线程
    auto kvServer=new KvServer(raftNode);

    //3.1 启动一个线程，不断从applyCh取ApplyMsg然后改调用Kv/server::Apply
    std::thread applyThread([applyCh,kvServer](){
        while(true){
            ApplyMsg msg;
            //这里给一个适当的超时时间，比如1000ms 避免永久阻塞不方便退出逻辑
            if(applyCh->timeOutPop(1000,&msg)){
                kvServer->Apply(msg);
            }
        }
    });
    applyThread.detach();//// 简单起见，main 不去 join，它会随进程一起退出
    //4.注册KvServer为RPC服务，并启动RPC服务器
    KrpcProvider provider;
    provider.NotifyService(kvServer);// KvServer 继承自 raftKVRpcProtoc::kvServerRpc
    provider.NotifyService(raftNode.get());
    std::cout <<"RAft KV server starting ...." << std::endl;
    //这一步会阻塞，开始箭筒端口并处理RPC请求
    provider.Run();

    return 0;
}
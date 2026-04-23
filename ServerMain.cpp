#include "myRPC/Server/Krpcapplication.h"
#include "myRPC/Server/Krpcprovider.h"

#include "KvServer/kvServer.h"
#include "Raft/raft.h"

#include <memory>
#include <thread>
#include <iostream>

int main(int argc,char** argv){
    //1.初始化myRPC框架（读取-i conf/myrpc.conf）
    KrpcApplication::Init(argc,argv);
    //2.创建Raft相关组件
    int me=0;
    if(const char* env=std::getenv("RAFT_ME")){
        me=std::atoi(env);
    }
    const int kTotalServers = 3;
    // 与 myrpc_0/1/2.conf 中的 rpcserverport 保持一致：8001/8002/8003
    const int kPeerPorts[3] = {8001, 8002, 8003};
    //2.1持久化组件（会自动创建./raft_persist 目录并加载状态）
    auto persister=std::make_shared<Persister>(me);
    //2.2 Raft->KvServer 的 apply 通道
    auto applyCh=std::make_shared<LockQueue<ApplyMsg>>();
    //2.3 peers：创建到其他节点的 RPC 客户端
    std::vector<std::shared_ptr<RaftRpcUtil>> peers(kTotalServers);
    for(int i=0;i<kTotalServers;i++){
        if(i==me){
            peers[i]=nullptr;
        }else{
            peers[i]=std::make_shared<KrpcRaftRpcClient>(
                "127.0.0.1",
                static_cast<uint16_t>(kPeerPorts[i])
            );
        }
    }
    //2.4 创建 KvServer（在 Raft 初始化前，以便 Raft 将 applyCh 绑定到 KvServer）
    auto kvServer=new KvServer(nullptr);
    //2.5 创建并初始化 Raft 节点
    auto raftNode=std::make_shared<Raft>();
    raftNode->init(peers,me,persister,applyCh);
    //2.6 KvServer 接管 Raft 引用
    kvServer->setRaftNode(raftNode);

    //2.7 启动 apply 线程：负责将 Raft 日志重放到 SkipList
    std::thread applyThread([applyCh,kvServer](){
        while(true){
            ApplyMsg msg;
            if(applyCh->timeOutPop(1000,&msg)){
                kvServer->Apply(msg);
            }
        }
    });
    applyThread.detach();

    // 注：HNSW 索引通过 Search RPC 首次触发时懒加载构建（buildIndexIfNeeded）
    // 这确保 SkipList 已经包含了 Raft 持久化的所有数据

    //3.注册 KvServer 为 RPC 服务，并启动 RPC 服务器
    KrpcProvider provider;
    provider.NotifyService(kvServer);  // KvServer 继承自 raftKVRpcProtoc::kvServerRpc
    provider.NotifyService(raftNode.get());
    std::cout <<"Raft KV server starting ...." << std::endl;
    provider.Run();

    return 0;
}

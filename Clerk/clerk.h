// clerk.h
#include <string>
#include <vector>
#include "rpc/mprpcchannel.h"
#include "raftRpcPro/kvServerRPC.pb.h"

class Clerk {
public:
    // 初始化：读取配置文件，获取集群节点信息
    void Init(const std::string& configFile);

    // 写入键值对
    void Put(const std::string& key, const std::string& value);

    // 读取键的值
    std::string Get(const std::string& key);

private:
    // 集群节点的 RPC 通道列表
    std::vector<std::shared_ptr<MprpcChannel>> channels_;
    // 当前尝试通信的节点索引（用于重试）
    int currentLeaderIndex_;
    // 生成唯一请求 ID（用于幂等性处理）
    int requestId_;
};

//Clerk类当作外部客户端，与Raft集群联系
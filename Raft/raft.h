#ifndef RAFT_H
#define RAFT_H
////方便网络分区的时候debug，网络异常的时候为disconnected,只要网络正常的就为AppNormal，防止match
constexpr int Disconnected = 0;
constexpr int AppNormal = 1;

////投票状态
constexpr int Killed=0;
constexpr int Vted=1;//本轮已经投过票了
constexpr int Expire=2;//投票（消息、竞选者）过期
constexpr int Normal=3;

class Persister;
class ApplyMsg;
class Raft : public raftRpcProtoc::raftRpc{
private:
    std::mutex m_mtx;
    std::vector<std::shared_ptr<RaftRpcUtil>>m_peers;//需要与其他的Raft节点通信，这里保存与其他节点通信的RPC入口
    std::shared_ptr<Persister>m_persister;//持久层，负责Raft数据的持久化
    int m_me;//Raft节点是是以集群启动，用来表示自己的编号
    int m_currentTerm;//记录当前的任期
    int m_votedFor;//记录当前term给谁投过票
    std::vector<raftRpcProtoc::LogEntry>m_logs;//日志条目数组，包含了状态机要执行的指令集，以及收到领导时的任期号
    //这两个状态所有节点都在维护，易丢失
    int m_commitIndex;//提交的Index
    int m_lastApplied;//已经汇报给状态机Log的Index;

    //这两个状态需要Leader来进行维护，易失
    std::vector<int> m_nextIndex;//下一个要发送给追随者的节点索引
    std::vector<int> m_matchIndex;//追随者返回给领导者的已经收到了多少日志条目的节点索引
    enum Status{
        Follower,
        Candidate,
        Leader
    };

    //保存当前身份
    Status m_status;
    std::shared_ptr<LockQueue<ApplyMsg>> applyChan;//client从这里取日志，client与Raft的通信接口
    //选举超时
    std::chrono::_V2::system_clock::time_point m_lastRestElectionTime;
    std::chrono::_v2::system_clock::time_point m_lastRestHearBeatTime;//心跳超时
};
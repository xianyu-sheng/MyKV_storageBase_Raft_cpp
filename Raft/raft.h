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

    //用于传入快照点和最后的任期
    int m_lastSnapshotIncludeIndex;
    int m_lastSnapshotIncludeTerm;
    //协程
    std::unique_ptr<monsoon::IOManager> m_ioManager = nullptr;

    public:
        void AppendEntries1(const raftRpcProtoc::AppendEntryiesArgs* args,raftRpcProtoc::AppendEntryiesReply* reply);//日志同步+心跳
        void applierTicker();//定期向状态机写入日志
        bool CondInstallSnapshot(int lastIncludeTerm,int lastIncludeIndex,std::string snapshot);//记录某个时候的状态

        void doElection();//执行选举
        void doHeartBeat();//发起心跳
        //监控是否发起选举
        //每隔一段时间检查睡眠时间有没有重置定时器，没有则超时
        //如果有则设置合适的睡眠时机，睡眠到重置时机+超时时机
        void electionTimeoutTicker();//监控是否发起选举
        std::vector<ApplyMsg> getApplyLogs();//获取应用日志
        int getnewCommandIndex();//获取新命令的索引

        void getPrevLogInfo(int server,int* preindex,int* preterm);//获取当前日志信息
        void GetState(int* term,bool* isLeader);//看当前的节点是否是Leader节点
        void InstallSnapshot1(const raftRpcProtoc::InstallSnapshotRequest* request,raftRpcProtoc::InstallSnapshotResponse* response);//安装快照
        void leaderHearBeatTicker();//负责查看是否该发送心跳了，如果该发起就执行doHeartBeat()
        void leaderSendSnapShot(int server);//领导节点发送快照
        void leaderUpdatecommitIndex(int index);//领导更新提交索引
        bool matchLog(int logIndex,int LogTerm);//对象Index日志是否匹配，用来判断领导节点的日志与追随者是否匹配，用于选举以及判断心跳日志是否最新你
        void persist();//持久化当前状态

        void RequestVote1(const raftRpcProtoc::RequestVoteArgs* args,raftRpcProtoc::RequestVoteReply* reply);//变成候选者请求其他节点投票
        bool UPtodata(int index,int term);//判断当前节点是否有最新日志
        int getLastLogIndex();//获取最后一个日志条目索引
        int getLastLogTerm();//获取最后一个日志条目的任期

        void getLastLogIndexAndTerm(int* lastLogIndex,int* lastLogTerm);//获取最后一个日志的索引和任期
        int GetRaftStateSize();//获取raft状态的大小
        int getSlicesIndexFromLogIndex(int Logindex);//将日志索引转换为日志条目在m_logs数组中的位置
        bool sendRequestVote(int server,shared_ptr<raftRpcProtoc::RequestVoteArgs> args,shared_ptr<raftRpcProtoc::RequestVoteReply> reply,shared_ptr<int> voteNum);//请求其他节点为自己投票
        bool sendAppendEntries(int server,shared_ptr<raftRpcProtoc::AppendEntryiesArgs>args,shared_ptr<raftRpcProtoc::AppendEntryiesReply>reply,shared_ptr<int>appendNum);//发送追加日志条目

        void pushMsgToKVserver(ApplyMsg msg);//向上层KVserver发送消息
        void readPersist(std::string data);//读取持久化数据
        std::string persistDate();//持久化数据
        void Start(Op command,int* newLogindex);//启动
        void Raft::init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persist,
                std::shared_ptr<LockQueue<ApplyMsg>> applyCh);//初始化
        bool callAppendEntriesRpc(int peerId, 
                             std::shared_ptr<raftRpcProtoc::AppendEntriesArgs> args,
                             std::shared_ptr<raftRpcProtoc::AppendEntriesReply> reply);
        bool handleTermMismatch(int replyTerm);
        bool isStillLeader();
        void handleLogMismatch(int peerId, std::shared_ptr<raftRpcProtoc::AppendEntriesReply> reply);
        void updatePeerSyncState(int peerId, 
                                std::shared_ptr<raftRpcProtoc::AppendEntriesArgs> args,
                                std::shared_ptr<int> appendNums);
        void tryUpdateCommitIndex(std::shared_ptr<raftRpcProtoc::AppendEntriesArgs> args,
                                std::shared_ptr<int> appendNums);
};
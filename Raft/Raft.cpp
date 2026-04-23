#include  "raft.h"
#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <random>
#include <thread>
#include <vector>
#include <atomic>
#include <algorithm>
#include <iostream>
#include <unistd.h>
#include <sstream>
#include <sys/time.h>  // gettimeofday for milliseconds
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/serialization/access.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include "op_coder.h"

namespace{
    // 这些值就是你旧工程 config.h 里的内容，直接拷过来即可。
constexpr bool Debug = true;
constexpr int debugMul = 1;  // 时间单位放大系数
constexpr int HeartBeatTimeout = 25 * debugMul;           // 选举里用到
constexpr int ApplyInterval    = 10 * debugMul;
constexpr int minRandomizedElectionTime = 300 * debugMul; // 选举超时下界
constexpr int maxRandomizedElectionTime = 500 * debugMul; // 选举超时上界
// 协程相关（用于 monsoon::IOManager）
constexpr int  FIBER_THREAD_NUM      = 1;
constexpr bool FIBER_USE_CALLER_THREAD = false;
// === 时间工具 ===
inline std::chrono::system_clock::time_point now() {
    return std::chrono::system_clock::now();
}

// 获取当前毫秒时间戳
inline int64_t getCurrentTimeMillis() {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return static_cast<int64_t>(tv.tv_sec) * 1000 + tv.tv_usec / 1000;
}
inline std::chrono::milliseconds getRandomizedElectionTimeout() {
    static thread_local std::mt19937 rng{std::random_device{}()};
    std::uniform_int_distribution<int> dist(
        minRandomizedElectionTime, maxRandomizedElectionTime);
    return std::chrono::milliseconds(dist(rng));
}
// === 断言和格式化 ===
inline void myAssert(bool cond, const std::string& msg) {
    if (!cond) {
        std::cerr << "Assert failed: " << msg << std::endl;
        std::terminate();
    }
}
template<typename... Args>
std::string format(const char* fmt, Args... args) {
    int size = std::snprintf(nullptr, 0, fmt, args...) + 1;  // 包含 '\0'
    if (size <= 0) {
        return "format error";
    }
    std::vector<char> buf(size);
    std::snprintf(buf.data(), size, fmt, args...);
    return std::string(buf.data(), buf.data() + size - 1);
}
// === 日志打印 ===
inline void DPrintf(const char* fmt, ...) {
    if (!Debug) return;
    va_list ap;
    va_start(ap, fmt);
    std::vprintf(fmt, ap);
    std::printf("\n");
    va_end(ap);
}
}

int Raft::getLogTermFromLogIndex(int logIndex) {
    if (logIndex == m_lastSnapshotIncludeIndex) {
        return m_lastSnapshotIncludeTerm;
    }
    if (logIndex < m_lastSnapshotIncludeIndex) {
        return -1;
    }
    int sliceIdx = getSlicesIndexFromLogIndex(logIndex);
    if (sliceIdx < 0 || sliceIdx >= static_cast<int>(m_logs.size())) {
        return -1;
    }
    return m_logs[sliceIdx].logterm();
}

void Raft::GetState(int* term, bool* isLeader) {
    std::lock_guard<std::mutex> lg(m_mtx);
    if (term) {
        *term = m_currentTerm;
    }
    if (isLeader) {
        *isLeader = (m_status == Leader);
    }
}

int Raft::getLastLogIndex() {
    // 日志全局索引 = 快照中最后包含的索引 + 当前内存日志条目数量
    return m_lastSnapshotIncludeIndex + static_cast<int>(m_logs.size());
}

int Raft::getLastLogTerm() {
    if (m_logs.empty()) {
        return m_lastSnapshotIncludeTerm;
    }
    return m_logs.back().logterm();
}

void Raft::getLastLogIndexAndTerm(int* lastLogIndex, int* lastLogTerm) {
    if (lastLogIndex) {
        *lastLogIndex = getLastLogIndex();
    }
    if (lastLogTerm) {
        *lastLogTerm = getLastLogTerm();
    }
}

int Raft::getSlicesIndexFromLogIndex(int logIndex) {
    // 将全局日志索引转换为 m_logs 中的下标（考虑快照截断）
    if (logIndex <= m_lastSnapshotIncludeIndex) {
        return -1;
    }
    return logIndex - m_lastSnapshotIncludeIndex - 1; // 例如：快照到 5，日志 6 对应下标 0
}

bool Raft::UPtodata(int index, int term) {
    // 判断候选者日志是否至少和当前节点一样新（Raft 选举规则）
    int selfLastIndex = getLastLogIndex();
    int selfLastTerm  = getLastLogTerm();

    if (term != selfLastTerm) {
        return term > selfLastTerm;
    }
    return index >= selfLastIndex;
}

void Raft::getPrevLogInfo(int server, int* preindex, int* preterm) {
    // 调用方在持有 m_mtx 的前提下调用本函数
    int nextIdx = m_nextIndex[server];
    int prevIdx = nextIdx - 1;
    if (preindex) {
        *preindex = prevIdx;
    }
    if (preterm) {
        *preterm = getLogTermFromLogIndex(prevIdx);
    }
}

bool Raft::matchLog(int logIndex, int logTerm) {
    // 与快照点匹配
    if (logIndex == m_lastSnapshotIncludeIndex) {
        return logTerm == m_lastSnapshotIncludeTerm;
    }
    // 早于快照或超出当前日志范围，一定不匹配
    if (logIndex < m_lastSnapshotIncludeIndex || logIndex > getLastLogIndex()) {
        return false;
    }
    int sliceIdx = getSlicesIndexFromLogIndex(logIndex);
    if (sliceIdx < 0 || sliceIdx >= static_cast<int>(m_logs.size())) {
        return false;
    }
    return m_logs[sliceIdx].logterm() == logTerm;
}

int Raft::GetRaftStateSize() {
    // 直接委托给 Persister 统计当前 RaftState 的大小
    return static_cast<int>(m_persister->RaftStateSize());
}

void Raft::leaderUpdateCommitIndex() {
    // 只有 Leader 会调用本函数，调用方应在持有 m_mtx 的前提下调用
    int lastIndex = getLastLogIndex();
    for (int N = lastIndex; N > m_commitIndex; --N) {
        int count = 1; // Leader 自己
        for (int i = 0; i < static_cast<int>(m_peers.size()); ++i) {
            if (i == m_me) continue;
            if (m_matchIndex[i] >= N) {
                ++count;
            }
        }
        if (count >= static_cast<int>(m_peers.size()) / 2 + 1) {
            // 只提交当前任期的日志，保证 Raft 安全性
            if (getLogTermFromLogIndex(N) == m_currentTerm) {
                m_commitIndex = N;
            }
            break;
        }
    }
}

void Raft::pushMsgToKVserver(ApplyMsg msg) {
    if (applyChan) {
        applyChan->push(msg);
    }
}

void Raft::init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persist,
                std::shared_ptr<LockQueue<ApplyMsg>> applyCh) {
    m_peers = peers;  // 与其他结点沟通的rpc接口
    m_me = me;        // 标记自己，不能给自己发送rpc
    m_persister = persist;  // 持久化类
    m_mtx.lock();
    this->applyChan = applyCh;  // 客户端与KV-server沟通的接口
    m_currentTerm = 0;          // 任期初始化为0
    m_status = Follower;        // 初始化身份为follower
    m_commitIndex = 0;          // 初始化提交的日志索引
    m_lastApplied = 0;          // 初始化提交到状态机的日志
    m_logs.clear();

    for (int i = 0; i < m_peers.size(); i++) {
        m_matchIndex.push_back(0);  // 表示没有日志条目已提交或已应用
        m_nextIndex.push_back(0);
    }

    m_votedFor = -1;  // 代表未有投票对象
    m_lastSnapshotIncludeIndex = 0;
    m_lastSnapshotIncludeTerm = 0;
    m_lastResetElectionTime = now();
    m_lastResetHearBeatTime = now();

    // Pipeline 状态初始化
    int n = static_cast<int>(m_peers.size());
    m_inflightCount.resize(n);
    m_inSnapshot.resize(n);
    m_peerSendThreads.resize(n);
    m_peerPipeline.resize(n);  // 初始化每个 Peer 的 Pipeline 状态
    // 初始化 Pipeline 互斥锁
    m_peerPipelineMutex.resize(n);
    for (int i = 0; i < n; ++i) {
        m_peerPipelineMutex[i] = std::make_unique<std::mutex>();
    }
    
    // 批量追加优化初始化
    m_pendingEntries.resize(n);

    readPersist(m_persister->ReadRaftState());  // 从持久化存储中恢复Raft状态

    // 如果m_lastSnapshotIncludeIndex大于0，则将m_lastApplied设置为该值。
    // 这是为了确保在崩溃后能够从快照中恢复状态
    if (m_lastSnapshotIncludeIndex > 0) {
        m_lastApplied = m_lastSnapshotIncludeIndex;
    }

    DPrintf("[Init&ReInit] Server %d, term %d, lastSnapshotIncludeIndex [%d], lastSnapshotIncludeTerm [%d]",
            m_me, m_currentTerm, m_lastSnapshotIncludeIndex, m_lastSnapshotIncludeTerm);

    m_mtx.unlock();  // 完成初始化后解锁，以便其他线程可以访问共享数据

    // 用普通线程替代原来的 monsoon::IOManager 协程调度
    std::thread t1(&Raft::leaderHearBeatTicker, this);
    std::thread t2(&Raft::electionTimeoutTicker, this);
    std::thread t3(&Raft::applierTicker, this);

    t1.detach();
    t2.detach();
    t3.detach();
}

//检验是否达到选举超时
void Raft::electionTimeoutTicker(){
    while(true){
        //如果不睡的话，那么对于Leader来说这个函数会一直空转，浪费CPU 且加入协程后，空转会导致其他协程无法运行
        while(m_status==Leader){
            //所以要让其睡hearBeat的时间，以为内hearbearBeat必选举超时一般小一个量级
            usleep(1000*HeartBeatTimeout);
        }
        std::chrono::duration<signed long int,std::ratio<1,1000000000>>suitableSleepTime{};//初始化一个纳秒级别的时间间隔对象
        std::chrono::system_clock::time_point weakTime{};//记录时间节点
        {
            m_mtx.lock();
            weakTime=now();
            suitableSleepTime=getRandomizedElectionTimeout()+m_lastResetElectionTime-weakTime;
            m_mtx.unlock();
        }
        if(std::chrono::duration<double,std::milli>(suitableSleepTime).count()>1){
            //获取当前的时间节点
            auto start=std::chrono::steady_clock::now();
            //因为没有超过睡眠时间继续睡眠
            usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());
            auto end=std::chrono::steady_clock::now();
            //计算时间差并输出结果（单位为毫秒）
            std::chrono::duration<double,std::milli>duration=end-start;
            auto sleepMs = std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count();
            std::cout << "\033[1;35m electionTimeoutTicker();函数设置睡眠时间为："
          << sleepMs << "毫秒\033[0m" << std::endl;
            //使用ANSI控制序列将输出颜色修改为紫色

            std::cout << "\033[1;35m electionTimeoutTicker();函数的实际睡眠时间为]"<<duration.count()<<"毫秒\033[0m"
                       << std::endl;
        }
        if(std::chrono::duration<double,std::milli>(m_lastResetElectionTime-weakTime).count()>0){
            //说明睡眠的这段时间有重置定时器，那么就没有超时 再次睡眠
            continue;
        }
        /*
        if(preElection()){
            doElection();
        }
        */
        doElection();
    }
}
//预选举
bool Raft::preElection(){
    int preVoteTerm=0;
    int lastLogIndex=-1;
    int lastLogTerm=-1;
    {
        std::lock_guard<std::mutex> lg(m_mtx);
        if(m_status==Leader){
            return false;
        }
        //预选举阶段不修改m_currentTerm 只是试探性的使用下一任期
        preVoteTerm=m_currentTerm+1;
        getLastLogIndexAndTerm(&lastLogIndex,&lastLogTerm);
    }
    int votes=1;//自己逻辑上投一篇
    int totalPeers=static_cast<int>(m_peers.size());
    //循环的发送预选举投票
    for(int i=0;i<totalPeers;++i){
        if(i==m_me){
            continue;
        }
        raftRpcProtoc::PreRequestVoteArgs args;
        args.set_term(preVoteTerm);
        args.set_candidateid(m_me);
        args.set_lastlogindex(lastLogIndex);
        args.set_lastlogterm(lastLogTerm);
        raftRpcProtoc::PreRequestVoteReply reply;
        //然后来送预选举请求
        bool ok=m_peers[i]->PreRequestVote(&args,&reply);
        if(!ok){
            continue;//不管 继续下一个节点
        }
        {
            //如果ok
            std::lock_guard<std::mutex> lg(m_mtx);
            //如果在预选举中发现别人的term更大 仍然要退回Follwer
            if(reply.term()>m_currentTerm){
                m_status=Follower;
                m_currentTerm=reply.term();
                m_votedFor=-1;
                persist();
            }
        }
        if(reply.votegranted()){
            ++votes;
        }
    }
    //最后来检查是否满足预选举的要求 如果满足在吊用真正的选举
    {
        std::lock_guard<std::mutex> lg(m_mtx);
        //预选举结束后，无论成功与否，都重置一下定时器，避免CPU空转
        m_lastResetElectionTime=now();
        if(m_status==Leader){
            return false;
        }
    }
    std::cout << "preelection sucess!" << std::endl;
    //拿到半数以上的投票 开始正式选举
    return votes >= totalPeers/2+1;
}
//节点选举：这里我想要实现预选举的操作【这里是我自己的创新，用于防止网络分区节点无限制增长任期，提高系统稳定性】
//预选举将在正式选举前进行，只有在预选举中获得足够多数票时才进行正式选举
//预选举阶段：不增加任期，只测试其他节点是否能够接收心跳
//在预选举阶段，节点会发送预投票请求，如果大多数节点响应，则进行正式选举
//预选举的主要优势是避免了因网络分区导致的任期不一致问题，提高了系统的稳定性和一致性

void Raft::doElection() {
    // 1. 定义局部变量，用于存储从锁内拿出来的状态快照
    std::shared_ptr<int> votedNum;
    int currentTermSnapshot = 0;
    int lastLogIndexSnapshot = -1;
    int lastLogTermSnapshot = -1;

    // --- 临界区开始 (只做内存操作，极快) ---
    {
        std::lock_guard<std::mutex> g(m_mtx);
        
        // 如果已经是 Leader，直接返回
        if (m_status == Leader) {
            return;
        }

        DPrintf("[ticker-fun-rf(%d)]选举定时器到期且不是Leader，开始选举", m_me);

        // 状态变更
        m_status = Candidate;
        m_currentTerm += 1;
        m_votedFor = m_me; // 给自己投票
        
        // 持久化状态 (I/O操作，但通常必须在锁内保证一致性，除非优化了 persist)
        persist();

        // 初始化票数计数器 (原子计数或通过锁保护，这里用 shared_ptr 传递给回调)
        votedNum = std::make_shared<int>(1);

        // 重置选举定时器
        m_lastResetElectionTime = now();

        // 【关键步骤】：获取当前 Log 的快照信息
        // 必须在锁内调用，因为访问 m_logs 需要线程安全
        getLastLogIndexAndTerm(&lastLogIndexSnapshot, &lastLogTermSnapshot);
        
        // 记录当前的 Term，供循环中使用
        currentTermSnapshot = m_currentTerm;

    } 
    // --- 临界区结束，锁自动释放 ---
    // 此时其他线程（如心跳处理、客户端请求）可以获取锁了


    // --- 网络 I/O 区 (无锁，耗时) ---
    // 使用刚才获取的 snapshot 变量来构建 RPC
    for (int i = 0; i < m_peers.size(); i++) {
        if (i == m_me) {
            continue;
        }

        // 构建请求参数 (使用局部变量 snapshot，而不是成员变量 m_xxx)
        std::shared_ptr<raftRpcProtoc::RequestVoteArgs> requestVoteArgs = std::make_shared<raftRpcProtoc::RequestVoteArgs>();
        
        // 注意：这里必须用 currentTermSnapshot，不能用 m_currentTerm (因为它可能已经被其他线程改了)
        requestVoteArgs->set_term(currentTermSnapshot);
        requestVoteArgs->set_candidateid(m_me);
        requestVoteArgs->set_lastlogindex(lastLogIndexSnapshot);
        requestVoteArgs->set_lastlogterm(lastLogTermSnapshot);

        auto requestVoteReply = std::make_shared<raftRpcProtoc::RequestVoteReply>();

        // 发送 RPC (此时不持有锁，不会阻塞 Raft 主逻辑)
        // sendRequestVote 内部通常会开启新线程或使用异步 IO，
        // 记得在 sendRequestVote 的回调里处理返回值时，需要再次加锁！
        sendRequestVote(i, requestVoteArgs, requestVoteReply, votedNum);
    }
}

//sendRequestVote;
//作为candidate的处理其他节点回复的视角
bool Raft::sendRequestVote(int server,
                            std::shared_ptr<raftRpcProtoc::RequestVoteArgs> args,
                            std::shared_ptr<raftRpcProtoc::RequestVoteReply> reply,
                            std::shared_ptr<int> voteNum) {
    auto start = now();
    DPrintf("[func-sendRequestVote rf{%d}]向server{%d}发送RequestVote 开始", m_me, server);
    bool ok = m_peers[server]->RequestVote(args.get(), reply.get());
    auto costMs = std::chrono::duration_cast<std::chrono::milliseconds>(now() - start).count();
    DPrintf("func-sendRequestVote rf{%d} 向 server {%d} 发送 RequestVote完毕,耗时:{%lld} ms",
            m_me, server, static_cast<long long>(costMs));

    if(!ok){
        return ok;//Rpc通信失败就理解返回，避免资源浪费
    }
    //对回应进行处理，记住无论什么时候收到回复就检查term
    std::lock_guard<std::mutex>lg(m_mtx);
    if(reply->term() > m_currentTerm){
        //回复的term比自己的大，说明自己落后了，更新状态并退出
        m_status=Follower;
        m_currentTerm=reply->term();
        m_votedFor=-1;//重置投票
        persist();//持久化当前状态
        return true;//这里的return true代表RPC调用成功并且响应回来
    }else if(reply->term() < m_currentTerm){
        return true;
    }

    myAssert(reply->term()==m_currentTerm,format("assert {rply.Term==rf.currentTerm} fail"));//判断响应的raft节点的任期和自己是否一样
    if(!reply->votegranted()){
        //这个节点因为某些原因没给本节点投票，结束该函数
        return true;
    }
    *voteNum = *voteNum + 1;
    if(*voteNum>=m_peers.size()/2+1){
        //Raft领导选举机制，超过半数节点投票就自动成为领导
        *voteNum=0;//设置为0避免重复当选
        if(m_status==Leader){
            //如果已经是Leader了  那么就不处理
            myAssert(false,format("[func-sendrequestVote-rf{%d}] term:{%d} 同一个term当两次领导",m_me,m_currentTerm));
        }
        //如果是第一次当选Leader 那么就需要初始化状态和nextIndex,matchIndex
        m_status=Leader;
        DPrintf("func-sendRequestVote rf{%d} elect success, current term:{%d},lastLogIndex:{%d}",m_me,m_currentTerm,getLastLogIndex());
        int lastLogIndex=getLastLogIndex();
        for(int i=0;i<m_peers.size();i++){
            m_nextIndex[i]=lastLogIndex+1;//有效下标从1开始
            m_matchIndex[i]=0;//表示已经接受到领导日志的index下标，并且换一次领导就要设置为0
            m_inflightCount[i]=0;  // Pipeline：重置在途计数
            m_inSnapshot[i]=false;          // Pipeline：重置快照状态
            // Pipeline: 清空该 Peer 的批次队列和窗口
            {
                std::lock_guard<std::mutex> lock(*m_peerPipelineMutex[i]);
                while (!m_peerPipeline[i].pending_batches.empty()) {
                    m_peerPipeline[i].pending_batches.pop();
                }
                m_peerPipeline[i].in_flight.clear();
                m_peerPipeline[i].next_batch_id = 0;
            }
        }
        persist();
        
        // 启动批量发送线程（Leader 批量追加优化）
        startBatchSenderThread();
        
        // Pipeline: 暂时禁用，调试选举问题
        // TODO: 修复选举问题后重新启用 Pipeline
        // for (int i = 0; i < m_peers.size(); ++i) {
        //     if (i == m_me) continue;
        //     startPeerPipelineThread(i);
        // }
    }
    return true;
}

//Request用于响应sendRequestVote,是否投票给他让其成为Leader
// 处理其他节点的投票请求（被请求方视角）
void Raft::RequestVote1(const raftRpcProtoc::RequestVoteArgs* args, raftRpcProtoc::RequestVoteReply* reply) {
    std::lock_guard<std::mutex> lg(m_mtx); // 保证状态修改线程安全
    // DEFERP宏：函数退出前执行persist（需确保DEFERP是“延迟执行”宏，如未定义可替换为析构类）
    struct DeferPersist {
        Raft* raft;
        DeferPersist(Raft* r) : raft(r) {}
        ~DeferPersist() { raft->persist(); } // 析构时执行persist，替代DEFERP
    } defer_persist(this);

    // ===== 规则1：任期校验（Raft核心，任期小的请求直接拒绝）=====
    if (args->term() < m_currentTerm) {
        reply->set_term(m_currentTerm);       // 返回自身任期，告知候选者其任期过期
        reply->set_votestate(Expire);          // 投票状态：请求过期
        reply->set_votegranted(false);        // 拒绝投票（修正拼写错误：votegrandted→votegranted）
        DPrintf("[RequestVote1] rf{%d} 拒绝投票：候选者term{%d} < 自身term{%d}", m_me, args->term(), m_currentTerm);
        return;
    }

    // ===== 规则2：请求任期更大 → 降级为Follower并重置状态 =====
    if (args->term() > m_currentTerm) {
        m_status = Follower;                  // 无论当前是Candidate/Leader，都降级为Follower
        m_currentTerm = args->term();         // 更新自身任期到最新
        m_votedFor = -1;                      // 重置投票状态（当前任期未投票）
        DPrintf("[RequestVote1] rf{%d} 发现更高term{%d}，降级为Follower并重置投票", m_me, args->term());
    }

    // 断言：经过上面两步，请求term必然等于自身term（防御性编程）
    myAssert(args->term() == m_currentTerm, 
             format("func--rf{%d} 任期校验失败：args.Term{%d} != rf.currentTerm{%d}", m_me, args->term(), m_currentTerm));

    // ===== 规则3：日志完整性校验（仅给日志≥自身的候选者投票）=====
    int selfLastLogIndex = getLastLogIndex();  // 自身最后一条日志的索引（修正：用户之前错写为getLastLogIndex赋值给lastLogTerm）
    int selfLastLogTerm = getLastLogTerm();    // 自身最后一条日志的任期
    bool candidateLogUpToDate = UPtodata(args->lastlogindex(), args->lastlogterm()); // 候选者日志是否最新

    if (!candidateLogUpToDate) {
        // 填充缺失逻辑：日志不满足 → 拒绝投票，输出调试日志
        DPrintf("[RequestVote1] rf{%d} 拒绝投票：候选者日志更旧 | 候选者(lastIdx:%d, lastTerm:%d) 自身(lastIdx:%d, lastTerm:%d)",
                m_me, args->lastlogindex(), args->lastlogterm(), selfLastLogIndex, selfLastLogTerm);
        reply->set_term(m_currentTerm);
       reply->set_votestate(Vted);             // 投票状态：本轮已投票（或直接Expire，核心是拒绝）
        reply->set_votegranted(false);
        return;
    }

    // ===== 规则4：同一任期内最多投一票 =====
    if (m_votedFor != -1 && m_votedFor != args->candidateid()) {
        // 已投给其他候选者（可能是网络重发请求）→ 拒绝
        DPrintf("[RequestVote1] rf{%d} 拒绝投票：本轮已投给节点{%d}，候选者{%d}请求投票", m_me, m_votedFor, args->candidateid());
        reply->set_term(m_currentTerm);
        reply->set_votestate(Vted);
        reply->set_votegranted(false);
        return;
    }

    // ===== 满足所有条件 → 投给该候选者 =====
    m_votedFor = args->candidateid();        // 记录本轮投票对象
    m_lastResetElectionTime = now();          // 重置选举定时器（避免自己超时发起选举，修正拼写错误：m_lastResetEelectionTime）
    reply->set_term(m_currentTerm);
    reply->set_votestate(Normal);             // 投票状态：正常投票
    reply->set_votegranted(true);             // 同意投票
    DPrintf("[RequestVote1] rf{%d} 同意投票给候选者{%d}，term{%d}", m_me, args->candidateid(), m_currentTerm);
    return;
}
//=====raftRpcProtoc::raftRpc rpc接口的具体实现
void Raft::AppendEntries(::google::protobuf::RpcController* controller,
                         const raftRpcProtoc::AppendEntriesArgs* request,
                         raftRpcProtoc::AppendEntriesReply* response,
                         ::google::protobuf::Closure* done) {
    (void)controller;  // 当前不使用 controller
    AppendEntries1(request, response);
    if (done) {
        done->Run();
    }
}
void Raft::RequestVote(::google::protobuf::RpcController* controller,
                       const raftRpcProtoc::RequestVoteArgs* request,
                       raftRpcProtoc::RequestVoteReply* response,
                       ::google::protobuf::Closure* done) {
    (void)controller;
    RequestVote1(request, response);
    if (done) {
        done->Run();
    }
}
void Raft::InstallSnapshot(::google::protobuf::RpcController* controller,
                           const raftRpcProtoc::InstallSnapshotRequest* request,
                           raftRpcProtoc::InstallSnapshotResponse* response,
                           ::google::protobuf::Closure* done) {
    (void)controller;
    InstallSnapshot1(request, response);
    if (done) {
        done->Run();
    }
}

//日志复制与心跳机制
//负责查看是否该发送该心跳，如果该发起就执行doHearBeat  ---- 其实就是检验是不是到时间了
void Raft::leaderHearBeatTicker(){
    while(true){
        while(m_status!=Leader){
            //避免不是Leader CPU空转  浪费资源 而且还要拿锁 让他们继续睡
            usleep(1000*HeartBeatTimeout);
        }
        static std::atomic<int32_t>atomicCount;//静态对象默认为0
        //表示当前线程需要睡眠的时间，计算方式基于心跳超时时间和上一次心跳重置时间m_lastresethearBeatTimeout
        //目的，用于动态太癌症睡眠时间，避免线程频繁检查状态导致CPU空转
        std::chrono::duration<signed long int,std::ratio<1,1000000000>>suitableSleepTime{};
        std::chrono::system_clock::time_point weakTime{};
        {
            std::lock_guard<std::mutex>lg(m_mtx);
            weakTime=now();
            // 注意：这里必须基于上一次“心跳重置时间”（m_lastResetHearBeatTime）来计算下一次心跳的睡眠时长，
            // 不能继续用 m_lastResetElectionTime，否则随着时间推移 suitableSleepTime 会变成负数，
            // 导致 leaderHearBeatTicker 不再睡眠、疯狂触发 doHeartBeat，最终创建海量线程耗尽系统资源。
            suitableSleepTime=std::chrono::milliseconds(HeartBeatTimeout)+m_lastResetHearBeatTime-weakTime;
        }
        if(std::chrono::duration<double,std::milli>(suitableSleepTime).count()>1){
            //说明此时还没到发心跳的时间 继续睡眠
            std::cout<<atomicCount <<"\033[1;35m leaderHearBeatTicker();函数设置睡眠时间为]"
                                    <<std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count()<<"毫秒\033[0m"
                                    <<std::endl;
            //获取当前时间点
            auto start=std::chrono::steady_clock::now();
            auto sleepUs = std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count();
            usleep(static_cast<useconds_t>(sleepUs));
            auto end=std::chrono::steady_clock::now();
            //计算时间差值单位是毫秒
            std::chrono::duration<double,std::milli>duration=end-start;
            //使用ANSI控制序列将输出颜色改为紫色
            std::cout<<atomicCount<<"\033[1;35m leaderHearBeatTicker();函数实际睡眠时间为："<<duration.count()
                                  <<"毫秒\033[0m"<<std::endl;
            ++atomicCount;//打印睡眠时间
        }
        if(std::chrono::duration<double,std::milli>(m_lastResetHearBeatTime-weakTime).count()>0){
            continue;//睡眠这段时间有重置心跳计时器，不触发心跳
        }
        doHeartBeat();  // <<< 这里真正发心跳/AppendEntries
    }
}

void Raft::doHeartBeat() {
    std::lock_guard<std::mutex> g(m_mtx);
    if (m_status != Leader) {
        return; // 非Leader状态直接返回，避免无效操作
    }

    DPrintf("[func-Raft::doHeartBeat()-Leader:{%d}] 触发心跳，开始向所有追随者发送消息\n", m_me);
    auto successCount = std::make_shared<int>(1); // 统计成功响应的追随者数量（初始包含自身）

    // 遍历所有追随者，分别处理快照发送或日志发送
    for (int peerId = 0; peerId < m_peers.size(); ++peerId) {
        if (peerId == m_me) {
            continue; // 跳过自身
        }

        DPrintf("[func-Raft::doHeartBeat()-Leader:{%d}] 处理追随者[%d]的心跳/日志同步\n", m_me, peerId);

        // 防御：nextIndex 最小为 1
        if (m_nextIndex[peerId] < 1) {
            DPrintf("[func-Raft::doHeartBeat()-Leader:{%d}] 修正异常 nextIndex[%d]=%d 为 1\n",
                    m_me, peerId, m_nextIndex[peerId]);
            m_nextIndex[peerId] = 1;
        }

        // 根据nextIndex判断需要发送快照还是日志条目
        if (m_nextIndex[peerId] <= m_lastSnapshotIncludeIndex) {   // ★ 建议用 <=
            startSnapshotSendThread(peerId, successCount);
        } else {
            startLogEntriesSendThread(peerId, successCount);
        }
    }

    // 重置Leader自身的心跳计时器
    m_lastResetHearBeatTime = now();
    DPrintf("[func-Raft::doHeartBeat()-Leader:{%d}] 完成所有追随者消息发送，重置心跳时间\n", m_me);
}

// 辅助函数：创建线程向指定追随者发送快照
void Raft::startSnapshotSendThread(int peerId, std::shared_ptr<int> successCount) {
    // 捕获当前锁保护下的快照相关参数（避免线程执行时数据被修改）
    int snapshotIndex = m_lastSnapshotIncludeIndex;
    int snapshotTerm = m_lastSnapshotIncludeTerm;
    std::string snapshotData = m_persister->ReadSnapshot(); // 假设从持久化组件获取快照数据

    // 创建线程发送快照，分离线程避免阻塞
    std::thread([this, peerId, snapshotIndex, snapshotTerm, snapshotData, successCount]() {
        leaderSendSnapShot(peerId, snapshotIndex, snapshotTerm, snapshotData, successCount);
    }).detach();
}

// 辅助函数：创建线程向指定追随者发送日志条目（AppendEntries）
// 注意：批量追加优化后，这里只发送空心跳
// 日志复制完全由批量追加后台线程负责
void Raft::startLogEntriesSendThread(int peerId, std::shared_ptr<int> successCount) {
    // 心跳只发送空心跳，不发送日志条目
    // 日志复制由批量追加优化（batchSenderLoop）负责
    auto args = std::make_shared<raftRpcProtoc::AppendEntriesArgs>();
    args->set_term(m_currentTerm);
    args->set_leaderid(m_me);
    args->set_prevlogindex(m_nextIndex[peerId] - 1);
    args->set_prevlogterm(getLogTermFromLogIndex(m_nextIndex[peerId] - 1));
    args->set_leadercommit(m_commitIndex);
    args->set_batchid(0);  // 0 表示空心跳
    
    auto reply = std::make_shared<raftRpcProtoc::AppendEntriesReply>();
    reply->set_appstate(Disconnected);
    std::thread([this, peerId, args, reply, successCount]() {
        sendAppendEntries(peerId, args, reply, successCount);
    }).detach();
}

// 优化后的快照发送实现（补充参数传递）
void Raft::leaderSendSnapShot(int peerId, int snapshotIndex, int snapshotTerm, 
                             const std::string& snapshotData, std::shared_ptr<int> successCount) {
    // 构造快照请求（使用捕获的参数，避免依赖锁）
    raftRpcProtoc::InstallSnapshotRequest req;
    req.set_leaderid(m_me);
    req.set_term(m_currentTerm);
    req.set_lastsnapshotincludeindex(snapshotIndex);
    req.set_lastsnapshotincludeterm(snapshotTerm);
    req.set_snapshotdata(snapshotData);

    raftRpcProtoc::InstallSnapshotResponse resp;
    bool success = m_peers[peerId]->InstallSnapshot(&req, &resp);

    // 处理响应（更新nextIndex等逻辑，原逻辑保持不变）
    if (success) {
        std::lock_guard<std::mutex> g(m_mtx);
        if (resp.term() > m_currentTerm) {
            // 发现更高term，降级为Follower
            m_status = Follower;
            m_currentTerm = resp.term();
            m_votedFor = -1;
            persist();
            return;
        }
        // 快照发送成功，更新该追随者的nextIndex
        m_nextIndex[peerId] = snapshotIndex + 1;
        m_matchIndex[peerId] = snapshotIndex;
        *successCount += 1;
        leaderUpdateCommitIndex(); // 尝试更新提交索引
    }
}

//sendAppendEntries
//原卡哥的实现 我感觉逻辑不是很清晰 没有很好的体现Raft共识算法的心跳-发送日志-匹配日志-状态变更的算法思想
/*
bool Raft::sendAppendEntries(int peerId, std::shared_ptr<raftRpcProtoc::AppendEntriesArgs> args,
                            std::shared_ptr<raftRpcProtoc::AppendEntriesReply> reply,
                            std::shared_ptr<int> appendNums) {
    DPrintf("func-Raft::sendAppendEntries()-Leader:{%d} Leader 向节点{%d}发送AE RPC开始，args->entries_size():{%d}",m_me,peerId,args->entries_size());

    //调用RPC开始
    bool ok=m_peers[peerId]->AppendEntries(args.get(),reply.get());
    if(!ok){
        //RPC调用失败（例如网络问题）
        DPrintf("[func-Raft::sendAppendEntries()-raft{%d} leader 向节点 {%d}发送  AE RPC失败]",m_me,peerId);
        return ok;
    }
    if(reply->appstate()==Disconnected){
        //RPC调用成功 但追随者因网络分区或其他原因未能处理请求  也就是超时了 或者是其他的状态值返回了
        return ok;
    }

    //检查返回的Term以维持日志的一致性
    if(reply->term()>m_currentTerm){
        //退回Follower状态
        m_status=Follower;
        m_currentTerm=reply->term();
        m_votedFor=-1;
        persist();
        return ok;
    }else if(reply->term()<m_currentTerm){
        //说明对方过时了 那么就需要在返回值中写入目前最新的term以及日志等等 或者是调用一个日志同步的接口 去跟当前的server对接 看看其到底丢失多少日志
        //然后给他
        //这里直接忽略----卡哥这里
        DPrintf("[func-sendAppendEntries rf{%d} 节点{%d}的term{%d} < rf {%d}的term{%d}\n]",m_me,peerId,reply->term(),m_me,m_currentTerm);
        return ok;
    }
    if(m_status!=Leader){
        //如果当前的节点不在是Leader了 则无需进行进一步的回应
        return ok;
    }
    myAssert(reply->term()==m_currentTerm,format("reply.Term{%d}!=rf.currentTerm{%d}",reply->term(),m_currentTerm));
    if(!reply->success()){
        //日志不匹配，调整nextindex继续尝试
            if(reply->updatenextindex()!=-100){
                //-100是特殊的标记，用于优化Leader的回退逻辑
                DPrintf("[func-sendAppendEntries rf{%d} 返回的日志term相等 但不匹配  回退nextindex[%d]:{%d}\n]",m_me,server,reply->updatenextindex());
                m_nextIndex[server]=reply->updatenextindex();//使用追随者发回来的nextindex  减少不必要的重试
            }
        }else{
            //日志匹配，更新appendnums 和相关索引
            *appendNums=*appendNums+1;//表示有一个追随者接受了日志或者是心跳
            DPrintf("----------temp ------------{%d}",*appendNums);
            //更新matchindex 和 nextindex
            m_matchIndex[server]=std::max(m_matchIndex[server],args->prevlogindex()+args->entries_size());
            m_nextIndex[server]=m_matchIndex[server]+1;

            int lastLogIndex=getlastLogIndex();
            myAssert(m_matchIndex[server]<=lastLogIndex+1,format("m_matchIndex[%d]>lastLogIndex[%d]",m_matchIndex[server],lastLogIndex));
            //检查日志是否可提交 即是否得到了多数票
            if(*appendNums>=m_peers.size()/2+1){
                *appendNums=0;//避免重复提交【多线程】
                if(args->entries_size()>0){
                    DPrintf("args->entries_size():{%d}",args->entries_size());
                }

                //只有当前term的日志被大多数追随者接受了 才能提交
                if(args->entries_size()>0 && args->entries(args->entries_size()-1).logterm()==m_currentTerm){
                    DPrintf("-------------日志提交成功");
                    m_commitIndex=std::max(m_commitIndex,args->prevlogindex()+args->entries_size());
                }
                myAssert(m_commitIndex<=lastLogIndex,format("m_commitIndex[%d]>lastLogIndex[%d]",m_commitIndex,lastLogIndex));
            }
            
        }
        return ok;
    }

*/

//下面是我抽象出来的逻辑 保证每一个函数处理一个问题  逻辑清晰
// ===================== 主函数：仅做流程调度，无嵌套逻辑 =====================
bool Raft::sendAppendEntries(int peerId, 
                            std::shared_ptr<raftRpcProtoc::AppendEntriesArgs> args,
                            std::shared_ptr<raftRpcProtoc::AppendEntriesReply> reply,
                            std::shared_ptr<int> appendNums) {
    // 子函数1：执行RPC调用 + 网络状态检查
    if (!callAppendEntriesRpc(peerId, args, reply)) {
        return false;
    }

    // 子函数2：处理任期不匹配（降级/忽略），返回true表示需终止后续逻辑
    std::lock_guard<std::mutex> lg(m_mtx); // 加锁保护共享状态（原逻辑隐含锁，需显式加）
    if (handleTermMismatch(peerId,reply->term())) {
        return true;
    }

    // 子函数3：检查Leader角色合法性
    if (!isStillLeader()) {
        return true;
    }

    // 断言：任期必须一致（原逻辑的防御性检查）
    myAssert(reply->term() == m_currentTerm, 
             format("reply.Term{%d}!=rf.currentTerm{%d}", reply->term(), m_currentTerm));

    // 子函数4：处理日志不匹配（success=false）
    if (!reply->success()) {
        handleLogMismatch(peerId, reply);
        return true;
    }

    // 子函数5：处理日志匹配（success=true），更新同步状态+计数
    updatePeerSyncState(peerId, args, appendNums);

    // 子函数6：检查多数派条件，尝试更新commitIndex
    tryUpdateCommitIndex(args, appendNums);

    return true;
}

// ===================== 子函数1：封装RPC调用 + 网络状态检查 =====================
bool Raft::callAppendEntriesRpc(int peerId, 
                               std::shared_ptr<raftRpcProtoc::AppendEntriesArgs> args,
                               std::shared_ptr<raftRpcProtoc::AppendEntriesReply> reply) {
    DPrintf("func-Raft::sendAppendEntries()-Leader:{%d} Leader 向节点{%d}发送AE RPC开始，args->entries_size():{%d}",
            m_me, peerId, args->entries_size());

    // 执行RPC调用
    bool ok = m_peers[peerId]->AppendEntries(args.get(), reply.get());
    if (!ok) {
        DPrintf("[func-Raft::sendAppendEntries()-raft{%d} leader 向节点 {%d}发送 AE RPC失败]", m_me, peerId);
        return false;
    }

    // 检查网络状态（Disconnected则终止）
    if (reply->appstate() == Disconnected) {
        return false;
    }

    return true;
}

// ===================== 子函数2：处理任期不匹配（Raft核心：任期优先） =====================
bool Raft::handleTermMismatch(int peerId, int  replyTerm) {
    // 情况1：对方term更大 → 降级为Follower
    if (replyTerm > m_currentTerm) {
        m_status = Follower;
        m_currentTerm = replyTerm;
        m_votedFor = -1;
        persist();
        return true; // 终止后续逻辑
    }

    // 情况2：对方term更小 → 忽略，终止后续逻辑
    if (replyTerm < m_currentTerm) {
        DPrintf("[func-sendAppendEntries rf{%d} 节点{%d}的term{%d} < rf {%d}的term{%d}\n]",
                m_me, peerId, replyTerm, m_me, m_currentTerm);
        return true; // 终止后续逻辑
    }

    // 情况3：任期一致 → 继续后续逻辑
    return false;
}

// ===================== 子函数3：检查Leader角色合法性 =====================
bool Raft::isStillLeader() {
    if (m_status != Leader) {
        return false; // 非Leader，终止后续逻辑
    }
    return true;
}

// ===================== 子函数4：处理日志不匹配（回退nextIndex） =====================
void Raft::handleLogMismatch(int peerId, std::shared_ptr<raftRpcProtoc::AppendEntriesReply> reply) {
    // -100是卡哥定义的特殊标记，跳过无效回退
    if (reply->updatenextindex() != -100) {
        int nextIdx=reply->updatenextindex();
        //防御性下界：Raft日志从Index从1开始，0谁prevelogindex的虚拟值
        if(nextIdx<1){
            nextIdx=1;
        }
        //如果已经已经有快照了  nextIndex至少要指向快照之后的第一条日志
        if(nextIdx<=m_lastSnapshotIncludeIndex){
            nextIdx=m_lastSnapshotIncludeIndex+1;
        }
        DPrintf("[func-sendAppendEntries rf{%d} 返回的日志term相等 但不匹配  回退nextindex[%d]:{%d}\n]",
                m_me, peerId, nextIdx);
        m_nextIndex[peerId] = nextIdx; // 用追随者返回的nextIndex优化重试
    }
}

// ===================== 子函数5：更新节点同步状态（日志匹配成功） =====================
void Raft::updatePeerSyncState(int peerId, 
                              std::shared_ptr<raftRpcProtoc::AppendEntriesArgs> args,
                              std::shared_ptr<int> appendNums) {
    // 1. 增加成功同步的追随者计数
    *appendNums = *appendNums + 1;
    DPrintf("----------temp ------------{%d}", *appendNums);

    // 2. 更新matchIndex和nextIndex（原逻辑的max保护）
    int newMatchIndex = args->prevlogindex() + args->entries_size();
    m_matchIndex[peerId] = std::max(m_matchIndex[peerId], newMatchIndex);
    m_nextIndex[peerId] = m_matchIndex[peerId] + 1;

    // 3. Pipeline: 只有批次中有日志条目时才触发窗口滑动
    if (args->entries_size() > 0) {
        DPrintf("[Pipeline] rf{%d} peer{%d} matchIndex updated to %d, triggering window slide", 
                m_me, peerId, m_matchIndex[peerId]);
        handlePipelineResponse(peerId, m_matchIndex[peerId]);
    }

    // 4. 防御性断言（原逻辑保留）
    int lastLogIndex = getLastLogIndex();
    myAssert(m_matchIndex[peerId] <= lastLogIndex + 1,
             format("m_matchIndex[%d]>lastLogIndex[%d]", m_matchIndex[peerId], lastLogIndex));
}

// ===================== 子函数6：检查多数派条件，尝试更新commitIndex（Raft核心） =====================
void Raft::tryUpdateCommitIndex(std::shared_ptr<raftRpcProtoc::AppendEntriesArgs> args,
                               std::shared_ptr<int> appendNums) {
    // 1. 检查是否达到多数派（原逻辑：m_peers.size()/2 +1）
    if (*appendNums < m_peers.size() / 2 + 1) {
        return; // 未达多数，无需更新
    }

    // 2. 重置计数，避免多线程重复提交（原逻辑）
    *appendNums = 0;

    // 3. 仅提交「当前任期」的日志（Raft安全性规则）
    if (args->entries_size() > 0 && args->entries(args->entries_size() - 1).logterm() == m_currentTerm) {
        DPrintf("-------------日志提交成功");
        int newCommitIndex = args->prevlogindex() + args->entries_size();
        m_commitIndex = std::max(m_commitIndex, newCommitIndex);
    }

    // 4. 防御性断言（原逻辑保留）
    int lastLogIndex = getLastLogIndex();
    myAssert(m_commitIndex <= lastLogIndex,
             format("m_commitIndex[%d]>lastLogIndex[%d]", m_commitIndex, lastLogIndex));
}

//AppendEntreis函数
//installsnapshot1函数
void Raft::InstallSnapshot1(const raftRpcProtoc::InstallSnapshotRequest* request,
                            raftRpcProtoc::InstallSnapshotResponse* response){
    std::lock_guard<std::mutex> lg(m_mtx);
    //1.任期处理  和其他RPC一样
    if(request->term() < m_currentTerm){
        //对方任期落后，直接拒绝，带上自己的term
        response->set_term(m_currentTerm);
        return;
    }
    if(request->term() > m_currentTerm){
        //更新状态 以及 自己的投票等等
        m_status=Follower;
        m_currentTerm=request->term();
        m_votedFor=-1;
        persist();
    }
    //2.更新快照元信息（包含最后的Index以及Term等）
    int snapIndex=request->lastsnapshotincludeindex();
    int snapTerm=request->lastsnapshotincludeterm();
    
    //如果这是一个更靠后的快照 才更新本地
    if(snapIndex >m_lastSnapshotIncludeIndex){
        m_lastSnapshotIncludeIndex=snapIndex;
        m_lastSnapshotIncludeTerm=snapTerm;

        //提交/已应用至少到快照点
        if(m_commitIndex < m_lastSnapshotIncludeIndex){
            m_commitIndex=m_lastSnapshotIncludeIndex;
        }
        if(m_lastApplied < m_lastSnapshotIncludeIndex){
            m_lastApplied=m_lastSnapshotIncludeIndex;
        }
        // TODO：后续可以在这里根据 snapIndex 截断 m_logs，
        // 保证快照前的日志不再保留（现在先不动，等验证完多节点再精细优化）
    }
    std::string raftState=persistDate();
    long long seq=m_persister->Save(raftState,request->snapshotdata());
    m_persister->WaitFlushed(seq);
    //4.返回当前的term
    response->set_term(m_currentTerm);
}
//函数理解链接  ：： https://www.doubao.com/thread/w4d7fb3d8d0c6b440
void Raft::AppendEntries1(const raftRpcProtoc::AppendEntriesArgs* args,
                          raftRpcProtoc::AppendEntriesReply* reply) {
    std::lock_guard<std::mutex> locker(m_mtx);
    reply->set_appstate(AppNormal);  // 能接收到代表网络是正常的

    if (args->term() < m_currentTerm) {
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        reply->set_updatenextindex(-100);  // 让 leader 能够快速回退 nextIndex
        DPrintf("[func-AppendEntries-rf{%d}] 拒绝了 因为Leader{%d}的term{%d}< rf{%d}.term{%d}\n",
                m_me, args->leaderid(), args->term(), m_me, m_currentTerm);
        return;
    }

    // 作用域结束时自动调用 persist()，替代原来的 DEFER 宏
    struct PersistGuard {
        Raft* rf;
        explicit PersistGuard(Raft* r) : rf(r) {}
        ~PersistGuard() { rf->persist(); }
    } guard(this);

    if (args->term() > m_currentTerm) {
        m_status    = Follower;
        m_currentTerm = args->term();
        m_votedFor  = -1;
    }
    myAssert(args->term() == m_currentTerm,
             format("assert {args.Term == rf.currentTerm} fail"));

    // candidate 收到同一 term 的 leader 消息，也要变成 Follower
    m_status = Follower;
    m_lastResetElectionTime = now();

    // 情况1：prevlogindex 超过本地日志范围
    if (args->prevlogindex() > getLastLogIndex()) {
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        reply->set_updatenextindex(getLastLogIndex() + 1);
        return;
    } else if (args->prevlogindex() < m_lastSnapshotIncludeIndex) {
        // prevlogIndex 还没追上快照
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        reply->set_updatenextindex(m_lastSnapshotIncludeIndex + 1);
        return;//必须立即返回，不能在走后面的matchlog/mismatch逻辑
    }

    // 日志匹配与否
    if (matchLog(args->prevlogindex(), args->prevlogterm())) {
        // Leader 发送的 entries 和本地逐条对比
        for (int i = 0; i < args->entries_size(); ++i) {
            auto log = args->entries(i);
            if (log.logindex() > getLastLogIndex()) {
                // 超出当前日志末尾，直接 push_back
                m_logs.push_back(log);
            } else {
                int sliceIdx = getSlicesIndexFromLogIndex(log.logindex());
                auto& local  = m_logs[sliceIdx];

                if (local.logterm() == log.logterm() &&
                    local.command() != log.command()) {
                    myAssert(false,
                             format("[func-AppendEntries-rf{%d}] 两节点logIndex{%d}和term{%d}相同，但是command不同",
                                    m_me, log.logindex(), log.logterm()));
                }
                if (local.logterm() != log.logterm()) {
                    local = log; // term 不同，用 leader 的那条覆盖
                }
            }
        }

        myAssert(
            getLastLogIndex() >= args->prevlogindex() + args->entries_size(),
            format("[func-AppendEntries1-rf{%d}]rf.getLastLogIndex(){%d} != args.PrevLogIndex{%d}+len(args.Entries){%d}",
                   m_me, getLastLogIndex(), args->prevlogindex(), args->entries_size()));

        if (args->leadercommit() > m_commitIndex) {
            m_commitIndex = std::min(args->leadercommit(), getLastLogIndex());
        }

        myAssert(getLastLogIndex() >= m_commitIndex,
                 format("[func-AppendEntries1-rf{%d}]  rf.getLastLogIndex{%d} < rf.commitIndex{%d}",
                        m_me, getLastLogIndex(), m_commitIndex));
        reply->set_success(true);
        reply->set_term(m_currentTerm);
        return;
    } else {
        // 优化：根据 term 跳跃回退 nextIndex
        reply->set_updatenextindex(args->prevlogindex());
        for (int index = args->prevlogindex(); index >= m_lastSnapshotIncludeIndex; --index) {
            if (getLogTermFromLogIndex(index) !=
                getLogTermFromLogIndex(args->prevlogindex())) {
                reply->set_updatenextindex(index + 1);
                break;
            }
        }
        reply->set_success(false);
        reply->set_term(m_currentTerm);
        return;
    }
}
struct BoostPersistLogEntry{
        int term=0;
        std::string command;
        template <class Archive>
        void serialize(Archive& ar,const unsigned int/*version*/){
            ar & term;
            ar & command;
        }
};
//持久化函数
//这里卡哥写的不清楚 我们后续需要进一步的重构
struct BoostPersistRaftNode {
    int m_currentTerm = 0;
    int m_votedFor = -1;
    int m_lastSnapshotIncludeIndex = 0;
    int m_lastSnapshotIncludeTerm = 0;
    std::vector<BoostPersistLogEntry> m_logs;

    template <class Archive>
    void serialize(Archive& ar, const unsigned int /*version*/) {
        ar & m_currentTerm;
        ar & m_votedFor;
        ar & m_lastSnapshotIncludeIndex;
        ar & m_lastSnapshotIncludeTerm;
        ar & m_logs;
    }
};

std::string Raft::persistDate() {
    BoostPersistRaftNode persistData;
    persistData.m_currentTerm = m_currentTerm;
    persistData.m_votedFor = m_votedFor;
    persistData.m_lastSnapshotIncludeIndex = m_lastSnapshotIncludeIndex;
    persistData.m_lastSnapshotIncludeTerm = m_lastSnapshotIncludeTerm;
    persistData.m_logs.clear();
    for (const auto& log : m_logs) {
        BoostPersistLogEntry e;
        e.term=log.logterm();
        e.command=log.command();
        persistData.m_logs.push_back(e);
    }

    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << persistData;
    return ss.str();
}

void Raft::readPersist(std::string data) {
    if (data.empty()) {
        return;
    }
    std::stringstream ss(std::move(data));
    boost::archive::text_iarchive ia(ss);
    BoostPersistRaftNode persistData;
    ia >> persistData;

    m_currentTerm = persistData.m_currentTerm;
    m_votedFor = persistData.m_votedFor;
    m_lastSnapshotIncludeIndex = persistData.m_lastSnapshotIncludeIndex;
    m_lastSnapshotIncludeTerm = persistData.m_lastSnapshotIncludeTerm;

    m_logs.clear();
    int index = m_lastSnapshotIncludeIndex;
    for (const auto& plog : persistData.m_logs) {
        ++index;
        raftRpcProtoc::LogEntry entry;
        entry.set_logindex(index);
        entry.set_logterm(plog.term);//使用持久化记录的term
        entry.set_command(plog.command);//使用持久化记录的command
        m_logs.push_back(entry);
    }
}

void Raft::persist() {
   std::string data = persistDate();
    long long seq = m_persister->SaveRaftState(data);
    m_persister->WaitFlushed(seq);
}

// ==================== Pipeline 优化实现 ====================

// 检查是否可以发送新的 Pipeline 批次
bool Raft::canSendPipelineBatch(int peerId) {
    std::lock_guard<std::mutex> lock(*m_peerPipelineMutex[peerId]);
    return static_cast<int>(m_peerPipeline[peerId].in_flight.size()) < PIPELINE_MAX_INFLIGHT;
}

// 检查是否需要强制 flush（窗口快满时）
bool Raft::shouldForceFlushPipeline(int peerId) {
    std::lock_guard<std::mutex> lock(*m_peerPipelineMutex[peerId]);
    return static_cast<int>(m_peerPipeline[peerId].in_flight.size()) >= PIPELINE_MAX_INFLIGHT * 8 / 10;  // 80% 时强制
}

// 创建 Pipeline 批次
Raft::PipelineBatch Raft::createPipelineBatch(int peerId, int startIndex, int endIndex) {
    PipelineBatch batch;
    {
        std::lock_guard<std::mutex> lock(*m_peerPipelineMutex[peerId]);
        batch.batch_id = m_peerPipeline[peerId].next_batch_id++;
    }
    batch.first_index = startIndex;
    batch.last_index = endIndex;
    batch.send_time_ms = getCurrentTimeMillis();
    batch.entry_count = endIndex - startIndex + 1;
    batch.matched = false;
    return batch;
}

// 发送 Pipeline 批次到指定 Peer
void Raft::sendPipelineBatch(int peerId, const PipelineBatch& batch) {
    DPrintf("[Pipeline] rf{%d} peer{%d} sending batch %lld [idx %d-%d], entries: %d",
            m_me, peerId, batch.batch_id, batch.first_index, batch.last_index, batch.entry_count);
    
    // 构造 AppendEntries 请求
    auto args = std::make_shared<raftRpcProtoc::AppendEntriesArgs>();
    
    // 计算 prevLogInfo
    int prevLogIndex = batch.first_index - 1;
    int prevLogTerm = getLogTermFromLogIndex(prevLogIndex);
    
    args->set_term(m_currentTerm);
    args->set_leaderid(m_me);
    args->set_prevlogindex(prevLogIndex);
    args->set_prevlogterm(prevLogTerm);
    args->set_leadercommit(m_commitIndex);
    args->set_batchid(batch.batch_id);  // 添加批次 ID
    
    // 填充日志条目
    int startSliceIdx = getSlicesIndexFromLogIndex(batch.first_index);
    for (int i = startSliceIdx; i < m_logs.size(); ++i) {
        *args->add_entries() = m_logs[i];
        if (m_logs[i].logindex() >= batch.last_index) {
            break;
        }
    }
    
    // 创建响应和计数器
    auto reply = std::make_shared<raftRpcProtoc::AppendEntriesReply>();
    reply->set_appstate(Disconnected);
    auto appendNums = std::make_shared<int>(0);
    
    // 发送 RPC（异步，在独立线程中处理）
    std::thread([this, peerId, batch, args, reply, appendNums]() {
        sendAppendEntries(peerId, args, reply, appendNums);
    }).detach();
    
    m_total_batches_sent++;
}

// 处理 Pipeline 响应
void Raft::handlePipelineResponse(int peerId, int matched_index) {
    slidePipelineWindow(peerId, matched_index);
}

// 滑动 Pipeline 窗口
void Raft::slidePipelineWindow(int peerId, int up_to_index) {
    std::lock_guard<std::mutex> lock(*m_peerPipelineMutex[peerId]);
    
    // 找出需要确认的批次
    std::vector<int64_t> to_remove;
    for (auto& [batch_id, batch] : m_peerPipeline[peerId].in_flight) {
        if (batch.last_index <= up_to_index) {
            to_remove.push_back(batch_id);
        }
    }
    
    // 确认并移除
    for (int64_t batch_id : to_remove) {
        m_peerPipeline[peerId].in_flight.erase(batch_id);
        m_total_batches_acked++;
    }
    
    DPrintf("[Pipeline] rf{%d} peer{%d} slide window to %d, remaining in_flight: %d", 
            m_me, peerId, up_to_index, static_cast<int>(m_peerPipeline[peerId].in_flight.size()));
}

// 重试 Pipeline 批次
void Raft::retryPipelineBatch(int peerId, int batch_id) {
    PipelineBatch batch;
    {
        std::lock_guard<std::mutex> lock(*m_peerPipelineMutex[peerId]);
        if (m_peerPipeline[peerId].in_flight.find(batch_id) != m_peerPipeline[peerId].in_flight.end()) {
            batch = m_peerPipeline[peerId].in_flight[batch_id];
        } else {
            return;  // 批次已不存在
        }
    }
    
    // 重新发送
    sendPipelineBatch(peerId, batch);
}

// 清理过期的 Pipeline 批次
void Raft::cleanupOldPipelineBatches(int peerId) {
    std::lock_guard<std::mutex> lock(*m_peerPipelineMutex[peerId]);
    
    // 清理过老的 in_flight 批次（超过一定时间的）
    int64_t now_ms = getCurrentTimeMillis();
    std::vector<int64_t> to_remove;
    
    for (auto& [batch_id, batch] : m_peerPipeline[peerId].in_flight) {
        if (now_ms - batch.send_time_ms > 5000) {  // 超过 5 秒未确认
            to_remove.push_back(batch_id);
        }
    }
    
    for (int64_t batch_id : to_remove) {
        DPrintf("[Pipeline] rf{%d} peer{%d} removing stale batch %lld", m_me, peerId, batch_id);
        m_peerPipeline[peerId].in_flight.erase(batch_id);
    }
}

// 停止 Peer 的 Pipeline
void Raft::stopPeerPipeline(int peerId) {
    {
        std::lock_guard<std::mutex> lock(*m_peerPipelineMutex[peerId]);
        // 清空待发送队列
        while (!m_peerPipeline[peerId].pending_batches.empty()) {
            m_peerPipeline[peerId].pending_batches.pop();
        }
        // 清空在途批次
        m_peerPipeline[peerId].in_flight.clear();
    }
    DPrintf("[Pipeline] rf{%d} peer{%d} pipeline stopped", m_me, peerId);
}

// 启动 Peer 的 Pipeline 发送线程
void Raft::startPeerPipelineThread(int peerId) {
    // 如果线程已存在，先分离它
    if (peerId < static_cast<int>(m_peerSendThreads.size()) && 
        m_peerSendThreads[peerId].joinable()) {
        m_peerSendThreads[peerId].detach();
    }
    
    DPrintf("[Pipeline] rf{%d} peer{%d} starting pipeline sender thread, lastLogIndex: %d", 
            m_me, peerId, getLastLogIndex());
    
    // 启动新的发送线程
    m_peerSendThreads[peerId] = std::thread(&Raft::peerPipelineSender, this, peerId);
    m_peerSendThreads[peerId].detach();
    DPrintf("[Pipeline] rf{%d} peer{%d} pipeline sender thread started", m_me, peerId);
}

// Peer Pipeline 发送线程主函数
void Raft::peerPipelineSender(int peerId) {
    DPrintf("[Pipeline] rf{%d} peer{%d} sender thread started", m_me, peerId);
    
    int consecutive_empty = 0;  // 统计连续没有新日志的次数
    
    while (true) {
        // 检查是否还是 Leader
        {
            std::lock_guard<std::mutex> lg(m_mtx);
            if (m_status != Leader) {
                DPrintf("[Pipeline] rf{%d} peer{%d} sender: not leader anymore, stopping", m_me, peerId);
                stopPeerPipeline(peerId);
                return;
            }
        }
        
        // 获取当前的 nextIndex 和 lastLogIndex
        int nextIdx;
        int lastLogIdx;
        {
            std::lock_guard<std::mutex> lg(m_mtx);
            nextIdx = m_nextIndex[peerId];
            lastLogIdx = getLastLogIndex();
        }
        
        // 检查是否有新的日志需要发送
        if (nextIdx <= lastLogIdx) {
            consecutive_empty = 0;
            
            // 计算批次大小：动态调整
            int batchSize;
            {
                std::lock_guard<std::mutex> lock(*m_peerPipelineMutex[peerId]);
                int in_flight = static_cast<int>(m_peerPipeline[peerId].in_flight.size());
                
                // 根据窗口使用情况动态调整批次大小
                if (in_flight >= PIPELINE_MAX_INFLIGHT * 8 / 10) {
                    // 窗口快满了，只发 1 条
                    batchSize = 1;
                } else if (in_flight >= PIPELINE_MAX_INFLIGHT * 5 / 10) {
                    // 窗口使用一半，发少量
                    batchSize = 4;
                } else {
                    // 窗口空闲，可以发更多
                    batchSize = PIPELINE_BATCH_SIZE;
                }
            }
            
            int endIdx = std::min(nextIdx + batchSize - 1, lastLogIdx);
            
            // 检查是否可以发送（窗口未满）
            if (canSendPipelineBatch(peerId)) {
                // 创建并发送批次
                PipelineBatch batch = createPipelineBatch(peerId, nextIdx, endIdx);
                
                // 添加到 in_flight
                {
                    std::lock_guard<std::mutex> lock(*m_peerPipelineMutex[peerId]);
                    m_peerPipeline[peerId].in_flight[batch.batch_id] = batch;
                }
                
                // 发送批次
                sendPipelineBatch(peerId, batch);
                
                // 更新 nextIndex
                {
                    std::lock_guard<std::mutex> lg(m_mtx);
                    m_nextIndex[peerId] = endIdx + 1;
                }
            }
            
            // 发送完成后短暂休眠，让响应有时间回来
            usleep(100);  // 0.1ms
            
        } else {
            // 没有新日志，增加计数器
            consecutive_empty++;
            
            // 使用指数退避策略，等待时间逐渐增加
            int wait_us;
            if (consecutive_empty <= 3) {
                wait_us = 500;       // 前3次：0.5ms
            } else if (consecutive_empty <= 10) {
                wait_us = 1000;       // 接下来：1ms
            } else if (consecutive_empty <= 30) {
                wait_us = 2000;       // 接下来：2ms
            } else {
                wait_us = 5000;       // 之后：5ms（最多）
            }
            
            usleep(wait_us);
            
            // 定期清理过期批次（每 50 次检查一次）
            if (consecutive_empty % 50 == 0) {
                cleanupOldPipelineBatches(peerId);
            }
        }
    }
}

// ==================== Pipeline 优化实现结束 ====================



void Raft::Start(const Op& command, int* newLogIndex, bool* isLeader) {
    std::lock_guard<std::mutex> lg(m_mtx);

    // 1. 如果当前不是 Leader，直接告知调用方
    if (m_status != Leader) {
        if (newLogIndex) *newLogIndex = -1;
        if (isLeader)    *isLeader    = false;
        return;
    }

    // 2. 构造新的日志条目
    int index = getLastLogIndex() + 1;

    raftRpcProtoc::LogEntry entry;
    entry.set_logterm(m_currentTerm);
    entry.set_logindex(index);

    // 使用 encodeOp 将 Op 编成字符串，放到 LogEntry::command 里
    std::string cmd = encodeOp(command);
    entry.set_command(cmd);

    // 3. 追加到本地日志
    m_logs.push_back(entry);

    // 4. 更新返回值
    if (newLogIndex) *newLogIndex = index;
    if (isLeader)    *isLeader    = true;

    // 5. 持久化（term/votedFor/logs 等）
    persist();

    // 6. 添加到批量缓冲区（Pipeline 优化核心）
    // 注意：这里不立即触发发送，而是由后台线程批量发送
    addToPendingBatch(entry);
}

// ==================== Leader 批量追加优化实现 ====================

// 添加日志到批量缓冲区
void Raft::addToPendingBatch(const raftRpcProtoc::LogEntry& entry) {
    // 为每个 Peer 添加相同的日志条目
    std::lock_guard<std::mutex> lock(m_batchMtx);
    for (size_t i = 0; i < m_pendingEntries.size(); ++i) {
        if (i == m_me) continue;  // 跳过自己
        auto& pending = m_pendingEntries[i];
        
        // 如果是第一个条目，记录时间戳
        if (pending.entries.empty()) {
            pending.first_entry_time_ms = getCurrentTimeMillis();
        }
        
        pending.entries.push_back(entry);
    }
}

// 启动批量发送线程
void Raft::startBatchSenderThread() {
    // 已经是 Leader 了，启动批量发送
    if (!m_batchRunning.load()) {
        m_batchRunning.store(true);
        m_batchSenderThread = std::thread(&Raft::batchSenderLoop, this);
        m_batchSenderThread.detach();
    }
}

// 停止批量发送线程
void Raft::stopBatchSenderThread() {
    m_batchRunning.store(false);
    m_batchCv.notify_all();
}

// 批量发送循环
void Raft::batchSenderLoop() {
    while (m_batchRunning.load()) {
        // 首先检查是否还是 Leader
        bool isLeader = false;
        int currentTerm = 0;
        {
            std::lock_guard<std::mutex> lg(m_mtx);
            isLeader = (m_status == Leader);
            currentTerm = m_currentTerm;
        }
        
        if (!isLeader) {
            // 不是 Leader 了，停止
            break;
        }
        
        std::vector<std::pair<int, std::vector<raftRpcProtoc::LogEntry>>> toSend;
        
        // 检查是否有待发送的条目
        {
            std::lock_guard<std::mutex> lock(m_batchMtx);
            int64_t now_ms = getCurrentTimeMillis();
            
            for (size_t i = 0; i < m_pendingEntries.size(); ++i) {
                if (i == m_me) continue;
                
                auto& pending = m_pendingEntries[i];
                if (!pending.entries.empty()) {
                    // 检查是否达到 flush 条件
                    bool shouldFlush = false;
                    
                    // 条件1: 缓冲区已满
                    if (pending.entries.size() >= static_cast<size_t>(PIPELINE_BATCH_SIZE)) {
                        shouldFlush = true;
                    }
                    // 条件2: 超时（5ms）
                    else if (getCurrentTimeMillis() - pending.first_entry_time_ms >= PIPELINE_MAX_DELAY_MS) {
                        shouldFlush = true;
                    }
                    // 条件3: 只有1条日志但等待时间超过2ms，也可以发送
                    // （确保低延迟，同时有批量效果）
                    else if (pending.entries.size() >= 1 &&
                            getCurrentTimeMillis() - pending.first_entry_time_ms >= 2) {
                        shouldFlush = true;
                    }
                    
                    if (shouldFlush) {
                        toSend.push_back({static_cast<int>(i), std::move(pending.entries)});
                        pending.entries.clear();
                    }
                }
            }
        }
        
        // 发送收集到的批次
        for (auto& [peerId, entries] : toSend) {
            if (entries.empty()) continue;
            
            // 再次检查 Leader 状态
            {
                std::lock_guard<std::mutex> lg(m_mtx);
                if (m_status != Leader) {
                    // Leader 切换了，丢弃这个批次
                    continue;
                }
            }
            
            // 构造 AppendEntries 请求
            auto args = std::make_shared<raftRpcProtoc::AppendEntriesArgs>();
            int prevLogIndex = entries.front().logindex() - 1;
            
            args->set_term(m_currentTerm);
            args->set_leaderid(m_me);
            args->set_prevlogindex(prevLogIndex);
            args->set_prevlogterm(getLogTermFromLogIndex(prevLogIndex));
            args->set_leadercommit(m_commitIndex);
            args->set_batchid(entries.front().logindex());
            
            for (auto& entry : entries) {
                *args->add_entries() = entry;
            }
            
            auto reply = std::make_shared<raftRpcProtoc::AppendEntriesReply>();
            reply->set_appstate(Disconnected);
            auto appendNums = std::make_shared<int>(1);  // Leader 自己算一个
            
            std::thread([this, peerId, args, reply, appendNums]() {
                sendAppendEntries(peerId, args, reply, appendNums);
            }).detach();
        }
        
        // 休眠一段时间再检查（1ms）
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

// 立即刷新某 Peer 的待发送条目（当需要立即提交时调用）
void Raft::flushPendingEntries(int peerId) {
    std::vector<raftRpcProtoc::LogEntry> entries;
    
    {
        std::lock_guard<std::mutex> lock(m_batchMtx);
        if (peerId < static_cast<int>(m_pendingEntries.size())) {
            entries = std::move(m_pendingEntries[peerId].entries);
            m_pendingEntries[peerId].entries.clear();
        }
    }
    
    if (entries.empty()) return;
    
    auto args = std::make_shared<raftRpcProtoc::AppendEntriesArgs>();
    int prevLogIndex = entries.front().logindex() - 1;
    
    args->set_term(m_currentTerm);
    args->set_leaderid(m_me);
    args->set_prevlogindex(prevLogIndex);
    args->set_prevlogterm(getLogTermFromLogIndex(prevLogIndex));
    args->set_leadercommit(m_commitIndex);
    args->set_batchid(entries.front().logindex());
    
    for (auto& entry : entries) {
        *args->add_entries() = entry;
    }
    
    auto reply = std::make_shared<raftRpcProtoc::AppendEntriesReply>();
    auto appendNums = std::make_shared<int>(1);
    
    std::thread([this, peerId, args, reply, appendNums]() {
        sendAppendEntries(peerId, args, reply, appendNums);
    }).detach();
}

void Raft::applierTicker(){
    while(true){
        std::vector<ApplyMsg> msgs;
        {
            std::lock_guard<std::mutex> lg(m_mtx);
            //如果没有新的可提交日志 就暂时什么都不做
            while(m_lastApplied < m_commitIndex){
                m_lastApplied++;
                int index=m_lastApplied;
                ApplyMsg msg;
                msg.CommandValid=true;
                msg.index=index;
                //从m-Logs取出对应日志项
                int sliceIdx=getSlicesIndexFromLogIndex(index);
                const auto& entry=m_logs[sliceIdx];
                msg.command=entry.command();
                msgs.push_back(std::move(msg));
            }
        }
        //在锁外推消息，避免长时间持锁
        for(auto& msg:msgs){
            pushMsgToKVserver(msg);
        }
        //控制apply频率 避免空缺
        usleep(1000);
    }
}


//预选举
void Raft::PreRequestVote1(const raftRpcProtoc::PreRequestVoteArgs* args,raftRpcProtoc::PreRequestVoteReply* reply){
    std::lock_guard<std::mutex> lg(m_mtx);
    //默认返回当前节点的term 候选者可以根据此知道自己是否落后
    reply->set_term(m_currentTerm);
    //1.候选者的term小于自己-->直接拒绝
    if(args->term()<m_currentTerm){
        reply->set_votegranted(false);
        return;
    }
    //2.日志必须至少一样新
    bool candidateLogUpToDate=UPtodata(args->lastlogindex(),args->lastlogterm());
    if(!candidateLogUpToDate){
        reply->set_votegranted(false);
        return;
    }
    //3.如果当前自己就是Leader了 可以拒绝预投票 叫少无畏的扰动
    if(m_status==Leader){
        reply->set_votegranted(false);
        return;
    }
    //4.不修改m_curentterm/m_votedFor/m_status 仅表示如果是真选举 我会投n
    reply->set_votegranted(true);
}
//实现RPC包装
void Raft::PreRequestVote(::google::protobuf::RpcController* controller,
                          const raftRpcProtoc::PreRequestVoteArgs* request,
                          raftRpcProtoc::PreRequestVoteReply* response,
                          ::google::protobuf::Closure* done) {
    (void)controller;
    PreRequestVote1(request, response);
    if (done) {
        done->Run();
    }
}

// ========== ReadIndex 优化实现 ==========

// 获取当前 commitIndex
int Raft::GetCommitIndex() {
    std::lock_guard<std::mutex> lg(m_mtx);
    return m_commitIndex;
}

// 获取当前 Leader 信息（简化版本：返回自身是否是 Leader）
void Raft::GetLeaderInfo(int* leaderId, std::string* leaderIp, int* leaderPort) {
    std::lock_guard<std::mutex> lg(m_mtx);
    if (leaderId) {
        // 如果自己是 Leader，返回自己的 ID
        *leaderId = (m_status == Leader) ? m_me : -1;
    }
    // leaderIp 和 leaderPort 需要从配置中获取，这里简化处理
    if (leaderIp) *leaderIp = "";
    if (leaderPort) *leaderPort = 0;
}

// 等待日志重放完成（m_lastApplied 追上 getLastLogIndex()）
// 用于确保 HNSW 索引在全量日志重放完成后再构建
bool Raft::WaitApplyDone(int timeoutMs) {
    auto deadline = std::chrono::steady_clock::now() +
                   std::chrono::milliseconds(timeoutMs);
    while (true) {
        {
            std::lock_guard<std::mutex> lg(m_mtx);
            // 追上最后一个日志条目就算恢复完成（此时 SkipList 有所有数据）
            if (m_lastApplied >= getLastLogIndex()) {
                return true;
            }
        }
        auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

// ReadIndex RPC 处理器：Follower 请求 Leader 确认
void Raft::ReadIndex1(const raftRpcProtoc::ReadIndexRequest* request,
                       raftRpcProtoc::ReadIndexResponse* response) {
    std::lock_guard<std::mutex> lg(m_mtx);

    // 1. 返回当前 term 和 commitIndex
    response->set_term(m_currentTerm);
    response->set_commitindex(m_commitIndex);

    // 2. 如果自己不是 Leader，返回 false
    if (m_status != Leader) {
        response->set_isleader(false);
        return;
    }

    // 3. 确认自己是合法 Leader
    //    Leader 在处理 ReadIndex 请求时，确认 term 没有发生变化
    //    （如果 term 变化，说明有新的 Leader，Follower 需要重试）
    response->set_isleader(true);

    // 4. Leader 需要确认 commitIndex 已经达成共识
    //    对于 lease read 优化，可以在这里跳过心跳确认
    //    这里使用简单的方式：直接返回当前的 commitIndex
}

// ReadIndex RPC 包装（供框架调用）
void Raft::ReadIndex(::google::protobuf::RpcController* controller,
                     const raftRpcProtoc::ReadIndexRequest* request,
                     raftRpcProtoc::ReadIndexResponse* response,
                     ::google::protobuf::Closure* done) {
    (void)controller;
    ReadIndex1(request, response);
    if (done) {
        done->Run();
    }
}

// 发送 ReadIndex 请求到指定服务器
bool Raft::sendReadIndex(int serverId,
                          std::shared_ptr<raftRpcProtoc::ReadIndexRequest> request,
                          std::shared_ptr<raftRpcProtoc::ReadIndexResponse> response) {
    if (serverId < 0 || serverId >= static_cast<int>(m_peers.size())) {
        return false;
    }
    return m_peers[serverId]->ReadIndex(request.get(), response.get());
}

// ========== ReadIndex 优化实现 End ==========

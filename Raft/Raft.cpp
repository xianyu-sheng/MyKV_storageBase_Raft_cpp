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
        doElection();
    }
}

//节点选举：这里我想要实现预选举的操作【这里是我自己的创新，用于防止网络分区节点无限制增长任期，提高系统稳定性】
//预选举将在正式选举前进行，只有在预选举中获得足够多数票时才进行正式选举
//预选举阶段：不增加任期，只测试其他节点是否能够接收心跳
//在预选举阶段，节点会发送预投票请求，如果大多数节点响应，则进行正式选举
//预选举的主要优势是避免了因网络分区导致的任期不一致问题，提高了系统的稳定性和一致性
void Raft::doElection(){
    //后续增加预选举
    std::lock_guard<std::mutex>g(m_mtx);
    if(m_status==Leader){
        //什么不干
        return;
    }
    if(m_status!=Leader){
        DPrintf("[ticker-fun-rf(%d)]选举定时器到期且不是Leader，开始选举",m_me);
        //当选举的时候定时器超时就必须重新选举，否则会因没有选票被卡死
        //重新选举又超时的话，term会增加
        m_status=Candidate;//身份设置为候选者2
        m_currentTerm+=1;//无论是刚开始竞选或者因为超时重新竞选，都会增加term的值
        m_votedFor=m_me;//把自己的的票先投给自己
        persist();//对以上数据进行持久化
        std::shared_ptr<int>votedNum=std::make_shared<int>(1);//记录获得的票数 是不是是大多数的票数 使用make_ptr的好处是避免了内存分配失败，将构建对象和内存分配一起进行
        //重新设置定时器，防止触发选举
        m_lastResetElectionTime=now();
        //设置请求参数和响应参数创建工作线程调用sendrequestVote发送给其他的raft节点
        //请求投票哦
        for(int i=0;i<m_peers.size();i++){
            if(i==m_me){
                continue;
            } 
            int lastLogIndex=-1,lastLogTerm=-1;
            getLastLogIndexAndTerm(&lastLogIndex,&lastLogTerm);//获取最后一个log和term的下标。以添加到RPC的发送
            std::shared_ptr<raftRpcProtoc::RequestVoteArgs>requestVoteArgs=std::make_shared<raftRpcProtoc::RequestVoteArgs>();
            requestVoteArgs->set_term(m_currentTerm);
            requestVoteArgs->set_candidateid(m_me);
            requestVoteArgs->set_lastlogindex(lastLogIndex);
            requestVoteArgs->set_lastlogterm(lastLogTerm);
            auto requestVoteReply=std::make_shared<raftRpcProtoc::RequestVoteReply>();
            //创建新线程执行sendRequestVote函数
            std::thread t(&Raft::sendRequestVote,this,i,requestVoteArgs,requestVoteReply,votedNum);
            t.detach(); 
        }
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
        }
        std::thread t(&Raft::doHeartBeat,this);//马上宣告自己是Leader进行心跳或日志复制
        t.detach();
        persist();
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
            suitableSleepTime=std::chrono::milliseconds(HeartBeatTimeout)+m_lastResetElectionTime-weakTime;
        }
        if(std::chrono::duration<double,std::milli>(suitableSleepTime).count()>1){
            //说明此时还没到发心跳的时间 继续睡眠
            std::cout<<atomicCount <<"\033[1;35m leaderHearBeatTicker();函数设置睡眠时间为]"
                                    <<std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count()<<"毫秒\033[0m"
                                    <<std::endl;
            //获取当前时间点
            auto start=std::chrono::steady_clock::now();
            usleep(std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count());
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
        doHeartBeat();//执行实际的心跳发送操作
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
        myAssert(m_nextIndex[peerId] >= 1, format("追随者[%d]的nextIndex异常: %d", peerId, m_nextIndex[peerId]));

        // 根据nextIndex判断需要发送快照还是日志条目
        if (m_nextIndex[peerId] < m_lastSnapshotIncludeIndex) {
            // 发送快照到追随者
            startSnapshotSendThread(peerId, successCount);
        } else {
            // 发送日志条目到追随者
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
void Raft::startLogEntriesSendThread(int peerId, std::shared_ptr<int> successCount) {
    // 1. 计算前置日志信息（prevLogIndex和prevLogTerm）
    int preLogIndex = -1;
    int preLogTerm = -1;
    getPrevLogInfo(peerId, &preLogIndex, &preLogTerm);

    // 2. 构造AppendEntries请求参数
    auto args = std::make_shared<raftRpcProtoc::AppendEntriesArgs>();
    args->set_term(m_currentTerm);
    args->set_leaderid(m_me);
    args->set_prevlogindex(preLogIndex);
    args->set_prevlogterm(preLogTerm);
    args->set_leadercommit(m_commitIndex);
    args->clear_entries(); // 清空残留条目

    // 3. 填充需要发送的日志条目
    if (preLogIndex != m_lastSnapshotIncludeIndex) {
        // 从preLogIndex的下一条开始发送（非快照后的第一条）
        int startSliceIdx = getSlicesIndexFromLogIndex(preLogIndex) + 1;
        for (int j = startSliceIdx; j < m_logs.size(); ++j) {
            *args->add_entries() = m_logs[j];
        }
    } else {
        // 从快照后的第一条日志开始发送
        for (const auto& log : m_logs) {
            *args->add_entries() = log;
        }
    }

    // 4. 验证日志条目连续性（保持原逻辑的断言检查）
    int lastLogIndex = getLastLogIndex();
    myAssert(preLogIndex + args->entries_size() == lastLogIndex,
             format("日志条目长度不匹配：prev[%d] + entries[%d] != last[%d]",
                    preLogIndex, args->entries_size(), lastLogIndex));

    // 5. 创建线程发送日志，分离线程
    auto reply = std::make_shared<raftRpcProtoc::AppendEntriesReply>();
    reply->set_appstate(Disconnected); // 初始化为未连接状态
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
        DPrintf("[func-sendAppendEntries rf{%d} 返回的日志term相等 但不匹配  回退nextindex[%d]:{%d}\n]",
                m_me, peerId, reply->updatenextindex());
        m_nextIndex[peerId] = reply->updatenextindex(); // 用追随者返回的nextIndex优化重试
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

    // 3. 防御性断言（原逻辑保留）
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

    // 6. 可以选择立即触发一次心跳/日志复制（也可依赖心跳线程）
    // doHeartBeat(); // 可选
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
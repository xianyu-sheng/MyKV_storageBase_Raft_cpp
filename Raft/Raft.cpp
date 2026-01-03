#include  "Raft.h"
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

    m_mtx.unlock();  // 完成初始化后解锁，以便其他线程或协程可以访问共享数据

    m_ioManager = std::make_unique<monsoon::IOManager>(FIBER_THREAD_NUM, FIBER_USE_CALLER_THREAD);

    // 启动三个循环定时器
    // todo：原来是启动了三个线程，现在是直接使用了协程，三个函数中leaderHearBeatTicker
    //、electionTimeOutTicker执行时间是恒定的，applierTicker时间受到数据库响应延迟和两次apply之间请求数量的影响，这个随看数据>
    m_ioManager->scheduler([this]() -> void { this->leaderHearBeatTicker(); });
    m_ioManager->scheduler([this]() -> void { this->electionTimeoutTicker(); });

    std::thread t3(&Raft::applierTicker, this);
    t3.detach();
}


//检验是否达到选举超时
void Raft::electionTimeoutTicker(){
    /*
        先检测时间是否达到选举超时，如果超时了 那么就更新当前的term然后为自己投票 并发起选举
        1. 检查是否达到选举超时时间
        2. 如果超时，增加任期，投票给自己
        3. 发起选举请求

        不过我认为在这里应该增加一个预选举的机制，防止一个网络分区的节点 无限制的增长任期
    */
    while(true){
        //如果不睡的话，那么对于Leader来说这个函数会一直空转，浪费CPU 且加入协程后，空转会导致其他协程无法运行
        while(m_status==Leader){
            //所以要让其睡hearBeat的时间，以为内hearbearBeat必选举超时一般小一个量级
            usleep(HeartBeatTimeout);
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
            //使用ANSI控制序列将输出颜色修改为紫色
            std::cout << "\033[1;35m electionTimeoutTicker();函数设置睡眠时间为："
                      <<std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count()<<"毫秒\033[0m"
                      << std::endl;
            std::cout << "\033[1;35m electionTimeoutTicker();函数的实际睡眠时间为]"<<duration.count()<<"毫秒\033[0m"
                       << std::endl;
        }
        if(std::chrono::duration<double,std::milli>(m_lastRestElectionTime-weakTime).count()>0){
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
            if(i=m_me){
                continue;
            } 
            int lastLogIndex=-1,lastLogTerm=-1;
            getLastLogIndexAndTerm(lastLogIndex,lastLogTerm);//获取最后一个log和term的下标。以添加到RPC的发送
            std::shared_ptr<raftRpcProtoc::RequestVoteArgs>requestVoteArgs=std::make_shared<raftRpcProtoc::RequestVoteArgs>();
            requestVoteArgs->set_term(m_currentTerm);
            requestVoteArgs->set_candidate_id(m_me);
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
bool Raft::sendReqeustVote(int server,std::shared_ptr<raftRpcProtoc::RequestVoteArgs>args,std::shared_ptr<raftRpcProtoc::RequestVoteReply>reply,std::shared_ptr<int>votedNum){
    auto start=now();
    DPrintf("[func-sendReqeustVote rf{%d}]向server{%d}发送RequestVote 开始",m_me,m_currentTerm,getlastLogIndex());
    bool ok=m_peers[server]->RequestVote1(args.get(),reply.get());//接收其他raft节点返回的结果
    DPrintf("func-sendRequestVote rf{%d} 向 server {%d} 发送 RequestVote完毕,耗时:{%d} ms",m_me,m_currentTerm,getLastLogIndex(),now-start);

    if(!ok){
        return ok;//Rpc通信失败就理解返回，避免资源浪费
    }
    //对回应进行处理，记住无论什么时候收到回复就检查term
    std::lock_guard<std::mutex>lg(mtx);
    if(reply->term > m_currentTerm){
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
    if(!reply->votegrandted()){
        //这个节点因为某些原因没给本节点投票，结束该函数
        return true;
    }
    *votedNum=*voteNum+1;
    if(*votedNum>=m_peers.size()/2+1){
        //Raft领导选举机制，超过半数节点投票就自动成为领导
        *votedNum=0;//设置为0避免重复当选
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
        std::thread t(&RAft::doHeartBeat,this);//马上宣告自己是Leader进行心跳或日志复制
        t.detach();
        persist();
    }
    return true;
}

//Request用于响应sendRequestVote,是否投票给他让其成为Leader
//那么其他节点也是需要检查这个term和最新日志
void  Raft::RequestVote1(const raftRpcProtoc::RequestVoteArgs* args,raftRpcProtoc::RequestVoteReply* reply){
    std::lock_guard<std::mutex>lg(m_mtx);
    DEFERP{
        persist();//要在锁释放之前更新状态避免在锁释放后才更新，会导致状态被别人更新
    };
    //任何情况下都需要先检查任期
    if(args->term() < m_currentTerm){
        reply->set_term(m_currentTerm);//在回复给出自己的任期，告诉他目前任期
        reply->set_votstate(Expire);//设置投票状态过期，告诉候选者你这个不行了
        reply-?set_votegrandted(false);//投票失败
        return;
    }
    //这里解决的是 如果自己也是候选者 如果遇到比自己大的任期 那么就需要更新自己状态和任期 以及投票状态
    if(args->term() > m_currentTerm){
        //转换状态
        m_status=Follower;
        m_currentTerm=args->term();
        m_votedFor=-1;//重置投票
    }
    myAssert(args->term()==m_currentTerm,format("func--rf{%d} 前面校验过 args.Term==rf.currentTerm,这里却不等",m_me));
    //现在节点任期都是相同的（任期小的也已经更新到新的args的term）
    //然后还需要检查日志是否是匹配的 日志至少需要大于等于自己然后在投票
    int lastLogTerm=getLastLogIndex();
    if(!UPtodata(args->lastlogidnex(),args->lastlogterm())){
        //Uptodata函数用于比较请求者的日志是否比当前节点的日志更新，只有当请求者的日志至少与当前节点一样，才会考虑投票
        if(args->lastlogterm()<lastLogTerm){
            //处理日志，可以输出不匹配 不投票
        }else{
            //同理
        }
        //日志的最后一个任期或者index和请求参数的不匹配无法投票
        reply->set_term(m_currentTerm);
        reply->set_votstate(Voted);
        reply->set_votegrandted(false);
        return;
    }
    //处理因为网络不好导致响应丢失重发的情况
    //这里的情况是该节点已经投过票了并且不是投给当前候选者，因网络不好导致响应丢失而进行的重发消息
    if(m_votedFor!=-1 && m_votedFor!=args->candidate_id()){
        reply->set_term(m_currentTerm);
        reply->set_votstate(Voted);
        reply->set_votegrandted(false);
        return;
    }else{
        //否则投给该请求投票的节点
        m_votedFor=args->candidate_id();
        m_lastResetEelectionTime=now();//在投出票之后重置定时器
        reply->set_term(m_currentTerm);
        reply->set_votstate(Normal);
        reply->set_votegrandted(true);
        return;
    }
}
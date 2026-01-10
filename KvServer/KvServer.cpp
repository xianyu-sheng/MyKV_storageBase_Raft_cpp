//在保证线性一致性之前我们如何读KV
// 核心函数：处理Get操作（保证线性一致性）
//函数理解链接：https://www.doubao.com/thread/wa43737e2bbd8fc16
void KvServer::Get(const raftKVRpcProtoc::GetArgs* args,raftKVRpcProtoc::GetReply* reply){
    Op op;
    op.Operation="Get";
    op.Key=args->key();
    op.value="";
    op.ClientId=args->clientid();
    op.RequestId=args->requestid();

    int raftindex=-1;
    int _=-1;
    bool isLeader=false;
    m_raftNode->Start(op,&_,&isLeader);//raftindex，raft预计的logIndex,虽然是预计，但是正确情况下，是准确的，op的具体内容对raft来说，是隔离的

    if(!isLeader){
        reply->set_err(ErrWrongLeader);
        return;
    }

    //create waitForch
    m_mtx.lock();
    if(waitApplyCh.find(raftindex)==waitApplyCh.end()){
        waitApplyCh.insert(std::make_pair(raftindex,new LockQueue<Op>()));
    }

    auto chForRaftIndex = waitApplyCh[raftindex];

    m_mtx.unlock();

    //超时了怎么做
    Op raftCommitOp;

     // 尝试从Raft提交通道中带超时获取已提交的日志
        if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {
            // 分支1：Raft共识超时（未在超时时间内拿到提交确认）
            int raftState = -1;  // 占位，Raft状态（follower/candidate/leader）
            bool isLeader = false;
            // 获取当前节点的Raft状态：判断是否是Leader
            m_raftNode->GetState(&raftState, &isLeader);

            // 关键判断：请求是重复的 + 当前节点是Leader
            if (ifRequestDuplicate(op.clientId, op.requestId) && isLeader) {
                // 逻辑：超时不代表日志没提交，只是集群没及时响应
                // 但如果是重复的Get请求（已处理过），可安全重读（不违反线性一致性）
                std::string value;
                bool exist = false;
                // 直接在本地KV执行Get
                ExecuteGetOpOnKVDB(op, &value, &exist);
                if (exist) {
                    reply->set_err(OK);
                    reply->set_value(value);
                } else {
                    reply->set_err(ErrNoKey);
                    reply->set_value("");
                }
            } else {
                // 非重复请求/非Leader：返回WrongLeader，让客户端换节点重试
                reply->set_err(ErrWrongLeader);
            }
        } else {
            // 分支2：Raft日志已成功提交（超时前拿到了提交确认）
            // 验证：确保提交的日志就是当前请求的日志（防串请求）
            if (raftCommitOp.clientId == op.clientId && raftCommitOp.requestId == op.requestId) {
                std::string value;
                bool exist = false;
                // 执行Get操作（此时日志已提交，线性一致性有保障）
                ExecuteGetOpOnKVDB(op, &value, &exist);
                if (exist) {
                    reply->set_err(OK);
                    reply->set_value(value);
                } else {
                    reply->set_err(ErrNoKey);
                    reply->set_value("");
                }
            } else {
                // 异常情况：提交的日志和当前请求不匹配（理论上不会发生）
                reply->set_err(ErrWrongLeader);
            }
        }
}
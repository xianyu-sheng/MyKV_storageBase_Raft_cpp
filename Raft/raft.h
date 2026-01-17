#ifndef RAFT_H
#define RAFT_H
////方便网络分区的时候debug，网络异常的时候为disconnected,只要网络正常的就为AppNormal，防止match
#include "ApplyMsg.h"
#include <vector>
#include <memory>
#include <mutex>
#include <chrono>
#include <string>
#include <condition_variable>
#include <queue>
#include "../Proto/raftKVRpcProtoc/KvServerRPC.pb.h"
#include "../Proto/raftRpcProtoc/raftRPC.pb.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <thread>
#include <atomic>

constexpr int Disconnected = 0;
constexpr int AppNormal = 1;

////投票状态
constexpr int Killed=0;
constexpr int Vted=1;//本轮已经投过票了
constexpr int Expire=2;//投票（消息、竞选者）过期
constexpr int Normal=3;

class Persister {
private:
  //原始的文件操作工具函数
    static void EnsureDir(const std::string& dir){
      ::mkdir(dir.c_str(),0755);
    }
    static bool ReadFile(const std::string& path,std::string* out){
        int fd=::open(path.c_str(),O_RDONLY);//只读文件
        if(fd<0){
          return false;
        }
        struct stat st{};
        if(::fstat(fd,&st)!=0){
          ::close(fd);
          return false;
        }
        out->assign(static_cast<size_t>(st.st_size), '\0');
        size_t off=0;
        while(off<out->size()){
          ssize_t n=::read(fd,&(*out)[off],out->size()-off);
          if(n<=0) break;
          off += static_cast<size_t>(n);
        }
        ::close(fd);
        return true;
    }
    static bool WriteFileAtomic(const std::string& path, const std::string& data) {
      std::string tmp = path + ".tmp";

      int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
      if (fd < 0) return false;

      size_t off = 0;
      while (off < data.size()) {
          ssize_t n = ::write(fd, data.data() + off, data.size() - off);
          if (n <= 0) { ::close(fd); return false; }
          off += static_cast<size_t>(n);
      }

      ::fsync(fd);
      ::close(fd);

      if (::rename(tmp.c_str(), path.c_str()) != 0) return false;
      return true;
  }
    private:
      //内存状态 供读取使用
      std::mutex m_mtx;
      std::string m_raftState;
      std::string m_snapshot;

      //持久化任务队列相关
      std::mutex m_ioMtx;//保护IO队列数据
      std::condition_variable m_cv;//唤醒后台线程

      //待写入磁盘的数据(pending Data)
      std::string m_pendingRaftState;
      std::string m_pendingSnapshot;
      bool m_hasPendingRaft;//标记是否有新RaftState需要写
      bool m_hasPendingSnapshot;//标记是否有新Snapshot需要写

      std::atomic<bool> m_stop;//停止标志
      std::thread m_ioThread;//后台IO线程

      //序号
      long long m_nextSeq;//下一个分配的序号
      long long m_flushedSeq;//已经刷盘完成的最大序号
      long long m_pendingSeq;//当前这批待写数据的序号
      std::condition_variable m_flushCv;//用来环形等待刷盘完成的信号量

      //路径配置
      std::string m_dir;
      std::string m_raftPath;
      std::string m_snapshotPath;

      //后台线程工作循环
      void WorkLoop(){
        while(true){
            std::string stateToWrite;
            std::string snapshotToWrite;
            bool writeState=false;
            bool writeSnap=false;
            long long seqToWrite=0;//这次写的对应序号
            {
                //1.等待任务
                std::unique_lock<std::mutex> lock((m_ioMtx));
                m_cv.wait(lock,[this]{
                  return m_stop || m_hasPendingRaft || m_hasPendingSnapshot;
                });

                if(m_stop && !m_hasPendingRaft && !m_hasPendingSnapshot){
                  return;//二次检查 退出
                }
                //2.取出最新的待写入的数据(使用Move语义，减少拷贝)
                //这里实现来写合并，如果队列里堆积了多次修改，那么只取最新的一次修改
                //这里现将快照存入磁盘 避免先存入Raft状态 系统突然宕机 到时候磁盘读取缺失数据
                //其实我们可以这样想 就是快照存储的是大量数据 而节点更新通常只是少量数据
                //如果我们先保存少量数据 那么我们系统突然宕机 那么就会缺失大量的数据
                if(m_hasPendingSnapshot){
                  //如果是有新的快照了
                  snapshotToWrite=std::move(m_pendingSnapshot);
                  writeSnap=true;
                  m_hasPendingSnapshot=false;//标记已经处理完毕
                }
                if(m_hasPendingRaft){
                  stateToWrite=std::move(m_pendingRaftState);
                  writeState=true;
                  m_hasPendingRaft=false;//不来就处理完毕
              }
                if(writeSnap || writeState){
                  seqToWrite=m_pendingSeq;
                }
            }//锁在这里释放 后续的IO操作不占用锁
              //3.执行耗时IO
              if(writeSnap){
                WriteFileAtomic(m_snapshotPath,snapshotToWrite);
              }
              if(writeState){
                WriteFileAtomic(m_raftPath,stateToWrite);
              }
              if(seqToWrite>0){
                std::lock_guard<std::mutex> lk(m_ioMtx);
                if(seqToWrite>m_flushedSeq){
                  m_flushedSeq=seqToWrite;
                }
                m_flushCv.notify_all();
              }
          }
      }
    public:
      explicit Persister(int me) : m_hasPendingRaft(false), m_hasPendingSnapshot(false), m_stop(false),m_nextSeq(0),m_flushedSeq(0),m_pendingSeq(0){
          m_dir="./raft_persist";
          EnsureDir(m_dir);

          m_raftPath=m_dir+"/raft_state_"+std::to_string(me);
          m_snapshotPath=m_dir+"/snapshot_"+std::to_string(me);
          //初始加载
          ReadFile(m_raftPath,&m_raftState);
          ReadFile(m_snapshotPath,&m_snapshot);
          //启动后台线程
          m_ioThread=std::thread(&Persister::WorkLoop,this);
      }
      ~Persister(){
          {
            std::lock_guard<std::mutex> lg(m_ioMtx);
            m_stop=true;
          }
          m_cv.notify_one();
          if(m_ioThread.joinable()){
            m_ioThread.join();
          }
      }
      //优化后的Save：非阻塞 立即返回
      long long  Save(std::string raftstate,std::string snapshot){
          //先snapshot
          //1.更新内存副本（供read使用）----比如重选Leader节点 这里我们将这个副本在内存中拷贝一份
          //可以加快速速度
          {
            std::lock_guard<std::mutex> lg(m_mtx);
            m_raftState=raftstate;
            m_snapshot=snapshot;
          }
          long long seq;
          //2.推送任务到后台线程
          {
              std::lock_guard<std::mutex> lg(m_ioMtx);
              //直接覆盖Pending数据->自动实现 write Batching
              //无论上一次Save还没写完  我们只关心最新的数据
              m_pendingRaftState=std::move(raftstate);
              m_pendingSnapshot=std::move(snapshot);
              m_hasPendingRaft=true;
              m_hasPendingSnapshot=true;
              ++m_nextSeq;
              m_pendingSeq=m_nextSeq;
              seq=m_nextSeq;
          }
          m_cv.notify_one();
          return seq;
      }
      //优化后的SaveRaftState
      long long  SaveRaftState(const std::string& data){
        //更新内存
        {
          std::lock_guard<std::mutex> lg(m_mtx);
          m_raftState=data;
        }
        long long seq;
        //推送任务
        {
            std::lock_guard<std::mutex> lg(m_ioMtx);
            m_pendingRaftState=data;
            m_hasPendingRaft=true;
            ++m_nextSeq;
            m_pendingSeq=m_nextSeq;
            seq=m_nextSeq;
        } 
        m_cv.notify_one();
        return seq;
      }
      void WaitFlushed(long long seq){
          std::unique_lock<std::mutex> lock(m_ioMtx);
          m_flushCv.wait(lock,[this,seq]{
              return m_flushedSeq>=seq;
          });
      }
      //读取操作保持极快，直接读取内存，不用管IO锁
      std::string ReadSnapshot(){
        std::lock_guard<std::mutex> lg(m_mtx);
        return m_snapshot;
      }
      std::string ReadRaftState(){
        std::lock_guard<std::mutex> lg(m_mtx);
        return m_raftState;
      }
      long long RaftStateSize(){
        std::lock_guard<std::mutex> lg(m_mtx);
        return static_cast<long long>(m_raftState.size());
      }
};

class RaftRpcUtil {
 public:
  virtual ~RaftRpcUtil() = default;

  virtual bool AppendEntries(const raftRpcProtoc::AppendEntriesArgs* args,
                             raftRpcProtoc::AppendEntriesReply* reply) = 0;
  virtual bool RequestVote(const raftRpcProtoc::RequestVoteArgs* args,
                           raftRpcProtoc::RequestVoteReply* reply) = 0;
  virtual bool InstallSnapshot(const raftRpcProtoc::InstallSnapshotRequest* request,
                               raftRpcProtoc::InstallSnapshotResponse* response) = 0;
};

// 一个简单的线程安全队列，供 Raft 和 KvServer 之间传递 ApplyMsg / Op 使用
// 只实现当前代码中用到的接口：push 和 timeOutPop
template <typename T>
class LockQueue {
public:
  LockQueue() = default;

  void push(const T& value) {
    std::lock_guard<std::mutex> lk(m_mtx);
    m_queue.push(value);
    m_cv.notify_one();
  }

  // 带超时 Pop，timeoutMs 为毫秒
  bool timeOutPop(int timeoutMs, T* out) {
    std::unique_lock<std::mutex> lk(m_mtx);
    if (!m_cv.wait_for(lk, std::chrono::milliseconds(timeoutMs), [this]{ return !m_queue.empty(); })) {
      return false; // 超时
    }
    *out = m_queue.front();
    m_queue.pop();
    return true;
  }

private:
  std::mutex m_mtx;
  std::condition_variable m_cv;
  std::queue<T> m_queue;
};

struct Op;
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
    std::chrono::_V2::system_clock::time_point m_lastResetElectionTime;
    std::chrono::_V2::system_clock::time_point m_lastResetHearBeatTime;

    //用于传入快照点和最后的任期
    int m_lastSnapshotIncludeIndex;
    int m_lastSnapshotIncludeTerm;

    public:
        void AppendEntries1(const raftRpcProtoc::AppendEntriesArgs* args,
                            raftRpcProtoc::AppendEntriesReply* reply);//日志同步+心跳
        void applierTicker();//定期向状态机写入日志
        bool CondInstallSnapshot(int lastIncludeTerm,int lastIncludeIndex,std::string snapshot);//记录某个时候的状态

        void doElection();//执行选举
        void doHeartBeat();//发起心跳
        void startSnapshotSendThread(int peerId, std::shared_ptr<int> successCount);
        void startLogEntriesSendThread(int peerId, std::shared_ptr<int> successCount);
        //监控是否发起选举
        //每隔一段时间检查睡眠时间有没有重置定时器，没有则超时
        //如果有则设置合适的睡眠时机，睡眠到重置时机+超时时机
        void electionTimeoutTicker();//监控是否发起选举
        std::vector<ApplyMsg> getApplyLogs();//获取应用日志
        int getnewCommandIndex();//获取新命令的索引

        void getPrevLogInfo(int server,int* preindex,int* preterm);//获取当前日志信息
        void GetState(int* term,bool* isLeader);//看当前的节点是否是Leader节点
        void InstallSnapshot1(const raftRpcProtoc::InstallSnapshotRequest* request,
                             raftRpcProtoc::InstallSnapshotResponse* response);//安装快照
        void leaderHearBeatTicker();//负责查看是否该发送心跳了，如果该发起就执行doHeartBeat()

        // 领导节点发送快照（实现中需要这些参数）
        void leaderSendSnapShot(int peerId, int snapshotIndex, int snapshotTerm,
                                const std::string& snapshotData,
                                std::shared_ptr<int> successCount);

        void leaderUpdateCommitIndex(); // 领导更新提交索引（无参数）
        bool matchLog(int logIndex,int LogTerm);//对象Index日志是否匹配，用来判断领导节点的日志与追随者是否匹配，用于选举以及判断心跳日志是否最新你
        void persist();//持久化当前状态

        void RequestVote1(const raftRpcProtoc::RequestVoteArgs* args,raftRpcProtoc::RequestVoteReply* reply);//变成候选者请求其他节点投票
        bool UPtodata(int index,int term);//判断当前节点是否有最新日志
        int getLastLogIndex();//获取最后一个日志条目索引
        int getLastLogTerm();//获取最后一个日志条目的任期
        int getLogTermFromLogIndex(int logIndex);

        void getLastLogIndexAndTerm(int* lastLogIndex,int* lastLogTerm);//获取最后一个日志的索引和任期
        int GetRaftStateSize();//获取raft状态的大小
        int getSlicesIndexFromLogIndex(int Logindex);//将日志索引转换为日志条目在m_logs数组中的位置
        bool sendRequestVote(int server,
                     std::shared_ptr<raftRpcProtoc::RequestVoteArgs> args,
                     std::shared_ptr<raftRpcProtoc::RequestVoteReply> reply,
                     std::shared_ptr<int> voteNum);

        bool sendAppendEntries(int server,
                       std::shared_ptr<raftRpcProtoc::AppendEntriesArgs> args,
                       std::shared_ptr<raftRpcProtoc::AppendEntriesReply> reply,
                       std::shared_ptr<int> appendNum);

        void pushMsgToKVserver(ApplyMsg msg);//向上层KVserver发送消息
        void readPersist(std::string data);//读取持久化数据
        std::string persistDate();//持久化数据
        void Start(const Op& command, int* newLogIndex, bool* isLeader);
        void init(std::vector<std::shared_ptr<RaftRpcUtil>> peers,
          int me,
          std::shared_ptr<Persister> persist,
          std::shared_ptr<LockQueue<ApplyMsg>> applyCh);
        bool callAppendEntriesRpc(int peerId, 
                             std::shared_ptr<raftRpcProtoc::AppendEntriesArgs> args,
                             std::shared_ptr<raftRpcProtoc::AppendEntriesReply> reply);
        bool handleTermMismatch(int peerId, int replyTerm);
        bool isStillLeader();
        void handleLogMismatch(int peerId, std::shared_ptr<raftRpcProtoc::AppendEntriesReply> reply);
        void updatePeerSyncState(int peerId, 
                                std::shared_ptr<raftRpcProtoc::AppendEntriesArgs> args,
                                std::shared_ptr<int> appendNums);
        void tryUpdateCommitIndex(std::shared_ptr<raftRpcProtoc::AppendEntriesArgs> args,
                                std::shared_ptr<int> appendNums);
};
#endif
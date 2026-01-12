#pragma once
#include <string>

struct ApplyMsg {
    bool CommandValid = false;  // 这条消息是不是一条普通日志
    int  index = 0;             // 日志索引（Raft logindex）
    std::string command;        // 日志里的 command 字节串（这里存 encodeOp 结果）

    // 预留 snapshot 用的字段（以后需要可以再用）
    bool SnapshotValid = false;
    int  lastIncludedIndex = 0;
    int  lastIncludedTerm  = 0;
    std::string snapshot;
};
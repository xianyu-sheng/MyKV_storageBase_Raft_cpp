#pragma once
#include <string>
#include <sstream>
#include "kvServer.h"  // 需要 Op 的定义

inline std::string encodeOp(const Op& op) {
    std::ostringstream oss;
    oss << op.Operation << '|'
        << op.Key << '|'
        << op.Value << '|'
        << op.ClientId << '|'
        << op.RequestId;
    return oss.str();
}

inline Op decodeOp(const std::string& data) {
    Op op;
    std::istringstream iss(data);
    std::string token;

    std::getline(iss, op.Operation, '|');
    std::getline(iss, op.Key, '|');
    std::getline(iss, op.Value, '|');
    std::getline(iss, op.ClientId, '|');
    std::getline(iss, token, '|');
    op.RequestId = std::stoi(token);

    return op;
}
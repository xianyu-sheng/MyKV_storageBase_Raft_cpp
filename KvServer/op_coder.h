#pragma once
#include <string>
#include <sstream>
#include <cstdint>
#include "kvServer.h"

inline std::string encodeOp(const Op& op) {
    std::ostringstream oss;
    // 使用长度前缀编码（二进制安全）：field_size(4字节)|field_content
    auto write_field = [&](const std::string& s) {
        uint32_t len = static_cast<uint32_t>(s.size());
        oss.write(reinterpret_cast<const char*>(&len), sizeof(len));
        oss.write(s.data(), len);
    };
    write_field(op.Operation);
    write_field(op.Key);
    write_field(op.Value);
    write_field(op.ClientId);
    write_field(std::to_string(op.RequestId));
    return oss.str();
}

inline Op decodeOp(const std::string& data) {
    Op op;
    std::istringstream iss(data);
    std::string token;

    auto read_field = [&](std::string* out) -> bool {
        if (iss.peek() == std::istream::traits_type::eof()) return false;
        uint32_t len = 0;
        if (!iss.read(reinterpret_cast<char*>(&len), sizeof(len))) return false;
        if (len > 1024 * 1024 * 1024) return false; // 防御性：拒绝超大字段
        out->resize(len);
        if (!iss.read(out->data(), len)) return false;
        return true;
    };

    if (!read_field(&op.Operation)) return op;
    if (!read_field(&op.Key)) return op;
    if (!read_field(&op.Value)) return op;
    if (!read_field(&op.ClientId)) return op;
    if (read_field(&token)) {
        try {
            op.RequestId = std::stoi(token);
        } catch (...) {
            op.RequestId = -1;
        }
    }
    return op;
}
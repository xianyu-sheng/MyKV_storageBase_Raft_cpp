cmake_minimum_required(VERSION 3.10)
project(MyKV_StorageBase_Raft_CPP LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 为了看到更详细的编译信息
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Protobuf
find_package(Protobuf REQUIRED)
message(STATUS "Found Protobuf: ${Protobuf_LIBRARIES}")

# 头文件搜索路径
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/Clerk
    ${CMAKE_CURRENT_SOURCE_DIR}/KvServer
    ${CMAKE_CURRENT_SOURCE_DIR}/Raft
    ${CMAKE_CURRENT_SOURCE_DIR}/Skiplist-CPP
    ${CMAKE_CURRENT_SOURCE_DIR}/Proto
    ${CMAKE_CURRENT_SOURCE_DIR}/Proto/raftRpcProtoc
    ${CMAKE_CURRENT_SOURCE_DIR}/Proto/raftKVRpcProtoc
    ${CMAKE_CURRENT_SOURCE_DIR}/myRPC/User
    ${CMAKE_CURRENT_SOURCE_DIR}/myRPC/Server
)

# 源文件列表
set(RAFT_SRC
    Raft/Raft.cpp
)

set(KVSERVER_SRC
    KvServer/KvServer.cpp
)

set(CLERK_SRC
    Clerk/clerk.cpp
)

set(PROTO_SRC
    Proto/raftRpcProtoc/raftRPC.pb.cc
    Proto/raftKVRpcProtoc/KvServerRPC.pb.cc
)

# 组合成一个库，供多个可执行程序复用（核心逻辑：Raft + KvServer + Clerk + Proto + Skiplist）
add_library(kvcore STATIC
    ${RAFT_SRC}
    ${KVSERVER_SRC}
    ${CLERK_SRC}
    ${PROTO_SRC}
)

target_link_libraries(kvcore
    PRIVATE
        ${Protobuf_LIBRARIES}
        pthread
)

# 目前项目中唯一的 main 在 Skiplist-CPP/main.cpp（这是 Skiplist demo），
# 真正的分布式 KV/Raft 服务和 Clerk 还没有单独的 main 文件。
# 如果你后面为服务端/客户端写了 main，例如 ServerMain.cpp / ClientMain.cpp，
# 可以在下面按需添加：
#
# add_executable(kvserver ServerMain.cpp)
# target_link_libraries(kvserver PRIVATE kvcore)
#
# add_executable(kvclient ClientMain.cpp)
# target_link_libraries(kvclient PRIVATE kvcore)


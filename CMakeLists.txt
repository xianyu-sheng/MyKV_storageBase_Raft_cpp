cmake_minimum_required(VERSION 3.10)
project(MyKV_StorageBase_Raft_CPP LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 为了看到更详细的编译信息
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Protobuf
find_package(Protobuf REQUIRED)
message(STATUS "Found Protobuf: ${Protobuf_LIBRARIES}")

# Boost.Serialization（用于 Raft 状态的 Boost 序列化）
find_package(Boost REQUIRED COMPONENTS serialization)
message(STATUS "Found Boost.Serialization: ${Boost_LIBRARIES}")

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
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/hnswlib
)

# 源文件列表
set(RAFT_SRC
    Raft/Raft.cpp
)

set(KVSERVER_SRC
    KvServer/KvServer.cpp
    KvServer/RecallEngine.cpp
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

# kvcore 依赖 Protobuf / pthread / Boost.Serialization。
# 使用 PUBLIC，后续链接 kvcore 的可执行程序会自动继承这些库。
target_link_libraries(kvcore
    PUBLIC
        ${Protobuf_LIBRARIES}
        pthread
        ${Boost_LIBRARIES}
)

# === 集成自研 myRPC 框架的核心库和 Provider ===
set(MYRPC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/myRPC")

# myrpc_core：配置、控制器、日志、应用上下文、ZooKeeper、以及 RPC 头部/用户的 Protobuf 代码
add_library(myrpc_core STATIC
    ${MYRPC_DIR}/Server/KrpcConfig.cc
    ${MYRPC_DIR}/Server/KrpcController.cc
    ${MYRPC_DIR}/Server/KrpcLogger.cc
    ${MYRPC_DIR}/Server/Krpcapplication.cc
    ${MYRPC_DIR}/ZooKeeper/ZooKeeper.cc
    "${MYRPC_DIR}/ProtoBuf protocol/user/user.pb.cc"
    "${MYRPC_DIR}/ProtoBuf protocol/RpcHeader/Krpcheader.pb.cc"
    ${MYRPC_DIR}/User/KrpcChannel.cc
)

target_include_directories(myrpc_core
    PUBLIC
        ${MYRPC_DIR}/Server
        ${MYRPC_DIR}/User
        ${MYRPC_DIR}/ZooKeeper
        "${MYRPC_DIR}/ProtoBuf protocol"
        "${MYRPC_DIR}/ProtoBuf protocol/RpcHeader"
        "${MYRPC_DIR}/ProtoBuf protocol/user"
)

target_link_libraries(myrpc_core
    PUBLIC
        ${Protobuf_LIBRARIES}
        glog
        zookeeper_mt
        pthread
)

# myrpc_provider：KrpcProvider（基于 muduo 的 RPC 服务端）
add_library(myrpc_provider STATIC
    ${MYRPC_DIR}/Server/Krpcprovider.cc
)

target_link_libraries(myrpc_provider
    PUBLIC
        myrpc_core
        muduo_net
        muduo_base
)

# 目前项目中唯一的 main 在 Skiplist-CPP/main.cpp（这是 Skiplist demo），
# 真正的分布式 KV/Raft 服务和 Clerk 还没有单独的 main 文件。
# 如果你后面为服务端/客户端写了 main，例如 ServerMain.cpp / ClientMain.cpp，
# 可以在下面按需添加：

add_executable(kvserver ServerMain.cpp)
target_link_libraries(kvserver
    PRIVATE
        kvcore
        myrpc_core
        myrpc_provider
)

add_executable(kvclient
    ClientMain.cpp
)
target_link_libraries(kvclient
    PRIVATE
        kvcore
        myrpc_core
)

# === 辅助目标：清理 kvserver 日志，防止日志无限增长 ===
# 使用 scripts/clean_logs.sh 将 kvserver{0,1,2}.log 截断为 0 字节。
# BUILD_DIR 通过环境变量传入，脚本内部会在源码根目录和 build 目录下查找日志。
add_custom_target(clean_kv_logs
    COMMAND ${CMAKE_COMMAND} -E env BUILD_DIR=${CMAKE_CURRENT_BINARY_DIR}
            bash ${CMAKE_CURRENT_SOURCE_DIR}/scripts/clean_logs.sh
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Truncate kvserver logs (kvserver0/1/2.log)"
)

# 便捷目标：清理日志后在 build 目录中运行 kvclient（普通模式）。
# 使用方式：在 build 目录中执行 `make run_kvclient_default`。
add_custom_target(run_kvclient_default
    COMMAND ${CMAKE_COMMAND} -E env BUILD_DIR=${CMAKE_CURRENT_BINARY_DIR}
            bash ${CMAKE_CURRENT_SOURCE_DIR}/scripts/clean_logs.sh
    COMMAND kvclient -i ../myRPC/conf/myrpc.conf
    DEPENDS kvclient
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Clean kvserver logs then run kvclient with default config"
)

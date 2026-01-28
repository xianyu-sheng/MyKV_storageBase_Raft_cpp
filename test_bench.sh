#!/bin/bash

# 完整的压测脚本

cd /home/xianyu-sheng/MyKV_storageBase_Raft_cpp/build

echo "=== 1. 停止所有 kvserver ==="
pkill kvserver 2>/dev/null
sleep 2

echo "=== 2. 清理日志和持久化数据 ==="
rm -f kvserver*.log
rm -rf raft_persist/*

echo "=== 3. 启动 3 个 kvserver 节点 ==="
RAFT_ME=0 ./kvserver -i ../myRPC/conf/myrpc_0.conf >kvserver0.log 2>&1 &
RAFT_ME=1 ./kvserver -i ../myRPC/conf/myrpc_1.conf >kvserver1.log 2>&1 &
RAFT_ME=2 ./kvserver -i ../myRPC/conf/myrpc_2.conf >kvserver2.log 2>&1 &

echo "=== 4. 等待服务启动和选举完成 ==="
sleep 5

echo "=== 5. 检查 Leader 选举状态 ==="
grep -i "leader" kvserver*.log | tail -5

echo ""
echo "=== 6. 运行压测：100 ops, 4 threads ===="
./kvclient -i ../myRPC/conf/myrpc.conf -- --bench --ops 100 --threads 4 --write-ratio 30

echo ""
echo "=== 7. 运行压测：200 ops, 8 threads ==="
./kvclient -i ../myRPC/conf/myrpc.conf -- --bench --ops 200 --threads 8 --write-ratio 30

echo ""
echo "=== 压测完成 ==="

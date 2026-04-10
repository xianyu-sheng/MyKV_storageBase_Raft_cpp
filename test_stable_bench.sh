#!/bin/bash
# test_stable_bench.sh - 稳定的 Raft KV 压测脚本
# 使用方法: cd build && bash ../test_stable_bench.sh

set -e
cd /home/xianyu-sheng/MyKV_storageBase_Raft_cpp/build

echo "========================================"
echo "   Raft KV 分布式存储 - 稳定压测脚本"
echo "========================================"

echo ""
echo "[1/7] 清理旧进程和日志..."
pkill -9 kvserver 2>/dev/null || true
pkill -9 kvclient 2>/dev/null || true
sleep 2
rm -rf *.log raft_persist/
mkdir -p raft_persist

echo ""
echo "[2/7] 启动 ZooKeeper（如果未启动）..."
zkServer.sh start 2>/dev/null || true
sleep 2

echo ""
echo "[3/7] 启动 kvserver 集群（3节点）..."
RAFT_ME=0 ./kvserver -i ../myRPC/conf/myrpc_0.conf >kvserver0.log 2>&1 &
RAFT_ME=1 ./kvserver -i ../myRPC/conf/myrpc_1.conf >kvserver1.log 2>&1 &
RAFT_ME=2 ./kvserver -i ../myRPC/conf/myrpc_2.conf >kvserver2.log 2>&1 &

echo "等待服务启动..."
sleep 3

echo ""
echo "[4/7] 等待 Leader 选举稳定（最长等待 30 秒）..."
STABLE=false
for i in {1..10}; do
    sleep 3

    # 获取最新 Term 值
    term1=$(grep -h "term{" kvserver0.log 2>/dev/null | tail -1 | grep -oP 'term\{\K[0-9]+' || echo "0")
    sleep 1
    term2=$(grep -h "term{" kvserver0.log 2>/dev/null | tail -1 | grep -oP 'term\{\K[0-9]+' || echo "0")

    if [ "$term1" == "$term2" ] && [ "$term1" != "0" ]; then
        echo "✓ Leader 选举稳定！Term = $term1（等待 $((i*3)) 秒）"
        STABLE=true
        break
    fi

    if [ $i -le 5 ]; then
        echo "  等待选举完成... ($((i*3))/30 秒) Term: $term1 -> $term2"
    else
        echo "  ⚠ Term 仍在变化: $term1 -> $term2（$((i*3))/30 秒）"
    fi
done

if [ "$STABLE" == "false" ]; then
    echo ""
    echo "⚠ 警告: Leader 选举尚未完全稳定"
    echo "  可能原因: 选举超时设置过短或网络延迟"
    echo "  建议: 调整 Raft.cpp 中的 minRandomizedElectionTime"
    echo ""
fi

echo ""
echo "[5/7] 验证 ZooKeeper 服务注册..."
sleep 1
count=$(echo "ls /kvServerRpc/Get" | nc -w 2 127.0.0.1 2181 2>/dev/null | grep -cE "800[0-2]" || echo "0")
echo "已注册的 kvserver 节点: $count/3"

if [ "$count" -lt 3 ]; then
    echo "⚠ 警告: 服务注册不完整，可能影响压测"
fi

echo ""
echo "[6/7] 显示当前 Leader 状态..."
sleep 1
echo ""
echo "=== kvserver 日志摘要 ==="
for i in 0 1 2; do
    echo "--- 节点 $i ---"
    grep -E "(选举|Leader|term)" kvserver$i.log 2>/dev/null | tail -3
done

echo ""
echo "[7/7] 执行压测（4线程，100操作）..."
echo "========================================"
./kvclient -i ../myRPC/conf/myrpc.conf -- --bench --ops 100 --threads 4

echo ""
echo "========================================"
echo "   压测完成！"
echo "========================================"
echo ""
echo "日志文件: kvserver0.log kvserver1.log kvserver2.log"
echo "查看 Leader 状态: grep Leader kvserver*.log"

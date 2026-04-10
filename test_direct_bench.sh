#!/bin/bash
# test_direct_bench.sh - 直连模式的 Raft KV 压测脚本
# 使用方法: cd build && bash ../test_direct_bench.sh

set -e
cd /home/xianyu-sheng/MyKV_storageBase_Raft_cpp/build

echo "========================================"
echo "   Raft KV 分布式存储 - 直连压测脚本"
echo "========================================"

echo ""
echo "[1/6] 清理旧进程和日志..."
pkill -9 kvserver 2>/dev/null || true
pkill -9 kvclient 2>/dev/null || true
sleep 2
rm -rf *.log raft_persist/
mkdir -p raft_persist

echo ""
echo "[2/6] 启动 kvserver 集群（3节点，直连模式）..."
# 直连模式：不配置 ZooKeeper，服务端跳过服务注册
RAFT_ME=0 ./kvserver -i ../myRPC/conf/myrpc_0.conf >kvserver0.log 2>&1 &
RAFT_ME=1 ./kvserver -i ../myRPC/conf/myrpc_1.conf >kvserver1.log 2>&1 &
RAFT_ME=2 ./kvserver -i ../myRPC/conf/myrpc_2.conf >kvserver2.log 2>&1 &

echo "等待服务启动..."
sleep 5

echo ""
echo "[3/6] 等待 Leader 选举稳定（最长等待 60 秒）..."
STABLE=false
for i in {1..20}; do
    sleep 3

    # 获取最新 Term 值
    term1=$(grep -h "term{" kvserver0.log 2>/dev/null | tail -1 | grep -oP 'term\{\K[0-9]+' || echo "0")
    sleep 1
    term2=$(grep -h "term{" kvserver0.log 2>/dev/null | tail -1 | grep -oP 'term\{\K[0-9]+' || echo "0")

    if [ "$term1" == "$term2" ] && [ "$term1" != "0" ]; then
        # 再等一个心跳周期确保稳定
        sleep 2
        term3=$(grep -h "term{" kvserver0.log 2>/dev/null | tail -1 | grep -oP 'term\{\K[0-9]+' || echo "0")
        if [ "$term2" == "$term3" ]; then
            echo "✓ Leader 选举稳定！Term = $term1（等待 $((i*3)) 秒）"
            STABLE=true
            break
        fi
    fi

    if [ $i -le 5 ]; then
        echo "  等待选举完成... ($((i*3))/60 秒) Term: $term1 -> $term2"
    else
        echo "  ⚠ Term 仍在变化: $term1 -> $term2（$((i*3))/60 秒）"
    fi
done

if [ "$STABLE" == "false" ]; then
    echo ""
    echo "⚠ 警告: Leader 选举尚未完全稳定"
    echo "  继续尝试压测..."
fi

echo ""
echo "[4/6] 显示当前 Leader 状态..."
echo ""
echo "=== kvserver 日志摘要 ==="
for i in 0 1 2; do
    echo "--- 节点 $i ---"
    grep -E "(选举|Leader|term)" kvserver$i.log 2>/dev/null | tail -3
done

echo ""
echo "[5/6] 验证服务端 RPC 端口..."
for i in 0 1 2; do
    port=$(grep "RPCProvider start service" kvserver$i.log 2>/dev/null | grep -oP 'port:\K[0-9]+' || echo "N/A")
    echo "  节点 $i 监听端口: $port"
done

echo ""
echo "[6/6] 执行压测（4线程，100操作）..."
echo "========================================"
./kvclient -i ../myRPC/conf/myrpc_direct.conf -- --bench --ops 100 --threads 4

echo ""
echo "========================================"
echo "   压测完成！"
echo "========================================"
echo ""
echo "日志文件: kvserver0.log kvserver1.log kvserver2.log"
echo "查看 Leader 状态: grep Leader kvserver*.log"

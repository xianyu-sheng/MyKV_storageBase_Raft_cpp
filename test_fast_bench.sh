#!/bin/bash
# test_fast_bench.sh - 快速 Raft KV 压测脚本
# 优化点：
#   1. 智能就绪检测（减少等待时间）
#   2. 快速预热（减少 ops）
#   3. 跳过不必要的检查
# 使用方法: cd build && bash ../test_fast_bench.sh [--ops 100] [--threads 4]

set -e
cd /home/xianyu-sheng/MyKV_storageBase_Raft_cpp/build

# 解析参数
OPS=${1:-100}
THREADS=${2:-4}

echo "========================================"
echo "   Raft KV - 快速压测模式"
echo "   ops=$OPS, threads=$THREADS"
echo "========================================"

echo ""
echo "[1/5] 清理旧进程..."
pkill -9 kvserver 2>/dev/null || true
pkill -9 kvclient 2>/dev/null || true
sleep 1
rm -rf *.log raft_persist/
mkdir -p raft_persist

echo ""
echo "[2/5] 启动 ZooKeeper..."
zkServer.sh start 2>/dev/null || true
sleep 1

echo ""
echo "[3/5] 启动 kvserver 集群（3节点）..."
RAFT_ME=0 ./kvserver -i ../myRPC/conf/myrpc_0.conf >kvserver0.log 2>&1 &
RAFT_ME=1 ./kvserver -i ../myRPC/conf/myrpc_1.conf >kvserver1.log 2>&1 &
RAFT_ME=2 ./kvserver -i ../myRPC/conf/myrpc_2.conf >kvserver2.log 2>&1 &
sleep 2

echo ""
echo "[4/5] 智能检测 Leader 就绪..."
# 快速检测：检查日志中的 Leader 标识
WAIT_COUNT=0
MAX_WAIT=15
while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    # 检查任意节点的日志中是否出现 Leader 标识
    for i in 0 1 2; do
        if grep -q "Leader" kvserver$i.log 2>/dev/null; then
            # 获取当前 term，确认稳定
            term=$(grep -h "term{" kvserver*.log 2>/dev/null | tail -1 | grep -oP 'term\{\K[0-9]+' || echo "0")
            if [ "$term" != "0" ]; then
                echo "✓ Leader 已就绪！Term = $term（等待 ${WAIT_COUNT}s）"
                # 再等一个心跳确保稳定
                sleep 1
                break 2
            fi
        fi
    done
    sleep 1
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

if [ $WAIT_COUNT -ge $MAX_WAIT ]; then
    echo "⚠ 等待超时，尝试继续压测..."
fi

echo ""
echo "[5/5] 执行压测..."
echo "========================================"
./kvclient -i ../myRPC/conf/myrpc.conf -- --bench --ops $OPS --threads $THREADS
echo "========================================"

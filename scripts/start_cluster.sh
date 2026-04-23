#!/bin/bash
#
# start_cluster.sh — 启动本地 3 节点 Raft KV 集群
#
# 用法：
#   ./start_cluster.sh start       启动集群
#   ./start_cluster.sh stop        停止集群
#   ./start_cluster.sh status      查看节点状态
#   ./start_cluster.sh tail        各节点最新日志
#   ./start_cluster.sh logs        实时 tail -f
#   ./start_cluster.sh wait-leader 等待 Leader 选举完成
#   ./start_cluster.sh restart     重启
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# 脚本在 scripts/ 子目录下，../ 即为项目根目录
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BUILD_DIR="${PROJECT_DIR}/build"
CONF_DIR="${PROJECT_DIR}/myRPC/conf"
LOG_DIR="${SCRIPT_DIR}/logs/cluster"
PID_DIR="${SCRIPT_DIR}/logs/pids"

mkdir -p "${LOG_DIR}" "${PID_DIR}"

declare -A NODE_PORT=([0]="8001" [1]="8002" [2]="8003")
declare -A NODE_CONF=([0]="myrpc_0.conf" [1]="myrpc_1.conf" [2]="myrpc_2.conf")

is_running() {
    local node=$1
    [ -f "${PID_DIR}/node_${node}.pid" ] && kill -0 "$(cat "${PID_DIR}/node_${node}.pid")" 2>/dev/null
}

do_start() {
    echo "=========================================="
    echo "  启动 3 节点 Raft KV 集群"
    echo "=========================================="

    for node in 0 1 2; do
        if is_running $node; then
            echo "  [节点 ${node}] 已在运行 (PID=$(cat ${PID_DIR}/node_${node}.pid))"
            continue
        fi

        local log_file="${LOG_DIR}/node_${node}.log"
        local pid_file="${PID_DIR}/node_${node}.pid"
        local conf_file="${CONF_DIR}/${NODE_CONF[$node]}"
        local node_workdir="${BUILD_DIR}/node_${node}_work"
        local persist_dir="${BUILD_DIR}/raft_persist_node${node}"

        mkdir -p "${node_workdir}" "${persist_dir}"
        if [ ! -L "${node_workdir}/raft_persist" ]; then
            ln -sfn "${persist_dir}" "${node_workdir}/raft_persist"
        fi

        local kvserver_bin="${BUILD_DIR}/kvserver"
        echo "  [节点 ${node}] 端口=${NODE_PORT[$node]}, 持久化=${persist_dir}, 配置=${conf_file##*/}"

        cd "${node_workdir}"
        RAFT_ME=${node} "${kvserver_bin}" \
            -i "${conf_file}" \
            > "${log_file}" 2>&1 &
        cd - >/dev/null

        local pid=$!
        echo $pid > "${pid_file}"
        echo "  [节点 ${node}] PID=${pid}, 工作目录=${node_workdir}, 日志=${log_file}"
    done

    echo ""
    echo "  等待各节点启动并完成 Leader 选举..."
    sleep 10

    echo ""
    echo "  === 各节点状态 ==="
    do_status

    echo ""
    echo "=========================================="
    echo "  集群启动完成"
    echo "=========================================="
}

do_stop() {
    echo "  停止 3 节点 Raft KV 集群..."
    for node in 0 1 2; do
        if [ -f "${PID_DIR}/node_${node}.pid" ]; then
            local pid=$(cat "${PID_DIR}/node_${node}.pid")
            if kill -0 $pid 2>/dev/null; then
                echo "  [节点 ${node}] SIGTERM PID=${pid}..."
                kill $pid 2>/dev/null
                sleep 1
                kill -9 $pid 2>/dev/null || true
            fi
            rm -f "${PID_DIR}/node_${node}.pid"
        fi
    done
    echo "  停止完成"
}

do_status() {
    echo "  节点  端口    PID         状态"
    echo "  ----  ------  ----------  ----------"
    for node in 0 1 2; do
        local pid_file="${PID_DIR}/node_${node}.pid"
        local status="未运行"
        local pid="-"

        if [ -f "${pid_file}" ]; then
            pid=$(cat "${pid_file}")
            if kill -0 $pid 2>/dev/null; then
                status="运行中"
            else
                status="已退出"
            fi
        fi

        printf "  [%d]    %s    %-9s  %s\n" $node "${NODE_PORT[$node]}" "$pid" "$status"
    done

    echo ""
    echo "  === Raft 选举关键日志 ==="
    for node in 0 1 2; do
        local log="${LOG_DIR}/node_${node}.log"
        if [ -f "$log" ]; then
            local last=$(tail -2 "$log" 2>/dev/null | tr '\n' '|')
            printf "  节点%d: %s\n" $node "${last%|}"
        fi
    done
}

do_tail() {
    echo "=== 各节点最新日志 ==="
    for node in 0 1 2; do
        echo ""
        echo ">>> 节点 ${node} (端口 ${NODE_PORT[$node]}) <<<"
        tail -15 "${LOG_DIR}/node_${node}.log" 2>/dev/null || echo "(无日志)"
    done
}

do_logs() {
    echo "=== 实时日志 tail -f（Ctrl+C 退出）==="
    tail -f "${LOG_DIR}/node_0.log" "${LOG_DIR}/node_1.log" "${LOG_DIR}/node_2.log"
}

do_wait_leader() {
    echo "  等待 Leader 选举完成（最多 30s）..."
    local waited=0
    while [ $waited -lt 30 ]; do
        for node in 0 1 2; do
            if grep -q "become leader\|becomeLeader\|is leader\|I am the leader\|选为 leader\|I am leader" \
                "${LOG_DIR}/node_${node}.log" 2>/dev/null; then
                echo "  Leader 已产生！节点 ${node}"
                return 0
            fi
        done
        sleep 1
        waited=$((waited + 1))
        echo -n "."
    done
    echo ""
    echo "  [警告] 30s 内未检测到 Leader，运行 'tail' 检查日志"
    return 1
}

# ========== 主入口 ==========

case "${1:-start}" in
    start)   do_start ;;
    stop)    do_stop ;;
    status)  do_status ;;
    tail)    do_tail ;;
    logs)    do_logs ;;
    wait-leader) do_wait_leader ;;
    restart)
        do_stop
        sleep 2
        do_start
        ;;
    *)
        echo "用法: $0 {start|stop|status|tail|logs|wait-leader|restart}"
        exit 1
        ;;
esac

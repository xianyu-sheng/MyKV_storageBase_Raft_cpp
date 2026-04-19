#!/bin/bash
#
# run.sh - Raft KV 分布式存储系统启动与测试脚本
# ==============================================================
#
# 提供集群启停、编译、压测等基础操作。
# 智能分析与优化建议请使用 SmartBench 工具。
#
# 用法:
#   ./run.sh bench                    快速压测（4线程，100操作，30%写）
#   ./run.sh bench --ops 200 --threads 8 --write-ratio 30   指定参数
#   ./run.sh start              启动集群（3节点+ZooKeeper）
#   ./run.sh stop               停止集群
#   ./run.sh restart            重启集群
#   ./run.sh rebuild            重新编译
#   ./run.sh clean              清理构建产物和日志
#   ./run.sh logs [n|all]       查看日志
#   ./run.sh status             查看集群状态
#   ./run.sh help               显示帮助
#
# 智能分析 & 优化建议:
#   请使用 SmartBench 工具:
#     cd /home/xianyu-sheng/SmartBench
#     python -m smartbench.cli run
#     python -m smartbench.cli auto-optimize --raft-path /home/xianyu-sheng/MyKV_storageBase_Raft_cpp
#
# ==============================================================

set -e

# ----- 路径配置 -----
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"

# ----- 默认压测参数 -----
DEFAULT_OPS=100
DEFAULT_THREADS=4
DEFAULT_WRITE_RATIO=30

# ----- 颜色输出 -----
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
BOLD='\033[1m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_step()  { echo -e "${CYAN}[STEP]${NC} $1"; }
log_cmd()   { echo -e "${MAGENTA}[CMD]${NC}  $1"; }

check_binary() {
    if [ ! -f "${BUILD_DIR}/kvserver" ] || [ ! -f "${BUILD_DIR}/kvclient" ]; then
        log_error "二进制文件未编译，请先运行: ./run.sh rebuild"
        exit 1
    fi
}

# ================================================================
# 子命令: help
# ================================================================

cmd_help() {
    cat << 'EOF'
==============================================================
  run.sh - Raft KV 分布式存储系统启动与测试脚本
==============================================================

用法:
  ./run.sh <子命令> [参数...]

━━━ 子命令 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  bench [参数...]     快速压测（默认）
                        参数:
                          --ops N          总操作数 (默认: 100)
                          --threads T      并发线程数 (默认: 4)
                          --write-ratio W  写操作占比 0-100 (默认: 30)

  start               启动集群（3节点 + ZooKeeper）
  stop                停止集群
  restart             重启集群

  rebuild             重新编译（cmake + make）
  clean               清理构建产物和日志

  logs [n|all]        查看日志
                          n=0,1,2  查看指定节点日志
                          all      查看所有节点日志

  status              查看集群 Leader 选举状态

  help                显示本帮助信息

━━━ 智能分析与优化建议 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  智能压测与优化建议请使用 SmartBench 工具:

    cd /home/xianyu-sheng/SmartBench
    python -m smartbench.cli run --system raft_kv
    python -m smartbench.cli auto-optimize \
        --raft-path /home/xianyu-sheng/MyKV_storageBase_Raft_cpp \
        --target-qps 500

━━━ 性能参考 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  4 线程 | 100 ops | 30% 写:  QPS ~347 | 平均延迟 ~1.4ms | P99 ~6.4ms
  1 线程 | 100 ops | 30% 写:  QPS ~98  | 平均延迟 ~10ms  | P99 ~18ms
  推荐配置: 4 线程

==============================================================
EOF
}

# ================================================================
# 子命令: rebuild
# ================================================================

cmd_rebuild() {
    log_step "重新编译 Raft KV 分布式存储系统..."
    cd "${BUILD_DIR}"
    log_cmd "cmake .. && make -j$(nproc)"
    cmake .. && make -j$(nproc)
    log_info "编译完成"
}

# ================================================================
# 子命令: clean
# ================================================================

cmd_clean() {
    log_step "清理构建产物..."
    rm -rf "${BUILD_DIR}"/*.log
    rm -rf "${BUILD_DIR}"/raft_persist/*
    rm -rf "${BUILD_DIR}"/log
    rm -rf "${BUILD_DIR}"/CMakeCache.txt
    rm -rf "${BUILD_DIR}"/cmake_install.cmake
    rm -rf "${BUILD_DIR}"/Makefile
    rm -rf "${BUILD_DIR}"/CMakeFiles
    rm -rf "${BUILD_DIR}"/CTestTestfile.cmake
    log_info "清理完成"
}

# ================================================================
# 子命令: start
# ================================================================

cmd_start() {
    check_binary
    log_step "启动 ZooKeeper..."
    zkServer.sh start 2>/dev/null || true
    sleep 2

    log_step "清理旧进程和日志..."
    pkill -9 kvserver 2>/dev/null || true
    pkill -9 kvclient 2>/dev/null || true
    sleep 2
    rm -rf "${BUILD_DIR}"/*.log
    rm -rf "${BUILD_DIR}"/raft_persist/
    mkdir -p "${BUILD_DIR}"/raft_persist

    log_step "启动 kvserver 集群（3节点）..."
    cd "${BUILD_DIR}"
    RAFT_ME=0 ./kvserver -i ../myRPC/conf/myrpc_0.conf >kvserver0.log 2>&1 &
    RAFT_ME=1 ./kvserver -i ../myRPC/conf/myrpc_1.conf >kvserver1.log 2>&1 &
    RAFT_ME=2 ./kvserver -i ../myRPC/conf/myrpc_2.conf >kvserver2.log 2>&1 &
    sleep 3

    log_step "等待 Leader 选举稳定（最长 30 秒）..."
    STABLE=false
    for i in {1..10}; do
        sleep 3
        term1=$(grep -h "term{" "${BUILD_DIR}"/kvserver*.log 2>/dev/null | tail -1 | grep -oP 'term\{\K[0-9]+' || echo "0")
        sleep 1
        term2=$(grep -h "term{" "${BUILD_DIR}"/kvserver*.log 2>/dev/null | tail -1 | grep -oP 'term\{\K[0-9]+' || echo "0")
        if [ "$term1" == "$term2" ] && [ "$term1" != "0" ]; then
            log_info "Leader 选举稳定！Term = $term1（等待 $((i*3)) 秒）"
            STABLE=true
            break
        fi
        echo "  等待选举完成... ($((i*3))/30 秒) Term: $term1 -> $term2"
    done

    if [ "$STABLE" == "false" ]; then
        log_warn "Leader 选举尚未完全稳定，继续..."
    fi

    log_info "集群启动完成"
}

# ================================================================
# 子命令: stop
# ================================================================

cmd_stop() {
    log_step "停止 kvserver 集群..."
    pkill -9 kvserver 2>/dev/null || true
    pkill -9 kvclient 2>/dev/null || true
    log_info "集群已停止"
}

# ================================================================
# 子命令: restart
# ================================================================

cmd_restart() {
    cmd_stop
    sleep 2
    cmd_start
}

# ================================================================
# 子命令: bench
# ================================================================

cmd_bench() {
    check_binary

    OPS="${DEFAULT_OPS}"
    THREADS="${DEFAULT_THREADS}"
    WRITE_RATIO="${DEFAULT_WRITE_RATIO}"

    while [ $# -gt 0 ]; do
        case "$1" in
            --ops)          OPS="$2"; shift 2 ;;
            --threads)      THREADS="$2"; shift 2 ;;
            --write-ratio)  WRITE_RATIO="$2"; shift 2 ;;
            --*)            shift ;;
            *)              shift ;;
        esac
    done

    log_step "压测: ops=${OPS}, threads=${THREADS}, write-ratio=${WRITE_RATIO}%"
    echo ""
    echo "========================================"
    echo "   Raft KV 压测"
    echo "   ops=${OPS} | threads=${THREADS} | 写占比=${WRITE_RATIO}%"
    echo "========================================"

    # 每次压测前都初始化集群状态，确保干净
    log_step "初始化集群状态..."
    pkill -9 kvserver 2>/dev/null || true
    pkill -9 kvclient 2>/dev/null || true
    sleep 2
    rm -rf "${BUILD_DIR}"/*.log
    rm -rf "${BUILD_DIR}"/raft_persist/
    mkdir -p "${BUILD_DIR}"/raft_persist

    log_step "启动 ZooKeeper..."
    zkServer.sh start 2>/dev/null || true
    sleep 2

    log_step "启动 kvserver 集群（3节点）..."
    cd "${BUILD_DIR}"
    RAFT_ME=0 ./kvserver -i ../myRPC/conf/myrpc_0.conf >kvserver0.log 2>&1 &
    RAFT_ME=1 ./kvserver -i ../myRPC/conf/myrpc_1.conf >kvserver1.log 2>&1 &
    RAFT_ME=2 ./kvserver -i ../myRPC/conf/myrpc_2.conf >kvserver2.log 2>&1 &
    sleep 3

    log_step "等待 Leader 选举稳定（最长 30 秒）..."
    STABLE=false
    for i in {1..10}; do
        sleep 3
        term1=$(grep -h "term{" "${BUILD_DIR}"/kvserver*.log 2>/dev/null | tail -1 | grep -oP 'term\{\K[0-9]+' || echo "0")
        sleep 1
        term2=$(grep -h "term{" "${BUILD_DIR}"/kvserver*.log 2>/dev/null | tail -1 | grep -oP 'term\{\K[0-9]+' || echo "0")
        if [ "$term1" == "$term2" ] && [ "$term1" != "0" ]; then
            log_info "Leader 选举稳定！Term = $term1（等待 $((i*3)) 秒）"
            STABLE=true
            break
        fi
        echo "  等待选举完成... ($((i*3))/30 秒) Term: $term1 -> $term2"
    done

    [ "$STABLE" == "false" ] && log_warn "Leader 选举尚未完全稳定，继续..."

    # 额外等待 ZK ephemeral 节点注册完成
    sleep 5

    log_step "执行压测..."
    log_cmd "./kvclient -i ../myRPC/conf/myrpc.conf -- --bench --ops ${OPS} --threads ${THREADS} --write-ratio ${WRITE_RATIO}"
    ./kvclient -i ../myRPC/conf/myrpc.conf -- --bench --ops ${OPS} --threads ${THREADS} --write-ratio ${WRITE_RATIO}
    echo ""
    log_info "压测完成"
}

# ================================================================
# 子命令: logs
# ================================================================

cmd_logs() {
    if [ "$1" == "all" ] || [ -z "$1" ]; then
        echo "=== kvserver0.log ==="
        tail -50 "${BUILD_DIR}/kvserver0.log" 2>/dev/null || echo "文件不存在"
        echo ""
        echo "=== kvserver1.log ==="
        tail -50 "${BUILD_DIR}/kvserver1.log" 2>/dev/null || echo "文件不存在"
        echo ""
        echo "=== kvserver2.log ==="
        tail -50 "${BUILD_DIR}/kvserver2.log" 2>/dev/null || echo "文件不存在"
    else
        echo "=== kvserver${1}.log ==="
        tail -100 "${BUILD_DIR}/kvserver${1}.log" 2>/dev/null || echo "文件不存在"
    fi
}

# ================================================================
# 子命令: status
# ================================================================

cmd_status() {
    echo ""
    echo "========================================"
    echo "   Raft KV 集群状态"
    echo "========================================"

    echo ""
    echo "━━━ 进程状态 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    for proc in kvserver kvclient; do
        count=$(pgrep -x ${proc} 2>/dev/null | wc -l)
        echo "  ${proc}: ${count} 个进程"
    done

    echo ""
    echo "━━━ Leader 选举状态 ━━━━━━━━━━━━━━━━━━━━━"
    for i in 0 1 2; do
        log_file="${BUILD_DIR}/kvserver${i}.log"
        if [ -f "$log_file" ]; then
            echo "  节点 $i:"
            grep -E "(Leader|term)" "$log_file" 2>/dev/null | tail -3 | sed 's/^/    /'
        else
            echo "  节点 $i: 日志文件不存在"
        fi
    done
}

# ================================================================
# 主入口
# ================================================================

SUBCOMMAND="${1:-bench}"
shift 2>/dev/null || true

case "$SUBCOMMAND" in
    bench|benchmark|b)
        cmd_bench "$@"
        ;;
    start)
        cmd_start
        ;;
    stop)
        cmd_stop
        ;;
    restart|reboot)
        cmd_restart
        ;;
    rebuild|build|compile)
        cmd_rebuild
        ;;
    clean|cleanup)
        cmd_clean
        ;;
    logs|log|tail)
        cmd_logs "$1"
        ;;
    status|stat)
        cmd_status
        ;;
    help|-h|--help)
        cmd_help
        ;;
    *)
        log_error "未知子命令: $SUBCOMMAND"
        echo ""
        echo "运行 './run.sh help' 查看可用子命令"
        exit 1
        ;;
esac

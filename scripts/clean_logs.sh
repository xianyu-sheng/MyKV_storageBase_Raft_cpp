#!/usr/bin/env bash
# 清理 kvserver 日志文件，防止日志无限增长。
# 约定：
# - 在源码根目录和 build 目录下查找 kvserver{0,1,2}.log
# - 每次运行脚本时，直接将这些日志文件 truncate 为 0（保留文件名）。

set -euo pipefail

# 仓库根目录：scripts/clean_logs.sh 的上一级
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# build 目录可以通过环境变量覆盖，默认使用 ROOT_DIR/build
BUILD_DIR="${BUILD_DIR:-"$ROOT_DIR/build"}"

LOG_DIRS=("$ROOT_DIR" "$BUILD_DIR")
LOG_FILES=("kvserver0.log" "kvserver1.log" "kvserver2.log")

for dir in "${LOG_DIRS[@]}"; do
  for name in "${LOG_FILES[@]}"; do
    f="$dir/$name"
    if [[ -f "$f" ]]; then
      # 使用重定向 truncate 文件，保留 inode，方便 tail -f 等继续工作
      : > "$f"
      echo "[clean_logs] truncated $f"
    fi
  done
done

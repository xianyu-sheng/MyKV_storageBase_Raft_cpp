#!/usr/bin/env python3
import sys
import os
import socket
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROTO_DIR = os.path.join(SCRIPT_DIR, "proto_gen")
sys.path.insert(0, PROTO_DIR)

import Krpcheader_pb2
import KvServerRPC_pb2
import numpy as np

ALL_PORTS = [8001, 8002, 8003]

def encode_varint(value):
    result = bytearray()
    while value > 0x7F:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value & 0x7F)
    return bytes(result)

def send_rpc(host, port, service, method, args_bytes, timeout_ms=10000):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(max(timeout_ms / 1000.0, 5.0))
    sock.connect((host, port))

    header = Krpcheader_pb2.KrpcHeader()
    header.service_name = service.encode("utf-8")
    header.method_name = method.encode("utf-8")
    header.args_size = len(args_bytes)
    header_bytes = header.SerializeToString()

    message = encode_varint(len(header_bytes)) + header_bytes + args_bytes
    sock.sendall(message)

    response = b""
    deadline = time.time() + max(timeout_ms / 1000.0, 5.0)
    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        sock.settimeout(min(remaining, 2.0))
        try:
            chunk = sock.recv(65536)
        except socket.timeout:
            break
        if not chunk:
            break
        response += chunk
        if len(response) >= 4:
            break
    sock.close()
    if not response:
        raise RuntimeError("server closed connection")
    return response

def search_rpc(host, port, query_vector, top_k):
    args = KvServerRPC_pb2.SearchRequest()
    for v in query_vector:
        args.query_vector.append(v)
    args.top_k = top_k
    args.search_type = "inner_product"
    args_bytes = args.SerializeToString()
    resp_bytes = send_rpc(host, port, "kvServerRpc", "Search", args_bytes)
    resp = KvServerRPC_pb2.SearchResponse()
    resp.ParseFromString(resp_bytes)
    item_ids = list(resp.item_ids)
    scores = list(resp.scores)
    search_time_us = int(resp.search_time_us)
    return item_ids, scores, search_time_us

def pct(data, p):
    s = sorted(data)
    idx = int(len(s) * p / 100.0)
    if idx >= len(s):
        idx = len(s) - 1
    return s[idx]

def run_stress(host, port, n_iterations, top_k):
    rng = np.random.default_rng(12345)
    print("")
    print("=" * 60)
    print(f"  高频 Search 压测 - {n_iterations} 次搜索, Top-K={top_k}")
    print("=" * 60)
    print(f"  目标: {host}:{port}")
    print("")
    latencies = []
    failures = 0
    result_sets = []
    print("  执行中...", flush=True)
    t0 = time.time()
    for i in range(n_iterations):
        q = [float(rng.random()) for _ in range(128)]
        try:
            item_ids, scores, st = search_rpc(host, port, q, top_k)
            latencies.append(st)
            result_sets.append(tuple(item_ids))
            if (i + 1) % 20 == 0:
                avg = sum(latencies[-20:]) / 20
                print(f"    进度: {i+1}/{n_iterations}  当前平均延迟: {avg:.1f} us")
        except Exception as e:
            failures += 1
            print(f"    第 {i} 次失败: {e}")
    t1 = time.time()
    elapsed = t1 - t0
    total_ops = len(latencies)
    qps = total_ops / elapsed if elapsed > 0 else 0
    if not latencies:
        print("  FAIL: 无有效数据")
        return False
    print("")
    print(f"  统计结果:")
    print(f"    总执行次数: {n_iterations}")
    print(f"    成功次数:   {total_ops}")
    print(f"    失败次数:   {failures}")
    print(f"    成功率:     {total_ops/n_iterations*100:.1f}%")
    print(f"    QPS:        {qps:.0f} ops/s")
    print(f"    延迟 (us):")
    print(f"      min:     {min(latencies)}")
    print(f"      avg:     {sum(latencies)/len(latencies):.1f}")
    print(f"      max:     {max(latencies)}")
    print(f"      P50:     {pct(latencies, 50)}")
    print(f"      P90:     {pct(latencies, 90)}")
    print(f"      P95:     {pct(latencies, 95)}")
    print(f"      P99:     {pct(latencies, 99)}")
    unique = len(set(result_sets))
    print(f"")
    print(f"  多样性检查:")
    print(f"    不同结果组合数: {unique}/{total_ops}")
    return True

def run_topk_test(host, port):
    print("")
    print("=" * 60)
    print("  不同 Top-K 参数测试")
    print("=" * 60)
    rng = np.random.default_rng(999)
    q = [float(rng.random()) for _ in range(128)]
    for topk in [5, 10, 20, 50]:
        try:
            item_ids, scores, st = search_rpc(host, port, q, topk)
            print(f"  Top-{topk:2d}: 返回 {len(item_ids):2d} 条, 延迟 {st:5d} us, "
                  f"score=[{min(scores):.3f}, {max(scores):.3f}]")
        except Exception as e:
            print(f"  Top-{topk:2d}: FAIL - {e}")

def run_all_nodes_test(host):
    print("")
    print("=" * 60)
    print("  所有节点 Search 延迟对比")
    print("=" * 60)
    rng = np.random.default_rng(777)
    q = [float(rng.random()) for _ in range(128)]
    results = {}
    for port in ALL_PORTS:
        lats = []
        for _ in range(30):
            try:
                _, _, st = search_rpc(host, port, q, 10)
                lats.append(st)
            except Exception as e:
                print(f"  端口 {port}: 错误 - {e}")
                break
        if lats:
            results[port] = lats
            avg = sum(lats) / len(lats)
            p99 = pct(lats, 99)
            print(f"  端口 {port}: avg={avg:.1f}us  P99={p99}us  ({len(lats)}次)")
    if len(results) >= 2:
        avgs = {p: sum(v)/len(v) for p, v in results.items()}
        vals = list(avgs.values())
        max_diff_pct = 0.0
        for i in range(len(vals)):
            for j in range(i+1, len(vals)):
                diff = abs(vals[i] - vals[j]) / max(vals[i], vals[j]) * 100
                if diff > max_diff_pct:
                    max_diff_pct = diff
        status = "正常（差异<20%）" if max_diff_pct < 20 else "异常（差异过大）"
        print(f"  节点间最大延迟差异: {max_diff_pct:.1f}% ({status})")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--topk", type=int, default=10)
    args = parser.parse_args()

    print("#" * 60)
    print("#  Search 高频压测开始")
    print("#" * 60)

    run_stress(args.host, args.port, args.iterations, args.topk)
    run_topk_test(args.host, args.port)
    run_all_nodes_test(args.host)

    print("")
    print("=" * 60)
    print("  压测完成")
    print("=" * 60)

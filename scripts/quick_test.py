#!/usr/bin/env python3
"""
quick_test.py — 快速测试脚本
测试内容：
1. 写入少量特征数据
2. 验证 Search 功能（亚毫秒召回）
3. 多节点一致性验证
"""

import sys
import os
import socket
import struct
import time
import argparse

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROTO_DIR = os.path.join(SCRIPT_DIR, "proto_gen")
sys.path.insert(0, PROTO_DIR)

import Krpcheader_pb2
import KvServerRPC_pb2

ALL_PORTS = [8001, 8002, 8003]

def encode_varint(value: int) -> bytes:
    result = bytearray()
    while value > 0x7F:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value & 0x7F)
    return bytes(result)

def send_rpc(host: str, port: int, service: str, method: str, args_bytes: bytes, timeout_ms: int = 10000) -> bytes:
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
        raise RuntimeError(f"server closed connection on {port}")
    return response

def put_feature(host: str, port: int, item_id: str, embedding, client_id: str, request_id: int) -> tuple:
    args = KvServerRPC_pb2.PutFeatureArgs()
    args.feature.item_id = item_id
    for v in embedding:
        args.feature.embedding.append(v)
    args.feature.timestamp = int(time.time() * 1000)
    args.ClientId = client_id.encode("utf-8")
    args.RequestId = request_id

    resp_bytes = send_rpc(host, port, "kvServerRpc", "PutFeature", args.SerializeToString())
    resp = KvServerRPC_pb2.PutFeatureReply()
    resp.ParseFromString(resp_bytes)
    err = resp.Err.decode("utf-8") if isinstance(resp.Err, bytes) else str(resp.Err)
    return err == "OK", err

def search(host: str, port: int, query_vector, top_k: int) -> tuple:
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

def find_leader(host: str) -> int:
    """通过轮询找到当前 Leader 端口"""
    for port in ALL_PORTS:
        try:
            # 尝试一个无意义的请求来探测
            args = KvServerRPC_pb2.PutFeatureArgs()
            args.feature.item_id = "__probe__"
            for _ in range(128):
                args.feature.embedding.append(0.0)
            args.ClientId = b"probe"
            args.RequestId = 0
            resp_bytes = send_rpc(host, port, "kvServerRpc", "PutFeature", args.SerializeToString(), timeout_ms=2000)
            resp = KvServerRPC_pb2.PutFeatureReply()
            resp.ParseFromString(resp_bytes)
            err = resp.Err.decode("utf-8") if isinstance(resp.Err, bytes) else str(resp.Err)
            if err == "OK":
                return port
        except Exception:
            continue
    return None

def run_test(host: str, n_items: int, top_k: int):
    import numpy as np

    print(f"\n{'='*60}")
    print(f"  快速测试 — {n_items} 条写入 + Search 验证")
    print(f"{'='*60}")
    print(f"  目标: {host}:{ALL_PORTS}")

    # Step 1: 找 Leader
    print(f"\n[1/5] 查找 Leader...")
    leader_port = find_leader(host)
    if leader_port is None:
        print("  FAIL: 无法连接任何节点")
        return False
    print(f"  Leader: 端口 {leader_port}")

    # Step 2: 写入数据
    print(f"\n[2/5] 写入 {n_items} 条特征数据...")
    rng = np.random.default_rng(42)
    ok_count = 0
    fail_count = 0
    errors = []

    t0 = time.time()
    for i in range(n_items):
        embedding = [float(rng.random()) for _ in range(128)]
        ok, err = put_feature(host, leader_port, f"item_{i}", embedding,
                               f"test_client_{i//100}", i % 100)
        if ok:
            ok_count += 1
        else:
            fail_count += 1
            if len(errors) < 5:
                errors.append(f"item_{i}: {err}")
        if (i + 1) % 100 == 0:
            print(f"  进度: {i+1}/{n_items}  OK: {ok_count}  FAIL: {fail_count}")

    t1 = time.time()
    elapsed = t1 - t0
    qps = ok_count / elapsed if elapsed > 0 else 0

    print(f"\n  写入完成: {ok_count}/{n_items} 成功  ({ok_count/n_items*100:.1f}%)")
    print(f"  QPS: {qps:.1f} ops/s")
    if errors:
        for e in errors:
            print(f"  错误: {e}")

    if ok_count == 0:
        print("  FAIL: 无数据可搜，退出")
        return False

    # Step 3: Search 验证（连到 Leader）
    print(f"\n[3/5] Search 验证（连接 Leader 端口 {leader_port}）...")
    query = [float(rng.random()) for _ in range(128)]
    try:
        item_ids, scores, search_time_us = search(host, leader_port, query, top_k)
        print(f"  Search 返回 {len(item_ids)} 条结果")
        print(f"  纯 HNSW 耗时: {search_time_us} us ({search_time_us/1000:.3f} ms)")
        for i, (iid, score) in enumerate(zip(item_ids, scores)):
            print(f"    #{i+1}: {iid}  score={score:.6f}")

        # 验证返回结果的合理性
        if len(item_ids) > 0 and all(iid.startswith("item_") for iid in item_ids):
            print(f"  格式验证: PASS（所有 item_id 格式正确）")
        else:
            print(f"  格式验证: FAIL（返回了非预期格式）")
            return False
    except Exception as e:
        print(f"  Search 失败: {e}")
        return False

    # Step 4: 多节点一致性验证
    print(f"\n[4/5] 多节点一致性验证（Search 遍历所有节点）...")
    all_results = {}
    all_ok = True
    for port in ALL_PORTS:
        try:
            item_ids, scores, search_time_us = search(host, port, query, top_k)
            all_results[port] = (item_ids, scores, search_time_us)
            print(f"  端口 {port}: {len(item_ids)} 条结果, {search_time_us} us")
        except Exception as e:
            print(f"  端口 {port}: 失败 — {e}")
            all_ok = False

    # 对比各节点返回的 item_ids
    if len(all_results) >= 2:
        base_ids = None
        for port, (ids, scores, _) in all_results.items():
            if base_ids is None:
                base_ids = tuple(ids)
            elif tuple(ids) != base_ids:
                print(f"  一致性: WARN（节点 {port} 结果与其他节点不同）")
            else:
                print(f"  一致性: OK（节点 {port} 与其他节点结果一致）")

    # Step 5: 多次 Search 延迟稳定性测试
    print(f"\n[5/5] Search 延迟稳定性测试（50 次）...")
    latencies = []
    for i in range(50):
        q = [float(rng.random()) for _ in range(128)]
        try:
            _, _, st = search(host, leader_port, q, top_k)
            latencies.append(st)
        except Exception as e:
            print(f"  第 {i} 次搜索失败: {e}")
    if latencies:
        avg_lat = sum(latencies) / len(latencies)
        max_lat = max(latencies)
        min_lat = min(latencies)
        # P99
        sorted_lat = sorted(latencies)
        p99_idx = int(len(sorted_lat) * 0.99)
        p99_lat = sorted_lat[p99_idx]

        print(f"  搜索次数: {len(latencies)}")
        print(f"  延迟 (us): min={min_lat}  avg={avg_lat:.1f}  max={max_lat}  P99={p99_lat}")
        if avg_lat < 1000:
            print(f"  结果: PASS（平均延迟 < 1ms）")
        else:
            print(f"  结果: WARN（平均延迟 > 1ms）")
    else:
        print(f"  结果: FAIL（无有效数据）")

    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--items", type=int, default=200, help="写入条数")
    parser.add_argument("--topk", type=int, default=10, help="Search Top-K")
    args = parser.parse_args()

    success = run_test(args.host, args.items, args.topk)
    sys.exit(0 if success else 1)

#!/usr/bin/env python3
"""
gen_features.py — 批量预热工具
通过 myRPC 协议直接向 Raft KV Server 写入 10 万条商品特征数据。

使用方式:
    python3 gen_features.py --total 100000 --batch 200 --workers 8 --host 127.0.0.1 --port 8000
"""

import sys
import os
import struct
import socket
import argparse
import time
import threading
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Optional

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROTO_DIR = os.path.join(SCRIPT_DIR, "proto_gen")
sys.path.insert(0, PROTO_DIR)

import Krpcheader_pb2
import KvServerRPC_pb2


# ============================================================================
# myRPC 协议实现（Python 客户端）
# ============================================================================

def encode_varint(value: int) -> bytes:
    """Protobuf varint 编码"""
    if value < 0:
        raise ValueError("varint cannot be negative")
    result = bytearray()
    while value > 0x7F:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value & 0x7F)
    return bytes(result)


def decode_varint(data: bytes, pos: int) -> Tuple[int, int]:
    """从字节流中解析一个 varint，返回 (value, new_pos)"""
    result = 0
    shift = 0
    while True:
        if pos >= len(data):
            raise EOFError("unexpected end of data while parsing varint")
        byte = data[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if (byte & 0x80) == 0:
            break
        shift += 7
    return result, pos


class MyRpcClient:
    """myRPC 二进制协议客户端（兼容 C++ KrpcChannel 的直连模式）"""

    RECV_SIZE = 65536

    def __init__(self, host: str, port: int, timeout_ms: int = 10000):
        self.host = host
        self.port = port
        self.timeout_s = timeout_ms / 1000.0
        self.ALL_PORTS = [8001, 8002, 8003]  # 用于 Search 时尝试所有节点

    def _send_request(self, service_name: str, method_name: str,
                      args_bytes: bytes, timeout_ms: int = 30000) -> bytes:
        """
        按照 myRPC 协议发送请求:
          [varint: header_size][header_size bytes: KrpcHeader]
          [args_bytes]
        接收原始响应 protobuf 字节流并返回。
        使用短连接：每次请求独立 socket。
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(max(timeout_ms / 1000.0, 5.0))
        sock.connect((self.host, self.port))

        header = Krpcheader_pb2.KrpcHeader()
        header.service_name = service_name.encode("utf-8")
        header.method_name = method_name.encode("utf-8")
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
                break  # 有数据了就不再等待

        sock.close()
        if not response:
            raise RuntimeError("server closed connection")
        return response

    def close(self):
        pass  # 短连接无状态，保留接口兼容

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    # ========================================================================
    # kvServerRpc 服务的 RPC 方法封装
    # ========================================================================

    def put_feature(self, client_id: str, request_id: int,
                    item_id: str, embedding: List[float],
                    timestamp: int = None) -> Tuple[str, str]:
        """PutFeature RPC"""
        args = KvServerRPC_pb2.PutFeatureArgs()
        args.feature.item_id = item_id
        for v in embedding:
            args.feature.embedding.append(v)
        if timestamp is not None:
            args.feature.timestamp = timestamp
        args.ClientId = client_id.encode("utf-8")
        args.RequestId = request_id

        resp_bytes = self._send_request(
            "kvServerRpc", "PutFeature",
            args.SerializeToString())

        resp = KvServerRPC_pb2.PutFeatureReply()
        resp.ParseFromString(resp_bytes)
        return resp.Err.decode("utf-8"), resp.Err  # Err is bytes

    def search(self, query_vector: List[float], top_k: int = 10,
               search_type: str = "inner_product") -> Tuple[List[str],
                                                            List[float],
                                                            int]:
        """Search RPC — SearchRequest 不走 Raft，直接查本地 HNSW 索引
        CQRS 架构下任意节点均可响应，依次尝试所有端口"""
        args = KvServerRPC_pb2.SearchRequest()
        for v in query_vector:
            args.query_vector.append(v)
        args.top_k = top_k
        args.search_type = search_type
        args_bytes = args.SerializeToString()

        for port in self.ALL_PORTS:
            try:
                client = MyRpcClient(self.host, port, timeout_ms=10000)
                resp_bytes = client._send_request(
                    "kvServerRpc", "Search", args_bytes, timeout_ms=10000)
                client.close()

                resp = KvServerRPC_pb2.SearchResponse()
                if not resp.ParseFromString(resp_bytes):
                    continue
                # proto3 repeated string 返回 str（无需 decode）
                item_ids = list(resp.item_ids)
                # proto3 repeated float 返回 list[float]
                scores = list(resp.scores)
                search_time = int(resp.search_time_us)
                return item_ids, scores, search_time
            except Exception as e:
                continue

        return [], [], 0


# ============================================================================
# Worker：单个线程负责一个连续的写批次
# ============================================================================

class BatchWriter:
    # 3 节点 Raft 集群的所有端口，客户端会自动发现当前 Leader
    ALL_PORTS = [8001, 8002, 8003]

    def __init__(self, host: str, port: int, worker_id: int):
        self.host = host
        self.port = port
        self.worker_id = worker_id
        self.ok_count = 0
        self.fail_count = 0
        self.latencies: List[float] = []
        self.errors: List[str] = []
        self._lock = threading.Lock()

    def _send_to_leader(self, method: str,
                        build_args_fn,
                        parse_response_fn,
                        max_retries: int = 3) -> Tuple[bool, str]:
        """
        自动发现 Leader 并发送请求。
        在 max_retries 轮内快速轮询所有端口，每次遇到 ErrWrongLeader 就立即切到下一个端口，
        不睡眠，保证每个请求都能在毫秒级找到当前 Leader。
        """
        args_bytes = build_args_fn()

        for retry in range(max_retries):
            tried = []
            for port in self.ALL_PORTS:
                if port in tried:
                    continue
                tried.append(port)

                try:
                    client = MyRpcClient(self.host, port, timeout_ms=10000)
                    resp_bytes = client._send_request(
                        "kvServerRpc", method, args_bytes, timeout_ms=10000)
                    client.close()

                    reply = parse_response_fn(resp_bytes)
                    err = reply.Err.decode("utf-8") if isinstance(reply.Err, bytes) else str(reply.Err)

                    if err == "OK":
                        self._leader_port = port
                        return True, err
                    elif err == "ErrWrongLeader":
                        self._leader_port = None
                        continue  # 立即切到下一个端口
                    else:
                        return False, err
                except Exception:
                    continue

        return False, "NoLeaderFound"

    def write_batch(self, features: List[Tuple[str, List[float], int]]):
        """
        写入一批特征。features 为 List[(item_id, embedding, seq_id)]
        seq_id 用于 client_id，以支持幂等去重。
        自动处理 Leader 切换。
        """
        for item_id, embedding, seq_id in features:
            client_id = f"gen_worker_{self.worker_id}_req_{seq_id}"

            def build_args():
                args = KvServerRPC_pb2.PutFeatureArgs()
                args.feature.item_id = item_id
                for v in embedding:
                    args.feature.embedding.append(v)
                args.feature.timestamp = int(time.time() * 1000)
                args.ClientId = client_id.encode("utf-8")
                args.RequestId = seq_id
                return args.SerializeToString()

            def parse(resp_bytes):
                r = KvServerRPC_pb2.PutFeatureReply()
                r.ParseFromString(resp_bytes)
                return r

            ok, err = self._send_to_leader("PutFeature", build_args, parse)

            with self._lock:
                if ok:
                    self.ok_count += 1
                else:
                    self.fail_count += 1
                    if len(self.errors) < 10:
                        self.errors.append(f"[{item_id}] {err}")

    def get_stats(self) -> dict:
        with self._lock:
            return {
                "ok": self.ok_count,
                "fail": self.fail_count,
                "errors": list(self.errors),
            }


# ============================================================================
# 主写入逻辑
# ============================================================================

def generate_embedding(rng) -> List[float]:
    """生成一个 128 维随机 embedding（模拟商品特征）"""
    return [rng.random() for _ in range(128)]


def run_warmup(args):
    print(f"{'='*60}")
    print(f"  gen_features.py — C++ Raft KV Server 批量预热工具")
    print(f"{'='*60}")
    print(f"  目标服务器: {args.host}:{args.port}")
    print(f"  总写入条数: {args.total:,}")
    print(f"  批次大小:   {args.batch} 条/批次")
    print(f"  并发线程:   {args.workers}")
    print(f"  维度:       128")
    print(f"{'='*60}")
    print()

    import numpy as np
    rng = np.random.default_rng(args.seed)

    total_items = args.total
    batch_size = args.batch
    n_workers = args.workers

    # 预分配所有 item_id 和 embedding，减少 GC 压力
    print(f"[{time.strftime('%H:%M:%S')}] 生成 {total_items:,} 条 embedding...")
    t0_gen = time.time()

    all_item_ids = [f"item_{i}" for i in range(total_items)]
    all_embeddings = [generate_embedding(rng) for _ in range(total_items)]

    t1_gen = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] 生成完成，耗时 {t1_gen-t0_gen:.2f}s")
    print(f"[{time.strftime('%H:%M:%S')}] 开始分批写入...")

    t0_write = time.time()
    last_print = t0_write
    printed_header = False

    def worker_fn(worker_id: int, start_idx: int, end_idx: int) -> dict:
        writer = BatchWriter(args.host, args.port, worker_id)
        seq = start_idx
        for batch_start in range(start_idx, end_idx, args.batch):
            batch_end = min(batch_start + args.batch, end_idx)
            batch = []
            for i in range(batch_start, batch_end):
                batch.append((all_item_ids[i], all_embeddings[i], seq))
                seq += 1
            writer.write_batch(batch)
        stats = writer.get_stats()
        return stats

    chunks = []
    chunk_size = total_items // n_workers
    for w in range(n_workers):
        s = w * chunk_size
        e = total_items if w == n_workers - 1 else (w + 1) * chunk_size
        chunks.append((w, s, e))

    futures = []
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        for (w, s, e) in chunks:
            futures.append(pool.submit(worker_fn, w, s, e))

        # 实时进度打印
        total_ok = 0
        total_fail = 0
        while any(not f.done() for f in futures):
            time.sleep(1.0)
            now = time.time()
            elapsed = now - t0_write
            done_ok = sum(f.result().get("ok", 0) for f in futures if f.done())
            done_fail = sum(f.result().get("fail", 0) for f in futures if f.done())
            total_ok = sum(w.result().get("ok", 0) for w in futures)
            total_fail = sum(w.result().get("fail", 0) for w in futures)

            progress = (total_ok + total_fail) / total_items * 100
            qps = (total_ok + total_fail) / elapsed if elapsed > 0 else 0

            # 每 5 秒或结束时打印一行
            if now - last_print >= 5.0 or all(f.done() for f in futures):
                print(f"  [{time.strftime('%H:%M:%S')}] "
                      f"进度: {total_ok + total_fail:,}/{total_items:,} "
                      f"({progress:.1f}%) | "
                      f"OK: {total_ok:,} | FAIL: {total_fail:,} | "
                      f"QPS: {qps:,.0f} ops/s")
                last_print = now

        results = [f.result() for f in as_completed(futures)]

    t1_write = time.time()

    # ========== 统计汇总 ==========
    total_ok = sum(r["ok"] for r in results)
    total_fail = sum(r["fail"] for r in results)
    all_errors = []
    for r in results:
        all_errors.extend(r["errors"])

    elapsed = t1_write - t0_write
    total_written = total_ok + total_fail
    qps = total_written / elapsed if elapsed > 0 else 0
    throughput_mbs = (total_written * (128 * 4 + 32)) / elapsed / 1e6  # embedding + item_id 估算

    print()
    print(f"{'='*60}")
    print(f"  写入完成")
    print(f"{'='*60}")
    print(f"  总耗时:        {elapsed:.2f}s")
    print(f"  成功写入:      {total_ok:,} 条")
    print(f"  失败:          {total_fail:,} 条")
    print(f"  成功率:        {total_ok/total_written*100:.2f}%")
    print(f"  平均 QPS:      {qps:,.0f} ops/s")
    print(f"  数据吞吐:      ~{throughput_mbs:.1f} MB/s (embedding估算)")
    print(f"  预估索引构建:  ~{total_ok * 0.0001:.1f}s (HNSW批量插入)")
    print(f"{'='*60}")

    if all_errors:
        print(f"\n前 {len(all_errors)} 条错误信息:")
        for e in all_errors[:10]:
            print(f"  {e}")

    return total_ok, total_fail


def run_search_demo(args):
    """可选：写完后执行一次 Search 验证"""
    print(f"\n[{time.strftime('%H:%M:%S')}] 执行 Search 验证...")
    import numpy as np
    rng = np.random.default_rng(42)
    query = [float(rng.random()) for _ in range(128)]

    with MyRpcClient(args.host, args.port, timeout_ms=10000) as client:
        item_ids, scores, search_time_us = client.search(query, top_k=10)
        print(f"[{time.strftime('%H:%M:%S')}] Search 返回 {len(item_ids)} 条结果，"
              f"耗时 {search_time_us} us:")
        for i, (iid, score) in enumerate(zip(item_ids, scores)):
            print(f"  #{i+1}: {iid}  score={score:.4f}")


def main():
    parser = argparse.ArgumentParser(
        description="批量预热 Raft KV Server：生成 100k 商品特征并写入")
    parser.add_argument("--host", default="127.0.0.1",
                        help="服务器地址 (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8001,
                        help="RPC 端口 (default: 8001)")
    parser.add_argument("--total", type=int, default=100_000,
                        help="总写入条数 (default: 100000)")
    parser.add_argument("--batch", type=int, default=200,
                        help="每批写入条数 (default: 200)")
    parser.add_argument("--workers", type=int, default=8,
                        help="并发写入线程数 (default: 8)")
    parser.add_argument("--seed", type=int, default=42,
                        help="随机种子 (default: 42)")
    parser.add_argument("--search-demo", action="store_true",
                        help="写入完成后执行一次 Search 验证")
    args = parser.parse_args()

    ok, fail = run_warmup(args)

    if args.search_demo and ok > 0:
        run_search_demo(args)


if __name__ == "__main__":
    main()

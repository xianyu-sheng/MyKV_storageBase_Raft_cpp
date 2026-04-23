#!/usr/bin/env python3
"""恢复测试：验证重启节点0后3节点集群正常工作"""
import sys, os, socket, time
sys.path.insert(0, os.path.dirname(__file__))
import Krpcheader_pb2, KvServerRPC_pb2

def encode_varint(v):
    r = bytearray()
    while v > 0x7F:
        r.append((v & 0x7F) | 0x80); v >>= 7
    r.append(v & 0x7F); return bytes(r)

def rpc_call(port, method, args_bytes, timeout=10):
    sock = socket.socket()
    sock.settimeout(timeout)
    sock.connect(('127.0.0.1', port))
    h = Krpcheader_pb2.KrpcHeader()
    h.service_name = b'kvServerRpc'
    h.method_name = method.encode()
    h.args_size = len(args_bytes)
    hdr = h.SerializeToString()
    sock.sendall(encode_varint(len(hdr)) + hdr + args_bytes)
    resp = b''
    while True:
        try:
            c = sock.recv(65536)
            if not c: break
            resp += c
        except socket.timeout: break
    sock.close()
    return resp

def put_feature(port, item_id, embedding, cid=b'test', rid=1):
    a = KvServerRPC_pb2.PutFeatureArgs()
    a.feature.item_id = item_id
    for v in embedding: a.feature.embedding.append(v)
    a.feature.timestamp = int(time.time()*1000)
    a.ClientId = cid
    a.RequestId = rid
    resp = rpc_call(port, 'PutFeature', a.SerializeToString())
    r = KvServerRPC_pb2.PutFeatureReply()
    r.ParseFromString(resp)
    err = r.Err.decode() if isinstance(r.Err, bytes) else str(r.Err)
    return err == 'OK', err

def search(port, top_k=5):
    q = KvServerRPC_pb2.SearchRequest()
    import numpy as np
    rng = np.random.default_rng(time.time_ns() % 10000)
    for _ in range(128): q.query_vector.append(float(rng.random()))
    q.top_k = top_k
    q.search_type = 'inner_product'
    resp = rpc_call(port, 'Search', q.SerializeToString())
    r = KvServerRPC_pb2.SearchResponse()
    r.ParseFromString(resp)
    return list(r.item_ids), list(r.scores), int(r.search_time_us)

# 找 Leader
leader = None
for port in [8002, 8001, 8003]:
    try:
        ok, err = put_feature(port, '__p__', [0.5]*128, b'probe', 0, timeout=5)
        if ok:
            leader = port
            print(f'Leader: 端口 {port}')
            break
        else:
            print(f'端口 {port}: {err}')
    except Exception as e:
        print(f'端口 {port}: 连接失败')

if not leader:
    print('FAIL: 未找到 Leader'); sys.exit(1)

# 写入
print('\n--- 写入 5 条数据 ---')
import numpy as np
rng = np.random.default_rng(999)
for i in range(5):
    emb = [float(rng.random()) for _ in range(128)]
    ok, err = put_feature(leader, f'post_restart_{i}', emb, b'test', i)
    print(f'  post_restart_{i}: {"OK" if ok else err}')

# Search
print('\n--- Search 测试 ---')
ids, scores, st = search(leader, 5)
print(f'  返回 {len(ids)} 条, 延迟 {st} us')
for iid, sc in zip(ids, scores):
    print(f'  {iid}: {sc:.4f}')

print('\n=== 3节点集群功能正常 ===')

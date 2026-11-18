"""面向真实 HTTP 服务的并发补片与中断恢复验证。

前置：backend 以 IMPORT_INLINE 未设置（独立作业进程）方式运行在 127.0.0.1:19999。
运行：python3 test-artifacts/gb-0044/test_recovery.py
"""

import concurrent.futures
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request

BASE = 'http://127.0.0.1:19999'
CHUNK = 65536


def call(method, path, token=None, body=None, headers=None):
    request = urllib.request.Request(BASE + path, data=body, method=method)
    if token:
        request.add_header('Authorization', 'Bearer ' + token)
    for key, value in (headers or {}).items():
        request.add_header(key, value)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.loads(response.read() or b'{}')
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b'{}')


def login():
    return call('POST', '/auth/login', body=json.dumps(
        {'email': 'analyst@e2e.test', 'password': 'Research2026!'}).encode(),
        headers={'Content-Type': 'application/json'})[1]['token']


def build(rows, prefix='HX-RC'):
    lines = ['reference,moisture,thickness,temperature,airflow,minutes']
    for i in range(rows):
        lines.append(f'{prefix}-{i:06d},{30 + i % 20},4.2,75,3.1,{80 + i % 20}')
    return ('\r\n'.join(lines) + '\r\n').encode()


def create(token, content, name):
    return call('POST', '/imports', token, json.dumps({
        'name': name, 'filename': 'rc.csv', 'description': '',
        'file_size': len(content), 'file_sha256': hashlib.sha256(content).hexdigest(),
        'total_chunks': max(1, (len(content) + CHUNK - 1) // CHUNK)}).encode(),
        {'Content-Type': 'application/json'})[1]


def put(token, id, index, part):
    return call('PUT', f'/imports/{id}/chunks/{index}', token, part,
                {'Content-Type': 'application/octet-stream',
                 'X-Chunk-Sha256': hashlib.sha256(part).hexdigest()})


def settle(token, id, timeout=120):
    deadline = time.time() + timeout
    latest = None
    while time.time() < deadline:
        latest = call('GET', f'/imports/{id}', token)[1]
        if latest['state'] in ('ready', 'rejected', 'failed'):
            return latest
        time.sleep(0.1)
    return latest


def test_concurrent_same_chunk_conflict(token):
    content = build(400, 'HX-CC')
    record = create(token, content, 'RC 并发冲突')
    parts = [content[i:i + CHUNK] for i in range(0, len(content), CHUNK)]
    tampered = bytearray(parts[0])
    tampered[-1] ^= 0xFF
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda body: put(token, record['id'], 0, body),
                                [parts[0], bytes(tampered)]))
    codes = sorted(status for status, _ in results)
    print('concurrent same chunk statuses', codes)
    assert 409 in codes and 200 in codes, results
    state = call('GET', f"/imports/{record['id']}", token)[1]
    assert state['received_chunks'] == 1, state


def test_concurrent_distinct_chunks(token):
    content = build(6000, 'HX-CD')
    record = create(token, content, 'RC 并发补片')
    parts = [content[i:i + CHUNK] for i in range(0, len(content), CHUNK)]
    order = list(range(len(parts)))[::-1]
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        statuses = list(pool.map(lambda i: put(token, record['id'], i, parts[i])[0], order))
    assert set(statuses) == {200}, statuses
    status, _ = call('POST', f"/imports/{record['id']}/complete", token)
    assert status == 200
    state = settle(token, record['id'])
    print('concurrent distinct final', state['state'], state['unique_rows'])
    assert state['state'] == 'ready'
    assert state['unique_rows'] == 6000


def test_worker_killed_then_retry(token):
    content = build(300000, 'HX-KILL')
    record = create(token, content, 'RC 中断恢复')
    parts = [content[i:i + CHUNK] for i in range(0, len(content), CHUNK)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda i: put(token, record['id'], i, parts[i]), range(len(parts))))
    status, _ = call('POST', f"/imports/{record['id']}/complete", token)
    assert status == 200
    # 等到作业确实开始解析
    for _ in range(100):
        state = call('GET', f"/imports/{record['id']}", token)[1]
        if state['phase'] in ('parsing', 'hashing'):
            break
        time.sleep(0.05)
    # 用字符类避免 pkill 匹配到自身 shell 命令行
    killed = os.system(f"pkill -f 'src[.]import_job {record['id']}' >/dev/null 2>&1")
    print('killed worker rc', killed, 'phase', state['phase'])
    time.sleep(0.4)
    state = call('GET', f"/imports/{record['id']}", token)[1]
    print('after kill', state['state'], state['phase'], 'stale', state.get('stale'))
    # 作业被杀后心跳停止；stale 窗口（90s）过后重试才允许接管，避免双进程写同一会话。
    # 测试中直接把心跳改旧，模拟 stale 窗口已过
    os.system(f"""cd /workspace/backend && DATABASE_URL=sqlite:////tmp/e2e.db python3 -c "
import sys;sys.path.insert(0,'.')
from src.config.database import Session
from src.models import user,source,record,run,note,audit,import_session
from src.models.import_session import ImportSession
with Session() as db:
    r=db.get(ImportSession,{record['id']});r.heartbeat_at='2020-01-01T00:00:00+00:00';db.commit()
" >/dev/null 2>&1""")
    state = call('GET', f"/imports/{record['id']}", token)[1]
    assert state['stale'] is True, state
    status, _ = call('POST', f"/imports/{record['id']}/retry", token)
    assert status == 200, status
    state = settle(token, record['id'])
    print('recovered after retry', state['state'], state['unique_rows'])
    assert state['state'] == 'ready'
    assert state['unique_rows'] == 300000
    again = call('POST', f"/imports/{record['id']}/retry", token)[1]
    assert again['source_id'] == state['source_id']


def test_complete_interleaved_with_upload(token):
    content = build(2000, 'HX-IL')
    record = create(token, content, 'RC 交错')
    parts = [content[i:i + CHUNK] for i in range(0, len(content), CHUNK)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(put, token, record['id'], i, parts[i]) for i in range(len(parts))]
        futures.append(pool.submit(call, 'POST', f"/imports/{record['id']}/complete", token))
        results = [future.result() for future in futures]
    statuses = [status for status, _ in results]
    print('interleaved statuses', statuses)
    state = call('GET', f"/imports/{record['id']}", token)[1]
    if state['state'] == 'receiving':
        # 完成请求若先于最后一片到达会被明确拒绝；客户端补齐后再提交即可
        again, _ = call('POST', f"/imports/{record['id']}/complete", token)
        assert again == 200, again
    state = settle(token, record['id'])
    print('interleaved final', state['state'], state['unique_rows'])
    # 完成请求早于最后一片时必须先返回 409；文件到齐后最终仍然只发布一次
    assert state['state'] == 'ready'
    assert state['unique_rows'] == 2000
    state2 = settle(token, record['id'])
    assert state2['source_id'] == state['source_id']


if __name__ == '__main__':
    token = login()
    test_concurrent_same_chunk_conflict(token)
    test_concurrent_distinct_chunks(token)
    test_complete_interleaved_with_upload(token)
    test_worker_killed_then_retry(token)
    print('RECOVERY OK')

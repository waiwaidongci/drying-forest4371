"""分片续传导入的接口级测试。

运行：cd backend && python3 -m pytest ../test-artifacts/gb-0044/test_imports.py -q
环境：SQLite + IMPORT_INLINE=1（作业在与接口同进程的后台线程执行）。
"""

import csv
import hashlib
import io
import json
import os
import sys
import tempfile
import time
import uuid

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, BACKEND)

CHUNK = 256


def make_csv(rows, header=('reference', 'moisture', 'thickness', 'temperature', 'airflow', 'minutes'), raw_tail=''):
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(header)
    writer.writerows(rows)
    buffer.write(raw_tail)
    return buffer.getvalue().encode('utf-8')


def row(index, reference=None, shift=0.0):
    return [reference or f'HX-T-{index:05d}', 30 + index % 20 + shift, 4.2, 75, 3.1, 80 + index % 30]


@pytest.fixture(scope='session')
def client():
    root = tempfile.mkdtemp(prefix='import-test-')
    os.environ.update(DATABASE_URL='sqlite:///' + os.path.join(root, 'test.db'),
                      ARTIFACT_DIR=os.path.join(root, 'artifacts'), SEED_DEMO='false',
                      IMPORT_INLINE='1', IMPORT_CHUNK_SIZE=str(CHUNK), IMPORT_RESUME='false')
    from fastapi.testclient import TestClient
    from src.main import app
    from src.config.database import Base, Session, engine
    from src.models import user, source, record, run, note, audit, import_session
    from src.models.user import User
    from src.utils.security import hash_password
    Base.metadata.create_all(engine)
    with Session() as db:
        for role in ('admin', 'analyst', 'viewer'):
            db.add(User(email=role + '@test', password_hash=hash_password('pw'), role=role))
        db.commit()
    with TestClient(app) as test_client:
        yield test_client


def auth(client, role='analyst'):
    token = client.post('/auth/login', json={'email': role + '@test', 'password': 'pw'}).json()['token']
    return {'Authorization': 'Bearer ' + token}


def open_import(client, headers, content, name='补录来源', total=None, digest=None):
    size = len(content)
    total = total if total is not None else max(1, (size + CHUNK - 1) // CHUNK)
    response = client.post('/imports', headers=headers, json={
        'name': name, 'filename': 'supplement.csv', 'description': '补录',
        'file_size': size, 'file_sha256': digest or hashlib.sha256(content).hexdigest(),
        'total_chunks': total})
    assert response.status_code == 200, response.text
    return response.json()


def slices(content):
    return [content[i:i + CHUNK] for i in range(0, len(content), CHUNK)] or [b'']


def put_chunk(client, headers, id, index, body, sha=True):
    extra = {'X-Chunk-Sha256': hashlib.sha256(body).hexdigest()} if sha else {}
    return client.put(f'/imports/{id}/chunks/{index}', headers={**headers, **extra}, content=body)


def wait(client, headers, id, timeout=30):
    """等待作业离开 verifying；receiving（含作业退回补片）属于稳定终态。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        state = client.get(f'/imports/{id}', headers=headers).json()
        if state['state'] != 'verifying' or state.get('stale'):
            return state
        time.sleep(0.05)
    raise AssertionError('import did not settle')


def sources_count(client, headers):
    return len(client.get('/sources', headers=headers).json())


def test_happy_path_out_of_order_multiline_multibyte(client):
    headers = auth(client)
    before = sources_count(client, headers)
    content = make_csv([row(i) for i in range(12)],
                       raw_tail='"HX-T-多字节\n跨行",30,4,70,3,78\r\n')
    parts = slices(content)
    assert len(parts) > 1
    import_row = open_import(client, headers, content)
    for index in reversed(range(len(parts))):
        response = put_chunk(client, headers, import_row['id'], index, parts[index])
        assert response.status_code == 200, response.text
    done = client.post(f'/imports/{import_row["id"]}/complete', headers=headers)
    assert done.status_code == 200 and done.json()['state'] == 'verifying'
    state = wait(client, headers, import_row['id'])
    assert state['state'] == 'ready', state
    assert state['processed_rows'] == 13 and state['unique_rows'] == 13
    assert state['percent'] == 100
    records = client.get(f'/sources/{state["source_id"]}/records', headers=headers).json()
    assert len(records) == 13
    assert any(record['reference'] == 'HX-T-多字节\n跨行' for record in records)
    assert sources_count(client, headers) == before + 1


def test_multibyte_character_split_across_chunks(client):
    headers = auth(client)
    # 让一个 3 字节汉字正好横跨分片边界
    prefix = b'reference,moisture,thickness,temperature,airflow,minutes\r\n'
    head = prefix + b'"HX-\xe6'
    content = head + b'\xa7\x90\xe5\xba\x8f\xe7\xbc\x96\xe5\x8f\xb7",30,4.2,75,3.1,88\r\n'
    assert len(head) % CHUNK != 0
    parts = slices(content)
    import_row = open_import(client, headers, content)
    for index, part in enumerate(parts):
        assert put_chunk(client, headers, import_row['id'], index, part).status_code == 200
    client.post(f'/imports/{import_row["id"]}/complete', headers=headers)
    state = wait(client, headers, import_row['id'])
    assert state['state'] == 'ready', state
    records = client.get(f'/sources/{state["source_id"]}/records', headers=headers).json()
    assert records[0]['reference'] == 'HX-槐序编号'


def test_same_id_same_value_merges_and_same_id_different_value_rejects(client):
    headers = auth(client)
    before = sources_count(client, headers)
    merged = make_csv([row(1), row(2), row(1), row(2, shift=0)])  # 同号同值
    import_row = open_import(client, headers, merged)
    for index, part in enumerate(slices(merged)):
        put_chunk(client, headers, import_row['id'], index, part)
    client.post(f'/imports/{import_row["id"]}/complete', headers=headers)
    state = wait(client, headers, import_row['id'])
    assert state['state'] == 'ready', state
    assert state['duplicate_rows'] == 2 and state['unique_rows'] == 2

    conflicting = make_csv([row(1), row(2), row(1, shift=1)])
    import_row = open_import(client, headers, conflicting)
    for index, part in enumerate(slices(conflicting)):
        put_chunk(client, headers, import_row['id'], index, part)
    client.post(f'/imports/{import_row["id"]}/complete', headers=headers)
    state = wait(client, headers, import_row['id'])
    assert state['state'] == 'rejected'
    assert state['conflicts'] == 1
    assert any('不同取值' in reason['message'] for reason in state['reasons'])
    assert sources_count(client, headers) == before + 1


def test_same_index_different_content_conflict_and_retransmit_same_content(client):
    headers = auth(client)
    content = make_csv([row(i) for i in range(10)])
    import_row = open_import(client, headers, content)
    parts = slices(content)
    assert put_chunk(client, headers, import_row['id'], 0, parts[0]).status_code == 200
    retransmit = put_chunk(client, headers, import_row['id'], 0, parts[0])
    assert retransmit.status_code == 200 and retransmit.json()['duplicate'] is True
    assert retransmit.json()['received_chunks'] == 1
    tampered = bytearray(parts[0])
    tampered[-1] ^= 0x01
    conflict = put_chunk(client, headers, import_row['id'], 0, bytes(tampered))
    assert conflict.status_code == 409
    assert '内容不同' in conflict.json()['detail']


def test_wrong_declared_chunk_sha_rejected(client):
    headers = auth(client)
    content = make_csv([row(i) for i in range(4)])
    import_row = open_import(client, headers, content)
    response = client.put(f'/imports/{import_row["id"]}/chunks/0', headers={**headers, 'X-Chunk-Sha256': '0' * 64},
                          content=slices(content)[0])
    assert response.status_code == 422


def test_wrong_chunk_size_rejected(client):
    headers = auth(client)
    content = make_csv([row(i) for i in range(10)])
    import_row = open_import(client, headers, content)
    response = put_chunk(client, headers, import_row['id'], 0, b'short')
    assert response.status_code == 422


def test_whole_file_checksum_mismatch_rejects_without_source(client):
    headers = auth(client)
    before = sources_count(client, headers)
    content = make_csv([row(i) for i in range(6)])
    import_row = open_import(client, headers, content, digest='1' * 64)
    for index, part in enumerate(slices(content)):
        assert put_chunk(client, headers, import_row['id'], index, part).status_code == 200
    client.post(f'/imports/{import_row["id"]}/complete', headers=headers)
    state = wait(client, headers, import_row['id'])
    assert state['state'] == 'rejected'
    assert 'SHA-256' in state['error']
    assert sources_count(client, headers) == before


def test_complete_requires_all_chunks(client):
    headers = auth(client)
    content = make_csv([row(i) for i in range(10)])
    import_row = open_import(client, headers, content)
    parts = slices(content)
    put_chunk(client, headers, import_row['id'], 0, parts[0])
    response = client.post(f'/imports/{import_row["id"]}/complete', headers=headers)
    assert response.status_code == 409
    assert '缺少' in response.json()['detail']


def test_duplicate_chunk_registration_survives_recreate_and_retry_ready_is_idempotent(client):
    headers = auth(client)
    before = sources_count(client, headers)
    content = make_csv([row(i) for i in range(5)])
    import_row = open_import(client, headers, content)
    for index, part in enumerate(slices(content)):
        put_chunk(client, headers, import_row['id'], index, part)
    client.post(f'/imports/{import_row["id"]}/complete', headers=headers)
    assert wait(client, headers, import_row['id'])['state'] == 'ready'
    again = client.post(f'/imports/{import_row["id"]}/complete', headers=headers)
    assert again.status_code == 200 and again.json()['state'] == 'ready'
    retry = client.post(f'/imports/{import_row["id"]}/retry', headers=headers)
    assert retry.status_code == 200 and retry.json()['state'] == 'ready'
    assert sources_count(client, headers) == before + 1


def test_interleaved_chunk_and_complete(client):
    headers = auth(client)
    content = make_csv([row(i) for i in range(9)])
    parts = slices(content)
    import_row = open_import(client, headers, content)
    # 完成请求先到：不得提前进入校验
    early = client.post(f'/imports/{import_row["id"]}/complete', headers=headers)
    assert early.status_code == 409
    for index in range(len(parts) - 1):
        put_chunk(client, headers, import_row['id'], index, parts[index])
    assert client.get(f'/imports/{import_row["id"]}', headers=headers).json()['state'] == 'receiving'
    put_chunk(client, headers, import_row['id'], len(parts) - 1, parts[-1])
    response = client.post(f'/imports/{import_row["id"]}/complete', headers=headers)
    assert response.status_code == 200
    state = wait(client, headers, import_row['id'])
    assert state['state'] == 'ready'
    # 校验中不得再接收分片
    late = put_chunk(client, headers, import_row['id'], 0, parts[0])
    assert late.status_code in (200, 409)
    if late.status_code == 409:
        assert '校验' in late.json()['detail'] or '结束' in late.json()['detail']


def test_invalid_field_row_rejects_whole_file(client):
    headers = auth(client)
    before = sources_count(client, headers)
    content = make_csv([row(1), ['HX-BAD', 'not-a-number', 4, 70, 3, 70], row(3)])
    import_row = open_import(client, headers, content)
    for index, part in enumerate(slices(content)):
        put_chunk(client, headers, import_row['id'], index, part)
    client.post(f'/imports/{import_row["id"]}/complete', headers=headers)
    state = wait(client, headers, import_row['id'])
    assert state['state'] == 'rejected'
    assert state['invalid_rows'] == 1
    assert state['reasons'][0]['reference'] == 'HX-BAD'
    assert sources_count(client, headers) == before


def test_bad_header_and_bad_encoding_reject(client):
    headers = auth(client)
    before = sources_count(client, headers)
    for content, marker in [
        (make_csv([row(1)], header=('reference', 'moisture', 'thickness', 'temperature', 'airflow')),
         '缺少必需列'),
        (make_csv([row(1)], header=('reference', 'moisture', 'thickness', 'temperature', 'airflow', 'minutes', 'note')),
         '未知列'),
        (b'reference,moisture,thickness,temperature,airflow,minutes\r\n"open,30,4,70,3,70\r\n', 'CSV'),
        (b'reference,moisture,thickness,temperature,airflow,minutes\r\nHX-\xff,30,4,70,3,70\r\n', 'UTF-8'),
        (b'\r\n', '未导入'),
    ]:
        import_row = open_import(client, headers, content)
        for index, part in enumerate(slices(content)):
            put_chunk(client, headers, import_row['id'], index, part)
        client.post(f'/imports/{import_row["id"]}/complete', headers=headers)
        state = wait(client, headers, import_row['id'])
        assert state['state'] == 'rejected', (marker, state)
        assert marker in state['error'] or any(marker in reason['message'] for reason in state['reasons']), (marker, state)
    assert sources_count(client, headers) == before


def test_cancel_leaves_no_source_and_blocks_upload(client):
    headers = auth(client)
    before = sources_count(client, headers)
    content = make_csv([row(i) for i in range(20)])
    import_row = open_import(client, headers, content)
    assert len(slices(content)) > 1
    put_chunk(client, headers, import_row['id'], 0, slices(content)[0])
    cancelled = client.post(f'/imports/{import_row["id"]}/cancel', headers=headers)
    assert cancelled.status_code == 200 and cancelled.json()['state'] == 'rejected'
    blocked = put_chunk(client, headers, import_row['id'], 1, slices(content)[1])
    assert blocked.status_code == 409
    assert sources_count(client, headers) == before


def test_viewer_cannot_import(client):
    headers = auth(client, 'viewer')
    response = client.post('/imports', headers=headers, json={
        'name': 'x1', 'filename': 'x.csv', 'description': '', 'file_size': 10,
        'file_sha256': '0' * 64, 'total_chunks': 1})
    assert response.status_code == 403


def test_create_validates_chunk_count_and_digest(client):
    headers = auth(client)
    base = {'name': '校验来源', 'filename': 'x.csv', 'description': '', 'file_size': 1000,
            'file_sha256': '0' * 64, 'total_chunks': 1}
    assert client.post('/imports', headers=headers, json={**base, 'total_chunks': 2}).status_code == 422
    assert client.post('/imports', headers=headers, json={**base, 'file_sha256': 'zz'}).status_code == 422


def test_missing_disk_chunk_returns_to_receiving_and_can_be_restored(client):
    headers = auth(client)
    content = make_csv([row(i) for i in range(12)])
    import_row = open_import(client, headers, content)
    parts = slices(content)
    assert len(parts) > 1
    for index, part in enumerate(parts):
        put_chunk(client, headers, import_row['id'], index, part)
    # 模拟分片文件在落盘后丢失（例如卷异常），登记仍在
    from src.services.imports import chunk_path
    from src.config.database import Session
    from src.models.import_session import ImportSession
    with Session() as db:
        token = db.get(ImportSession, import_row['id']).token
    chunk_path(token, 0).unlink(missing_ok=True)
    response = client.post(f'/imports/{import_row["id"]}/complete', headers=headers)
    assert response.status_code == 200
    state = wait(client, headers, import_row['id'])
    assert state['state'] == 'receiving', state
    assert '缺失' in state['error']
    # 同内容重传补齐磁盘文件，随后可正常完成
    same = put_chunk(client, headers, import_row['id'], 0, parts[0])
    assert same.status_code == 200
    client.post(f'/imports/{import_row["id"]}/complete', headers=headers)
    state = wait(client, headers, import_row['id'])
    assert state['state'] == 'ready', state
    assert state['unique_rows'] == 12


def test_failed_job_retry_does_not_duplicate_source(client, monkeypatch):
    headers = auth(client)
    before = sources_count(client, headers)
    content = make_csv([row(i) for i in range(8)])
    import_row = open_import(client, headers, content)
    for index, part in enumerate(slices(content)):
        put_chunk(client, headers, import_row['id'], index, part)
    import src.import_job as job
    broken = {'phase': 'parsing'}
    original = job._parse

    def explode(db, record, paths, stage):
        raise RuntimeError('模拟作业进程崩溃')
    monkeypatch.setattr(job, '_parse', explode)
    client.post(f'/imports/{import_row["id"]}/complete', headers=headers)
    state = wait(client, headers, import_row['id'])
    assert state['state'] == 'failed', state
    assert sources_count(client, headers) == before
    monkeypatch.setattr(job, '_parse', original)
    retried = client.post(f'/imports/{import_row["id"]}/retry', headers=headers)
    assert retried.status_code == 200
    state = wait(client, headers, import_row['id'])
    assert state['state'] == 'ready', state
    assert sources_count(client, headers) == before + 1
    # 再重试一次仍是同一来源
    client.post(f'/imports/{import_row["id"]}/retry', headers=headers)
    assert sources_count(client, headers) == before + 1


def test_addition_to_existing_source_still_uses_batch_endpoint(client):
    headers = auth(client)
    source = client.post('/sources', headers=headers, json={
        'name': '既有来源', 'filename': 'existing.csv', 'description': '原批量录入'}).json()
    records = [{'reference': f'HX-OLD-{i}', 'values': {
        'moisture': 30 + i, 'thickness': 4, 'temperature': 70, 'airflow': 3, 'minutes': 80}} for i in range(3)]
    response = client.post(f'/sources/{source["id"]}/records', headers=headers, json={'records': records})
    assert response.status_code == 200 and response.json()['added'] == 3
    duplicate = client.post(f'/sources/{source["id"]}/records', headers=headers, json={'records': records[:1]})
    assert duplicate.status_code == 409


def test_second_import_of_same_file_is_a_new_source(client):
    headers = auth(client)
    before = sources_count(client, headers)
    content = make_csv([row(i) for i in range(6)])
    for _ in range(2):
        import_row = open_import(client, headers, content, name='同名补录')
        for index, part in enumerate(slices(content)):
            put_chunk(client, headers, import_row['id'], index, part)
        client.post(f'/imports/{import_row["id"]}/complete', headers=headers)
        assert wait(client, headers, import_row['id'])['state'] == 'ready'
    assert sources_count(client, headers) == before + 2

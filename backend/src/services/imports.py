import hashlib
import logging
import math
import os
import subprocess
import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import HTTPException
from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import IntegrityError

from ..config.settings import ARTIFACT_DIR
from ..models.import_session import ImportChunk, ImportSession
from ..utils.serialization import now, row
from .audit import log

CHUNK_SIZE = int(os.getenv('IMPORT_CHUNK_SIZE', str(4 * 1024 * 1024)))
MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024
MAX_CHUNKS = 8192
STALE_AFTER = timedelta(seconds=90)
SWEEP_AFTER = timedelta(hours=24)


def _out(record):
    data = row(record)
    data.pop('created_by', None)
    data.pop('token', None)
    data['stale'] = bool(data['state'] == 'verifying' and _stale(record))
    data['total_chunks'] = int(data['total_chunks'])
    data['chunk_size'] = int(data['chunk_size'])
    size = data['file_size'] or 1
    data['percent_received'] = round(100 * min(data['received_bytes'], size) / size, 1)
    if data['state'] in ('receiving', 'failed'):
        data['percent_processed'] = data['percent_received']
    elif data['phase'] == 'publishing':
        published = _published(record)
        total = data['unique_rows'] or 1
        data['published_rows'] = min(published, total)
        data['percent_processed'] = round(100 * data['published_rows'] / total, 1)
    elif data['state'] == 'ready':
        data['percent_processed'] = 100.0
    else:
        data['percent_processed'] = round(100 * min(data['processed_bytes'], size) / size, 1)
    data['percent'] = 100.0 if data['state'] == 'ready' else data['percent_processed']
    return data


def import_dir(token):
    return Path(ARTIFACT_DIR) / 'imports' / token


def chunk_path(token, index):
    return import_dir(token) / f'{index:08d}.part'


def expected_size(record, index):
    if index < record.total_chunks - 1:
        return record.chunk_size
    return record.file_size - record.chunk_size * (record.total_chunks - 1)


def _chunk_lock(token):
    directory = import_dir(token)
    directory.mkdir(parents=True, exist_ok=True)
    return open(directory / '.chunks.lock', 'a')


def get_import(db, id):
    record = db.get(ImportSession, id)
    if not record:
        raise HTTPException(404, '未找到导入任务')
    return record


def _published(record):
    try:
        return int((import_dir(record.token) / 'progress.txt').read_text())
    except (OSError, ValueError):
        return 0


def config():
    return {'chunk_size': CHUNK_SIZE, 'max_file_size': MAX_FILE_SIZE, 'max_chunks': MAX_CHUNKS}


def create_import(db, user, data):
    if data.file_size > MAX_FILE_SIZE:
        raise HTTPException(422, f'单个文件不得超过 {MAX_FILE_SIZE // (1024 * 1024)} MiB')
    expected_chunks = math.ceil(data.file_size / CHUNK_SIZE)
    if data.total_chunks != expected_chunks:
        raise HTTPException(422, f'分片总数应为 {expected_chunks}（分片大小 {CHUNK_SIZE} 字节）')
    if not 1 <= data.total_chunks <= MAX_CHUNKS:
        raise HTTPException(422, '分片总数超出可接受范围')
    digest = data.file_sha256.lower()
    if len(digest) != 64 or any(c not in '0123456789abcdef' for c in digest):
        raise HTTPException(422, '文件校验和必须是 64 位十六进制 SHA-256')
    token = hashlib.sha256(f'{user.email}:{now()}:{os.urandom(16).hex()}'.encode()).hexdigest()[:32]
    record = ImportSession(token=token, source_name=data.name, source_filename=data.filename,
                           source_description=data.description, state='receiving', phase='receiving',
                           file_size=data.file_size, file_sha256=digest, chunk_size=CHUNK_SIZE,
                           total_chunks=data.total_chunks, created_by=user.email,
                           created_at=now(), updated_at=now(), reasons=[])
    db.add(record)
    db.flush()
    log(db, user, 'import.started', record.id, f'{data.name} · {data.total_chunks} 片')
    db.commit()
    import_dir(token).mkdir(parents=True, exist_ok=True)
    return _out(record)


def list_imports(db, limit=20):
    records = db.scalars(select(ImportSession).order_by(ImportSession.id.desc()).limit(limit))
    return [_out(record) for record in records]


def status(db, id, with_indexes=True):
    record = get_import(db, id)
    data = _out(record)
    if with_indexes:
        data['received_indexes'] = [int(index) for index in db.scalars(
            select(ImportChunk.chunk_index).where(ImportChunk.import_id == id).order_by(ImportChunk.chunk_index))]
    return data


def upload_chunk(db, user, id, index, body, declared_sha):
    record = get_import(db, id)
    if record.state in ('ready', 'rejected'):
        raise HTTPException(409, '导入已经结束，不能再上传分片')
    if index < 0 or index >= record.total_chunks:
        raise HTTPException(422, f'分片号需在 0–{record.total_chunks - 1} 之间')
    size = len(body)
    if size != expected_size(record, index):
        raise HTTPException(422, f'第 {index} 片大小应为 {expected_size(record, index)} 字节，实收 {size} 字节')
    digest = hashlib.sha256(body).hexdigest()
    if declared_sha and declared_sha.lower() != digest:
        raise HTTPException(422, '分片内容与声明校验和不一致')
    expected_chunks = math.ceil(record.file_size / record.chunk_size)
    if expected_chunks != record.total_chunks:
        raise HTTPException(422, '分片总数与文件大小不一致，请重新创建导入')

    lock = _chunk_lock(record.token)
    try:
        import fcntl
        fcntl.flock(lock, fcntl.LOCK_EX)
        db.refresh(record)
        if record.state not in ('receiving', 'failed'):
            raise HTTPException(409, '导入正在校验或已经结束，不能再上传分片')
        existing = db.scalar(select(ImportChunk).where(ImportChunk.import_id == id,
                                                      ImportChunk.chunk_index == index))
        if existing and existing.sha256 != digest:
            raise HTTPException(409, f'第 {index} 片已存在且内容不同；请核对原文件后重传，不得覆盖')
        target = chunk_path(record.token, index)
        if existing:
            duplicate = True
            # 已登记分片若磁盘文件丢失（例如卷重置），同内容重传时补齐文件
            if not target.exists() or target.stat().st_size != size:
                _write_chunk(target, body)
        else:
            _write_chunk(target, body)
            db.add(ImportChunk(import_id=id, chunk_index=index, size=size, sha256=digest,
                               received_at=now()))
            try:
                db.commit()
            except IntegrityError:
                db.rollback()
                existing = db.scalar(select(ImportChunk).where(ImportChunk.import_id == id,
                                                               ImportChunk.chunk_index == index))
                if existing is None or existing.sha256 != digest:
                    raise HTTPException(409, f'第 {index} 片并发写入内容不同，请核对原文件')
            duplicate = False
        values = {}
        if record.state == 'failed':
            values.update(state='receiving', phase='receiving', error='')
        count, total_bytes = db.execute(
            select(func.count(ImportChunk.id), func.coalesce(func.sum(ImportChunk.size), 0))
            .where(ImportChunk.import_id == id)).one()
        db.execute(update(ImportSession).where(ImportSession.id == id).values(
            received_chunks=int(count), received_bytes=int(total_bytes), updated_at=now(), **values))
        db.commit()
        return {'id': id, 'index': index, 'size': size, 'sha256': digest, 'duplicate': duplicate,
                'received_chunks': int(count), 'total_chunks': record.total_chunks,
                'received_bytes': int(total_bytes), 'file_size': record.file_size}
    finally:
        lock.close()


def _write_chunk(target, body):
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_suffix('.tmp')
    temp.write_bytes(body)
    os.replace(temp, target)


def _spawn(id):
    if os.getenv('IMPORT_INLINE') == '1':
        from ..import_job import execute_import
        thread = threading.Thread(target=execute_import, args=(id,), daemon=True)
        thread.start()
        return
    base = Path(__file__).resolve().parents[2]
    environment = dict(os.environ)
    environment.setdefault('PYTHONPATH', str(base))
    log_path = import_dir('_spawn')
    log_path.mkdir(parents=True, exist_ok=True)
    handle = open(log_path / f'job-{id}.log', 'ab')
    try:
        process = subprocess.Popen([sys.executable, '-m', 'src.import_job', str(id)], cwd=str(base),
                                   env=environment, stdout=handle, stderr=subprocess.STDOUT,
                                   start_new_session=True, close_fds=True)
    finally:
        handle.close()
    logging.info('import job %s spawned pid=%s', id, process.pid)


def _chunk_state(db, record):
    rows = db.execute(select(ImportChunk.chunk_index, ImportChunk.size).where(
        ImportChunk.import_id == record.id)).all()
    sizes = {int(index): int(size) for index, size in rows}
    missing = [index for index in range(record.total_chunks)
               if index not in sizes or sizes[index] != expected_size(record, index)]
    return missing


def complete_import(db, user, id):
    record = get_import(db, id)
    lock = _chunk_lock(record.token)
    try:
        import fcntl
        fcntl.flock(lock, fcntl.LOCK_EX)
        return _complete_locked(db, user, id)
    finally:
        lock.close()


def _complete_locked(db, user, id):
    db.expire_all()
    record = get_import(db, id)
    if record.state == 'ready':
        return _out(record)
    if record.state == 'rejected':
        raise HTTPException(409, '该导入已被拒绝，请核对原文件后重新发起导入：' + (record.error or ''))
    if record.state == 'verifying' and not _stale(record):
        return _out(record)
    missing = _chunk_state(db, record)
    if missing:
        if record.state == 'verifying':
            db.execute(update(ImportSession).where(ImportSession.id == id).values(
                state='receiving', phase='receiving', updated_at=now()))
            db.commit()
            record = get_import(db, id)
        preview = '、'.join(str(index) for index in missing[:20])
        suffix = '…' if len(missing) > 20 else ''
        raise HTTPException(409, f'还缺少 {len(missing)} 个分片（{preview}{suffix}），补齐后才可完成校验')
    if record.state not in ('receiving', 'failed', 'verifying'):
        raise HTTPException(409, '当前状态不能完成导入')

    cutoff = (datetime.now(timezone.utc) - STALE_AFTER).isoformat()
    claimed = db.execute(update(ImportSession).where(
        ImportSession.id == id,
        (ImportSession.state != 'verifying') | (ImportSession.heartbeat_at < cutoff)
    ).values(state='verifying', phase='queued', error='', queued_at=now(), heartbeat_at=now(),
             processed_rows=0, processed_bytes=0, updated_at=now()))
    db.commit()
    if claimed.rowcount == 0:
        return _out(get_import(db, id))
    _spawn(id)
    log(db, user, 'import.verifying', id, f'{record.total_chunks} 片已齐，开始全文件校验')
    db.commit()
    return _out(get_import(db, id))


def _stale(record):
    threshold = datetime.now(timezone.utc) - STALE_AFTER
    try:
        file_beat = (import_dir(record.token) / 'heartbeat').stat().st_mtime
        if datetime.fromtimestamp(file_beat, timezone.utc) >= threshold:
            return False
    except OSError:
        pass
    if not record.heartbeat_at:
        return True
    try:
        beat = datetime.fromisoformat(record.heartbeat_at)
    except ValueError:
        return True
    return beat < threshold


def retry_import(db, user, id):
    record = get_import(db, id)
    if record.state == 'rejected':
        raise HTTPException(409, '该导入已被拒绝，请核对原文件后重新发起导入：' + (record.error or ''))
    if record.state == 'ready':
        return _out(record)
    if record.state == 'verifying' and not _stale(record):
        return _out(record)
    missing = _chunk_state(db, record)
    if missing:
        db.execute(update(ImportSession).where(ImportSession.id == id).values(
            state='receiving', phase='receiving', error=f'还缺少 {len(missing)} 个分片',
            updated_at=now()))
        db.commit()
        raise HTTPException(409, f'还缺少 {len(missing)} 个分片，请补片后再重试完成')
    db.execute(update(ImportSession).where(ImportSession.id == id).values(
        state='failed', phase='done', error='', updated_at=now()))
    db.commit()
    return complete_import(db, user, id)


def _purge_files(record):
    directory = import_dir(record.token)
    if not directory.exists():
        return
    for path in directory.iterdir():
        if path.name in ('.lock', '.chunks.lock'):
            continue
        try:
            if path.is_file():
                path.unlink()
        except OSError:
            logging.warning('import cleanup failed: %s', path)


def cancel_import(db, user, id):
    record = get_import(db, id)
    if record.state == 'verifying' and not _stale(record):
        raise HTTPException(409, '导入正在校验，不能取消')
    if record.state in ('ready', 'rejected'):
        raise HTTPException(409, '导入已经结束，不能取消')
    db.execute(delete(ImportChunk).where(ImportChunk.import_id == id))
    db.execute(update(ImportSession).where(ImportSession.id == id).values(
        state='rejected', phase='done', error='导入已由用户取消，整份文件未导入',
        reasons=[{'line': 0, 'reference': '', 'message': '用户取消导入'}],
        received_chunks=0, received_bytes=0, completed_at=now(), updated_at=now()))
    log(db, user, 'import.cancelled', id)
    db.commit()
    _purge_files(record)
    return _out(get_import(db, id))


def sweep_finished(db):
    """清理超过保留期的已完成/已拒绝会话的落盘文件；数据库账册保留。"""
    cutoff = (datetime.now(timezone.utc) - SWEEP_AFTER).isoformat()
    ids = list(db.scalars(select(ImportSession.id).where(
        ImportSession.state.in_(('ready', 'rejected')), ImportSession.updated_at < cutoff)))
    for id in ids:
        record = db.get(ImportSession, id)
        if not record:
            continue
        _purge_files(record)
        log(db, 'import', 'import.cleaned', id, '分片文件已按保留期清理')
        db.execute(update(ImportSession).where(ImportSession.id == id).values(updated_at=now()))
        db.commit()


def _job_alive(record):
    """作业进程持有会话目录锁；能取到锁说明没有存活作业。"""
    import fcntl
    directory = import_dir(record.token)
    if not directory.exists():
        return False
    handle = open(directory / '.lock', 'a')
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return False
    except BlockingIOError:
        return True
    finally:
        handle.close()


def resume_stale(db):
    """服务重启后重新拉起没有存活作业的校验会话；已落盘分片继续可用。"""
    ids = list(db.scalars(select(ImportSession.id).where(ImportSession.state == 'verifying')))
    for id in ids:
        try:
            record = db.get(ImportSession, id)
            if not record or _job_alive(record):
                continue
            db.execute(update(ImportSession).where(ImportSession.id == id).values(
                state='verifying', phase='queued', heartbeat_at=now(), updated_at=now()))
            db.commit()
            _spawn(id)
            log(db, 'import', 'import.resumed', id, '服务重启后重新拉起校验')
            db.commit()
        except Exception:
            logging.exception('import resume failed: %s', id)
            db.rollback()

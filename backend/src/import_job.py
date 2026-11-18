"""分片续传导入作业。

单进程顺序处理：分片文件 → 全文件校验 → 流式解析 → 暂存去重 → 原子发布。
任何阶段只保留固定大小的读缓冲与一批待写入行，内存不随文件行数增长。
"""

import csv
import fcntl
import hashlib
import io
import json
import logging
import os
import resource
import secrets
import sqlite3
import sys
import time
import unicodedata
from pathlib import Path

from sqlalchemy import delete, func, insert, select, update

from .config.database import Session
from .config.settings import ARTIFACT_DIR, CONFIG
from .models.import_session import ImportChunk, ImportSession
from .models.record import Record
from .models.source import Source
from .models import audit as _audit  # noqa: F401  （注册审计表，保证外键可解析）
from .services.audit import log
from .services.domain_validation import validate_record
from .utils.serialization import now

REQUIRED = ['reference'] + [field[0] for field in CONFIG['fields']]
MAX_REASONS = 50
HASH_BLOCK = 1024 * 1024
ROW_BATCH = 5000
csv.field_size_limit(4 * 1024 * 1024)
PUBLISH_BATCH = 5000


def import_dir(token):
    return Path(ARTIFACT_DIR) / 'imports' / token


def chunk_path(token, index):
    return import_dir(token) / f'{index:08d}.part'


def staging_path(token):
    return import_dir(token) / 'staging.sqlite3'


def progress_path(token):
    return import_dir(token) / 'progress.txt'


def heartbeat_path(token):
    return import_dir(token) / 'heartbeat'


def _beat(record):
    """发布事务提交前无法对外可见行心跳，改用文件心跳，避免被误判为中断。"""
    path = heartbeat_path(record.token)
    temp = path.with_suffix('.tmp')
    temp.write_text(str(time.time()), encoding='utf-8')
    os.replace(temp, path)


def canonical(values):
    return json.dumps(values, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def peak_rss_kib():
    return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)


def expected_size(record, index):
    if index < record.total_chunks - 1:
        return record.chunk_size
    return record.file_size - record.chunk_size * (record.total_chunks - 1)


def _update(db, session_id, **values):
    if 'updated_at' not in values:
        values['updated_at'] = now()
    db.execute(update(ImportSession).where(ImportSession.id == session_id).values(**values))
    db.commit()


def _state(db, session_id):
    return db.execute(select(ImportSession.state).where(ImportSession.id == session_id)).scalar()


def _reject(db, session_id, summary, reasons, invalid_rows=0):
    db.rollback()
    db.execute(update(ImportSession).where(ImportSession.id == session_id).values(
        state='rejected', phase='done', error=summary[:2000], reasons=(reasons or [])[:MAX_REASONS],
        invalid_rows=invalid_rows, completed_at=now(), updated_at=now(), peak_rss_kib=peak_rss_kib()))
    db.commit()
    log(db, 'import', 'import.rejected', session_id, summary[:500])
    db.commit()


def _reopen(db, record, message, missing):
    """分片落盘文件缺失时退回接收中；客户端补片后可再次完成。"""
    db.rollback()
    if missing:
        db.execute(delete(ImportChunk).where(
            ImportChunk.import_id == record.id, ImportChunk.chunk_index.in_(missing)))
        db.commit()
    count, size = db.execute(select(func.count(ImportChunk.id), func.coalesce(func.sum(ImportChunk.size), 0))
                             .where(ImportChunk.import_id == record.id)).one()
    _update(db, record.id, state='receiving', phase='receiving', error=message[:2000],
            received_chunks=int(count), received_bytes=int(size), processed_rows=0,
            processed_bytes=0, queued_at='', reasons=[])


class ChunkReader(io.RawIOBase):
    """把有序分片当作一个连续字节流读取；任一时刻只打开一个分片句柄。"""

    def __init__(self, paths):
        self._paths = list(paths)
        self._sizes = [path.stat().st_size for path in self._paths]
        self._index = 0
        self._handle = None
        self.offset = 0

    def readable(self):
        return True

    def readinto(self, buffer):
        while True:
            if self._handle is None:
                if self._index >= len(self._paths):
                    return 0
                self._handle = open(self._paths[self._index], 'rb')
            read = self._handle.readinto(buffer)
            if read:
                self.offset += read
                return read
            if self._handle.tell() != self._sizes[self._index]:
                self._handle.close()
                self._handle = None
                raise OSError('分片文件在读取过程中发生变化：' + self._paths[self._index].name)
            self._handle.close()
            self._handle = None
            self._index += 1

    def close(self):
        if self._handle is not None:
            self._handle.close()
            self._handle = None
        super().close()


def _whole_file_sha(db, record, paths):
    digest = hashlib.sha256()
    read_total = 0
    for path in paths:
        with open(path, 'rb') as handle:
            while True:
                block = handle.read(HASH_BLOCK)
                if not block:
                    break
                digest.update(block)
                read_total += len(block)
                if read_total % (16 * HASH_BLOCK) < HASH_BLOCK:
                    _beat(record)
                    _update(db, record.id, processed_bytes=min(read_total, record.file_size),
                            heartbeat_at=now())
    return digest.hexdigest()


def _staging(path):
    connection = sqlite3.connect(str(path))
    connection.execute('PRAGMA journal_mode=OFF')
    connection.execute('PRAGMA synchronous=OFF')
    connection.execute('PRAGMA temp_store=FILE')
    connection.execute('PRAGMA cache_size=-16384')
    connection.execute('''CREATE TABLE seen (
        reference TEXT PRIMARY KEY,
        values_json TEXT NOT NULL,
        value_hash TEXT NOT NULL,
        line INTEGER NOT NULL,
        conflict INTEGER NOT NULL DEFAULT 0)''')
    return connection


UPSERT = '''INSERT INTO seen (reference, values_json, value_hash, line) VALUES (?, ?, ?, ?)
ON CONFLICT(reference) DO UPDATE SET conflict=1, line=excluded.line
WHERE seen.value_hash <> excluded.value_hash'''


def _parse(db, record, paths, stage):
    """流式解析。引号内换行由 csv 处理，跨片多字节字符由增量解码器处理。"""
    raw = ChunkReader(paths)
    buffered = io.BufferedReader(raw, buffer_size=64 * 1024)
    text = io.TextIOWrapper(buffered, encoding='utf-8-sig', errors='strict', newline='')
    reader = csv.reader(text, strict=True)
    total = 0
    invalid = 0
    reasons = []
    header = None
    batch = []
    last_progress = 0
    try:
        for fields in reader:
            if not fields:
                continue
            if header is None:
                header = [unicodedata.normalize('NFC', cell).strip() for cell in fields]
                problems = []
                duplicates = sorted({name for name in header if header.count(name) > 1})
                if duplicates:
                    problems.append('表头存在重复列：' + '、'.join(duplicates))
                missing = [name for name in REQUIRED if name not in header]
                if missing:
                    problems.append('表头缺少必需列：' + '、'.join(missing))
                unknown = [name for name in header if name not in REQUIRED]
                if unknown:
                    problems.append('表头包含未知列：' + '、'.join(unknown) + '（请仅保留样本编号与五个工艺字段）')
                if problems:
                    return {'fatal': 'CSV 表头不符合导入格式，整份文件未导入',
                            'reasons': [{'line': reader.line_num, 'reference': '', 'message': p}
                                        for p in problems]}
                continue
            total += 1
            if len(fields) != len(header):
                invalid += 1
                if len(reasons) < MAX_REASONS:
                    reasons.append({'line': reader.line_num, 'reference': '',
                                    'message': f'第 {total} 条记录字段数为 {len(fields)}，应为 {len(header)}'})
                continue
            row = dict(zip(header, fields))
            reference = unicodedata.normalize('NFC', (row.get('reference') or '')).strip()
            if not reference:
                invalid += 1
                if len(reasons) < MAX_REASONS:
                    reasons.append({'line': reader.line_num, 'reference': '',
                                    'message': f'第 {total} 条记录样本编号为空'})
                continue
            if len(reference) > 120:
                invalid += 1
                if len(reasons) < MAX_REASONS:
                    reasons.append({'line': reader.line_num, 'reference': reference[:120],
                                    'message': f'第 {total} 条记录样本编号超过 120 字符'})
                continue
            row['reference'] = reference
            try:
                values = validate_record(row)
            except ValueError as error:
                invalid += 1
                if len(reasons) < MAX_REASONS:
                    reasons.append({'line': reader.line_num, 'reference': reference,
                                    'message': f'第 {total} 条记录（{reference}）：{error}'})
                continue
            body = canonical(values)
            batch.append((reference, body, hashlib.sha256(body.encode('utf-8')).hexdigest(), total))
            if len(batch) >= ROW_BATCH:
                stage.executemany(UPSERT, batch)
                stage.commit()
                batch.clear()
            if total - last_progress >= 20000:
                last_progress = total
                _beat(record)
                _update(db, record.id, processed_rows=total,
                        processed_bytes=min(raw.offset, record.file_size), heartbeat_at=now())
    except UnicodeDecodeError:
        return {'fatal': '文件不是有效的 UTF-8 文本，整份文件未导入',
                'reasons': [{'line': reader.line_num, 'reference': '',
                             'message': 'UTF-8 解码失败，请确认文件编码'}]}
    except csv.Error as error:
        return {'fatal': 'CSV 结构错误，整份文件未导入',
                'reasons': [{'line': reader.line_num, 'reference': '',
                             'message': f'CSV 解析失败：{error}'}]}
    finally:
        text.close()
    if batch:
        stage.executemany(UPSERT, batch)
        stage.commit()
    if header is None:
        return {'fatal': '文件为空，整份文件未导入',
                'reasons': [{'line': 1, 'reference': '', 'message': '文件没有表头'}]}
    if total == 0:
        return {'fatal': '文件没有数据记录，整份文件未导入',
                'reasons': [{'line': reader.line_num, 'reference': '', 'message': '表头之后没有记录'}]}
    valid = total - invalid
    unique = stage.execute('SELECT COUNT(*) FROM seen').fetchone()[0]
    conflict_rows = stage.execute(
        'SELECT reference, line FROM seen WHERE conflict=1 ORDER BY line LIMIT ?',
        (MAX_REASONS,)).fetchall()
    conflicts = stage.execute('SELECT COUNT(*) FROM seen WHERE conflict=1').fetchone()[0]
    return {'total': total, 'unique': unique, 'duplicate': valid - unique, 'invalid': invalid,
            'reasons': reasons, 'conflicts': conflicts,
            'conflict_reasons': [{'line': line, 'reference': reference,
                                  'message': f'样本编号 {reference} 在同一文件中出现不同取值'}
                                 for reference, line in conflict_rows]}


def _write_progress(record, published):
    """发布阶段事务持有至提交，进度另写小文件供状态接口读取。"""
    target = progress_path(record.token)
    temp = target.with_suffix('.tmp')
    temp.write_text(str(published), encoding='utf-8')
    os.replace(temp, target)


def _publish(db, record, stage, source_id):
    """来源与全部记录在同一事务提交，任何失败都会整体回滚，不留半成品。"""
    cursor = stage.execute('SELECT reference, values_json FROM seen ORDER BY rowid')
    inserted = 0
    if db.bind.dialect.name == 'postgresql':
        from psycopg2.extras import Json, execute_values
        statement = f"INSERT INTO {CONFIG['record_table']} (source_id, reference, values) VALUES %s"
        with db.connection().connection.cursor() as dbapi:
            while True:
                block = cursor.fetchmany(PUBLISH_BATCH)
                if not block:
                    break
                execute_values(dbapi, statement,
                               [(source_id, reference, Json(json.loads(body))) for reference, body in block],
                               page_size=len(block))
                inserted += len(block)
                _write_progress(record, inserted)
                _beat(record)
    else:
        while True:
            block = cursor.fetchmany(2000)
            if not block:
                break
            db.execute(insert(Record), [{'source_id': source_id, 'reference': reference,
                                         'values': json.loads(body)} for reference, body in block])
            inserted += len(block)
            _write_progress(record, inserted)
            _beat(record)
    return inserted


def _run(db, record):
    paths = [chunk_path(record.token, index) for index in range(record.total_chunks)]
    missing = [index for index, path in enumerate(paths)
               if not path.exists() or path.stat().st_size != expected_size(record, index)]
    if missing:
        _reopen(db, record, f'{len(missing)} 个已确认分片在磁盘缺失，已退回接收中，请补片后重试', missing)
        return

    _beat(record)
    _update(db, record.id, phase='hashing', processed_bytes=0, heartbeat_at=now())
    if _whole_file_sha(db, record, paths) != record.file_sha256:
        _reject(db, record.id, '拼接后的全文件 SHA-256 与创建时声明不一致，整份文件未导入',
                [{'line': 0, 'reference': '', 'message': '分片重新拼装后的校验和与原文件声明不符'}])
        return

    _update(db, record.id, phase='parsing', heartbeat_at=now())
    staging_file = staging_path(record.token)
    if staging_file.exists():
        staging_file.unlink()
    stage = _staging(staging_file)
    try:
        result = _parse(db, record, paths, stage)
        if result.get('fatal'):
            _reject(db, record.id, result['fatal'], result['reasons'])
            return
        _update(db, record.id, processed_rows=result['total'], processed_bytes=record.file_size,
                unique_rows=result['unique'], duplicate_rows=result['duplicate'],
                invalid_rows=result['invalid'], conflicts=result['conflicts'], heartbeat_at=now())
        if result['invalid']:
            _reject(db, record.id, f'共 {result["invalid"]} 行未通过字段校验，整份文件未导入',
                    result['reasons'], invalid_rows=result['invalid'])
            return
        if result['conflicts']:
            _reject(db, record.id, f'{result["conflicts"]} 个样本编号出现不同取值，整份文件未导入',
                    result['conflict_reasons'])
            return

        _update(db, record.id, phase='publishing', heartbeat_at=now())
        source = Source(key='user-' + secrets.token_hex(10), name=record.source_name,
                        filename=record.source_filename, description=record.source_description or '')
        db.add(source)
        db.flush()
        source_id = source.id
        inserted = _publish(db, record, stage, source_id)
        db.execute(update(ImportSession).where(ImportSession.id == record.id).values(
            state='ready', phase='done', source_id=source_id, published_rows=inserted,
            unique_rows=inserted, heartbeat_at=now(), completed_at=now(), updated_at=now(),
            error='', peak_rss_kib=peak_rss_kib()))
        log(db, record.created_by or 'import', 'import.completed', record.id,
            f'{record.source_name} · {inserted} 条')
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        stage.close()
        staging_file.unlink(missing_ok=True)
        progress_path(record.token).unlink(missing_ok=True)
        heartbeat_path(record.token).unlink(missing_ok=True)
        if _state(db, record.id) in ('ready', 'rejected'):
            for path in paths:
                path.unlink(missing_ok=True)


def execute_import(session_id):
    with Session() as db:
        record = db.get(ImportSession, session_id)
        if record is None or record.state != 'verifying':
            return
        directory = import_dir(record.token)
        directory.mkdir(parents=True, exist_ok=True)
        lock = open(directory / '.lock', 'w')
        try:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return
            _update(db, record.id, heartbeat_at=now())
            try:
                _run(db, record)
            except Exception as error:
                logging.exception('import job failed')
                db.rollback()
                outcome = _state(db, record.id)
                if outcome in ('ready', 'rejected'):
                    return
                _update(db, record.id, state='failed', phase='done',
                        error='处理中断，分片仍保留；可重试完成，整份通过前不会生成来源',
                        peak_rss_kib=peak_rss_kib())
                log(db, record.created_by or 'import', 'import.failed', record.id, str(error)[:500])
                db.commit()
        finally:
            lock.close()


def main():
    if len(sys.argv) != 2:
        raise SystemExit('usage: python -m src.import_job <session-id>')
    execute_import(int(sys.argv[1]))


if __name__ == '__main__':
    main()

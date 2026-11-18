-- 分片续传导入：会话与已确认分片。
-- 表由 SQLAlchemy metadata 幂等创建；本文件记录语义，便于数据库审计与手工核对。
-- 导入会话在 state='verifying' 期间不得被并发作业重复接管；
-- 来源与样本只在全文件 SHA-256、逐行字段、同号异值全部通过后，随发布事务一次性写入。

CREATE TABLE IF NOT EXISTS import_sessions (
    id SERIAL PRIMARY KEY,
    token VARCHAR(32) NOT NULL UNIQUE,
    source_name VARCHAR(160) NOT NULL,
    source_filename VARCHAR(160) NOT NULL,
    source_description TEXT DEFAULT '',
    state VARCHAR(20) NOT NULL DEFAULT 'receiving',
    phase VARCHAR(20) NOT NULL DEFAULT 'receiving',
    file_size BIGINT NOT NULL,
    file_sha256 VARCHAR(64) NOT NULL,
    chunk_size INTEGER NOT NULL,
    total_chunks INTEGER NOT NULL,
    received_chunks INTEGER NOT NULL DEFAULT 0,
    received_bytes BIGINT NOT NULL DEFAULT 0,
    processed_rows BIGINT NOT NULL DEFAULT 0,
    processed_bytes BIGINT NOT NULL DEFAULT 0,
    unique_rows BIGINT NOT NULL DEFAULT 0,
    duplicate_rows BIGINT NOT NULL DEFAULT 0,
    invalid_rows BIGINT NOT NULL DEFAULT 0,
    published_rows BIGINT NOT NULL DEFAULT 0,
    source_id INTEGER REFERENCES drying_studies(id),
    error TEXT DEFAULT '',
    reasons JSON DEFAULT '[]',
    conflicts INTEGER NOT NULL DEFAULT 0,
    created_by VARCHAR(120) DEFAULT '',
    created_at VARCHAR(40) NOT NULL,
    updated_at VARCHAR(40) NOT NULL,
    queued_at VARCHAR(40) DEFAULT '',
    heartbeat_at VARCHAR(40) DEFAULT '',
    completed_at VARCHAR(40) DEFAULT '',
    peak_rss_kib INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS ix_import_session_state ON import_sessions (state);

CREATE TABLE IF NOT EXISTS import_chunks (
    id SERIAL PRIMARY KEY,
    import_id INTEGER NOT NULL REFERENCES import_sessions(id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    size INTEGER NOT NULL,
    sha256 VARCHAR(64) NOT NULL,
    received_at VARCHAR(40) NOT NULL,
    CONSTRAINT uq_import_chunk_index UNIQUE (import_id, chunk_index)
);

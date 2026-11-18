from sqlalchemy import Column,Integer,String,Text,JSON,ForeignKey,BigInteger,UniqueConstraint,Index
from ..config.database import Base
from ..config.settings import CONFIG


class ImportSession(Base):
    """一次分片续传导入。来源与记录只在全文件校验通过后一次性落库。"""
    __tablename__='import_sessions'
    __table_args__=(Index('ix_import_session_state','state'),)
    id=Column(Integer,primary_key=True)
    token=Column(String(32),unique=True,nullable=False)
    source_name=Column(String(160),nullable=False)
    source_filename=Column(String(160),nullable=False)
    source_description=Column(Text,default='')
    state=Column(String(20),nullable=False,default='receiving')
    phase=Column(String(20),nullable=False,default='receiving')
    file_size=Column(BigInteger,nullable=False)
    file_sha256=Column(String(64),nullable=False)
    chunk_size=Column(Integer,nullable=False)
    total_chunks=Column(Integer,nullable=False)
    received_chunks=Column(Integer,default=0,nullable=False)
    received_bytes=Column(BigInteger,default=0,nullable=False)
    processed_rows=Column(BigInteger,default=0,nullable=False)
    processed_bytes=Column(BigInteger,default=0,nullable=False)
    unique_rows=Column(BigInteger,default=0,nullable=False)
    duplicate_rows=Column(BigInteger,default=0,nullable=False)
    invalid_rows=Column(BigInteger,default=0,nullable=False)
    published_rows=Column(BigInteger,default=0,nullable=False)
    source_id=Column(Integer,ForeignKey(CONFIG['source_table']+'.id'),nullable=True)
    error=Column(Text,default='')
    reasons=Column(JSON,default=list)
    conflicts=Column(Integer,default=0,nullable=False)
    created_by=Column(String(120),default='')
    created_at=Column(String(40),nullable=False)
    updated_at=Column(String(40),nullable=False)
    queued_at=Column(String(40),default='')
    heartbeat_at=Column(String(40),default='')
    completed_at=Column(String(40),default='')
    peak_rss_kib=Column(Integer,default=0,nullable=False)


class ImportChunk(Base):
    __tablename__='import_chunks'
    __table_args__=(UniqueConstraint('import_id','chunk_index',name='uq_import_chunk_index'),)
    id=Column(Integer,primary_key=True)
    import_id=Column(Integer,ForeignKey('import_sessions.id',ondelete='CASCADE'),nullable=False)
    chunk_index=Column(Integer,nullable=False)
    size=Column(Integer,nullable=False)
    sha256=Column(String(64),nullable=False)
    received_at=Column(String(40),nullable=False)

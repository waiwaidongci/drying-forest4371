from fastapi import APIRouter,Depends,Header,HTTPException,Request
from ..config.database import db_session
from ..schemas.source import ImportInput
from ..services.auth import current_user,writer
from ..controllers.imports import ImportsController as C
from ..services import imports
MAX_CHUNK_BODY=imports.CHUNK_SIZE
router=APIRouter()
@router.post('/imports')
def create(data:ImportInput,db=Depends(db_session),u=Depends(writer)):return C.create(db,u,data)
@router.get('/imports')
def listing(db=Depends(db_session),u=Depends(current_user)):return C.list(db)
@router.get('/imports/config')
def config():return imports.config()
@router.get('/imports/{id}')
def status(id:int,db=Depends(db_session),u=Depends(current_user)):return C.status(db,id)
@router.put('/imports/{id}/chunks/{index}')
async def chunk(id:int,index:int,request:Request,db=Depends(db_session),u=Depends(writer),x_chunk_sha256:str=Header(default='')):
    declared=request.headers.get('content-length')
    try: declared=int(declared) if declared is not None else None
    except ValueError: declared=None
    if declared is not None and declared>MAX_CHUNK_BODY:raise HTTPException(413,'分片超过允许的大小')
    body=bytearray()
    async for part in request.stream():
        body.extend(part)
        if len(body)>MAX_CHUNK_BODY:raise HTTPException(413,'分片超过允许的大小')
    return C.chunk(db,u,id,index,bytes(body),x_chunk_sha256)
@router.post('/imports/{id}/complete')
def complete(id:int,db=Depends(db_session),u=Depends(writer)):return C.complete(db,u,id)
@router.post('/imports/{id}/retry')
def retry(id:int,db=Depends(db_session),u=Depends(writer)):return C.retry(db,u,id)
@router.post('/imports/{id}/cancel')
def cancel(id:int,db=Depends(db_session),u=Depends(writer)):return C.cancel(db,u,id)

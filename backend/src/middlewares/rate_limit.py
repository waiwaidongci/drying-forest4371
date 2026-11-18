import re,time
from collections import defaultdict,deque
from fastapi.responses import JSONResponse
buckets=defaultdict(deque)
CHUNK_PATH=re.compile(r'^/imports/[0-9]+/chunks/[0-9]+$')
async def rate_limit(request,call_next):
    if request.method in ('POST','PUT','DELETE'):
        path=request.url.path
        # 分片上传是高频续传操作，按导入会话合并计数并放宽上限，其余路径维持每分钟90次
        limit=90
        if CHUNK_PATH.match(path):
            path=re.sub(r'/chunks/[0-9]+$','/chunks',path);limit=600
        key=(request.client.host,path); t=time.monotonic();q=buckets[key]
        while q and t-q[0]>60:q.popleft()
        if len(q)>=limit:return JSONResponse({'detail':'一分钟内操作过多，请稍后重试'},status_code=429)
        q.append(t)
    return await call_next(request)

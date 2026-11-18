# 分片续传导入 — 测试与实测记录

批号 `gb-0044`。源码改动在 `backend/src` 与 `frontend/src`，本目录只放测试脚本与产物，不混入源码。

## 覆盖场景 → 位置

| 场景 | 测试 |
|---|---|
| 乱序分片、引号内换行、跨片多字节字符 | `test_imports.py::test_happy_path_out_of_order_multiline_multibyte`、`test_multibyte_character_split_across_chunks` |
| 同号同值合并、同号异值整份拒绝 | `test_imports.py::test_same_id_same_value_merges_and_same_id_different_value_rejects` |
| 片号相同内容不同报冲突；同内容重传不重复计数 | `test_imports.py::test_same_index_different_content_conflict_and_retransmit_same_content` |
| 全文件校验和不符整份拒绝、不生成来源 | `test_imports.py::test_whole_file_checksum_mismatch_rejects_without_source` |
| 分片未齐不得进入校验；完成与补片交错 | `test_imports.py::test_complete_requires_all_chunks`、`test_interleaved_chunk_and_complete`、`test_recovery.py::test_complete_interleaved_with_upload` |
| 重试完成不新增来源、已完成幂等 | `test_imports.py::test_duplicate_chunk_registration_survives_recreate_and_retry_ready_is_idempotent`、`test_failed_job_retry_does_not_duplicate_source` |
| 已确认分片落盘后中断可恢复 | `test_imports.py::test_missing_disk_chunk_returns_to_receiving_and_can_be_restored`、`test_recovery.py::test_worker_killed_then_retry` |
| 并发补片（同片冲突 / 不同片并行） | `test_recovery.py::test_concurrent_same_chunk_conflict`、`test_concurrent_distinct_chunks` |
| 非法字段、坏表头、坏编码整份拒绝并给原因 | `test_imports.py::test_invalid_field_row_rejects_whole_file`、`test_bad_header_and_bad_encoding_reject` |
| 页面进度与拒绝原因渲染 | `render_check.mjs` |
| 真实前端模块 + 真实 HTTP 服务端到端 | `e2e_importer.mjs`；百万行 `big_e2e.mjs` |

## 运行

```bash
# 单元/接口级（SQLite + 线程内作业，无需 Redis/Postgres）
cd backend && python3 -m pytest ../test-artifacts/gb-0044/test_imports.py -q

# 真实服务（独立作业进程）。先起一个带假 Redis 的本地服务：
#   见下方“本地服务”片段 → 127.0.0.1:19999
cd test-artifacts/gb-0044 && node e2e_importer.mjs && node render_check.mjs
python3 test_recovery.py

# 百万行实测（先生成 CSV）
python3 test-artifacts/gb-0044/gen_csv.py /tmp/big.csv 1000000
node test-artifacts/gb-0044/big_e2e.mjs
```

本地服务片段（仅测试用，生产走 docker compose）：

```python
import sys, os
sys.path.insert(0, '/workspace/backend')
os.environ.update(DATABASE_URL='sqlite:////tmp/e2e.db', ARTIFACT_DIR='/tmp/e2e-art',
                  SEED_DEMO='false', IMPORT_RESUME='true')
import fakeredis, redis
redis.Redis.from_url = staticmethod(lambda *a, **k: fakeredis.FakeRedis())
import src.services.runs as runs
runs.queue = fakeredis.FakeRedis()
from src import main
main.queue = runs.queue
import uvicorn
uvicorn.run(main.app, host='127.0.0.1', port=19999, log_level='warning')
```

E2E 脚本需要账号 `analyst@e2e.test` / `Research2026!`（脚本外创建）。

## 实测结果（2026-11-18，容器内 SQLite，独立作业进程）

| 文件 | 行数 | 体积 | 作业峰值 RSS | 结果 |
|---|---|---|---|---|
| /tmp/big.csv | 1,000,000 | 42 MiB | 67.6 MiB | ready，1,000,000 行，19.8 s |
| /tmp/dup.csv | 1,000,000（80% 同号同值） | 31 MiB | 67.8 MiB | ready，198,646 行，合并 801,354，12.4 s |
| /tmp/big2.csv | 2,000,000 | 67 MiB | 67.3 MiB | ready，2,000,000 行，36.4 s |
| 浏览器路径 4 MiB × 11 片 | 1,000,000 | 42 MiB | 78.3 MiB（含 FastAPI 服务进程） | ready |

峰值内存与行数无关：1M 与 2M 行均为约 68 MiB，低于 128 MiB 上限。发布阶段事务本身不堆积内存，暂存用磁盘 SQLite，读缓冲固定 64 KiB，每批仅 5000 行。

## 未覆盖面

- PostgreSQL 实库未在本机跑通（无 PostgreSQL 二进制与 root 权限）；发布分支使用 `psycopg2.extras.execute_values`，SQL 与列名经静态核对，`docker compose` 环境需实际跑一次 `test_imports.py` 之外的冒烟。
- 浏览器缺系统库（libnss3 等）且无 root，未跑 Playwright 真机点击；前端以真实模块 + 真实 HTTP 覆盖，DOM 渲染单独校验转义与状态。

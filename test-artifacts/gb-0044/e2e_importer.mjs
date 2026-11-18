// 以真实前端模块 + 真实 HTTP 服务验证：分片上传、断点续传、进度与拒绝原因。
const BASE = 'http://127.0.0.1:19999';
const realFetch = globalThis.fetch;
globalThis.fetch = (path, options) => realFetch(path.startsWith('/api/') ? BASE + path.slice(4) : path.startsWith('/') ? BASE + path : path, options);
const stubs = new Map();
globalThis.document = {
  getElementById: id => stubs.get(id) || {textContent: '', className: '', hidden: true, scrollIntoView() {}},
  querySelector: () => null,
  querySelectorAll: () => []
};
globalThis.window = {noticeTimer: null};
globalThis.location = {port: '19999', hash: ''};
globalThis.localStorage = {getItem: () => null, setItem() {}, removeItem() {}};
const { importer, startImport, resumeUpload, loadImports } = await import('/workspace/frontend/src/hooks/importer.js');
const { session } = await import('/workspace/frontend/src/stores/session.js');
const { sha256 } = await import('/workspace/frontend/src/utils/sha256.js');
const { createHash } = await import('node:crypto');
const { readFileSync, writeFileSync } = await import('node:fs');

const login = await (await fetch('/auth/login', {method: 'POST', headers: {'Content-Type': 'application/json'},
  body: JSON.stringify({email: 'analyst@e2e.test', password: 'Research2026!'})})).json();
session.token = login.token;

function makeCsv(rows) {
  const lines = ['reference,moisture,thickness,temperature,airflow,minutes'];
  for (let i = 0; i < rows; i++) lines.push(`HX-E2E-${String(i).padStart(6, '0')},${30 + i % 15},4.2,75,3.1,${80 + i % 20}`);
  // 引号内换行 + 多字节
  lines.push('"HX-E2E-多字节\n跨行",33,4.1,76,3,88');
  return Buffer.from(lines.join('\r\n') + '\r\n', 'utf8');
}

await loadImports();
const sourceIdsBefore = (await (await fetch('/sources', {headers: {Authorization: 'Bearer ' + session.token}})).json()).map(s => s.id);
console.log('config', importer.config.chunk_size);

// 1) 顺序异常：乱序 + 重传在接口层已由 pytest 覆盖；此处跑完整浏览器路径
const content = makeCsv(20000);
const file = new File([content], 'e2e.csv', {type: 'text/csv'});
const expectSha = createHash('sha256').update(content).digest('hex');
console.log('file bytes', content.length, 'chunks', Math.ceil(content.length / importer.config.chunk_size));
if (Math.ceil(content.length / importer.config.chunk_size) < 5) throw new Error('test file must span several chunks');

const started = await startImport(file, {name: 'E2E 补录', filename: 'e2e.csv', description: '端到端'});
console.log('created', started.id, started.state, started.total_chunks);
if (started.file_sha256 !== expectSha) throw new Error('declared sha mismatch');

let state;
for (let i = 0; i < 200; i++) {
  await new Promise(r => setTimeout(r, 200));
  state = await (await fetch('/imports/' + started.id, {headers: {Authorization: 'Bearer ' + session.token}})).json();
  if (['ready', 'rejected', 'failed'].includes(state.state)) break;
}
console.log('final', state.state, 'rows', state.unique_rows, 'percent', state.percent, 'peakMiB', (state.peak_rss_kib / 1024).toFixed(1));
if (state.state !== 'ready') throw new Error('import not ready: ' + JSON.stringify(state).slice(0, 400));
if (state.processed_rows !== 20001 || state.unique_rows !== 20001) throw new Error('row count wrong');

// 2) 断点续传：先用错文件续传应被拒绝；再用原文件补齐
const second = new File([makeCsv(900)], 'other.csv', {type: 'text/csv'});
const session2 = await (await fetch('/imports', {method: 'POST', headers: {'Content-Type': 'application/json', Authorization: 'Bearer ' + session.token},
  body: JSON.stringify({name: 'E2E 续传', filename: 'resume.csv', description: '续传', file_size: file.size,
    file_sha256: expectSha, total_chunks: Math.ceil(file.size / importer.config.chunk_size)})})).json();
importer.active = await (await fetch('/imports/' + session2.id, {headers: {Authorization: 'Bearer ' + session.token}})).json();
let rejectedWrongFile = false;
try { await resumeUpload(second); } catch (error) { rejectedWrongFile = /不一致/.test(error.message); }
console.log('wrong file rejected', rejectedWrongFile);
if (!rejectedWrongFile) throw new Error('wrong resume file accepted');

await resumeUpload(file);
for (let i = 0; i < 200; i++) {
  await new Promise(r => setTimeout(r, 200));
  state = await (await fetch('/imports/' + session2.id, {headers: {Authorization: 'Bearer ' + session.token}})).json();
  if (['ready', 'rejected', 'failed'].includes(state.state)) break;
}
console.log('resumed', state.state, 'rows', state.unique_rows);
if (state.state !== 'ready' || state.unique_rows !== 20001) throw new Error('resume failed');

// 3) 同号异值：整份拒绝并给出原因
const bad = Buffer.from('reference,moisture,thickness,temperature,airflow,minutes\r\n' +
  'HX-DUP,30,4,70,3,80\r\nHX-DUP,31,4,70,3,80\r\n', 'utf8');
const badFile = new File([bad], 'bad.csv', {type: 'text/csv'});
await startImport(badFile, {name: 'E2E 冲突', filename: 'bad.csv', description: ''});
for (let i = 0; i < 100; i++) {
  await new Promise(r => setTimeout(r, 200));
  state = await (await fetch('/imports/' + importer.active.id, {headers: {Authorization: 'Bearer ' + session.token}})).json();
  if (['ready', 'rejected', 'failed'].includes(state.state)) break;
}
console.log('conflict', state.state, state.error, JSON.stringify(state.reasons));
if (state.state !== 'rejected' || !state.reasons.length) throw new Error('conflict not rejected with reasons');

const sources = await (await fetch('/sources', {headers: {Authorization: 'Bearer ' + session.token}})).json();
const added = sources.filter(s => !sourceIdsBefore.includes(s.id)).map(s => s.name);
console.log('new sources', added);
if (added.length !== 2) throw new Error('unexpected sources: ' + sources.map(s => s.id + ':' + s.name));
console.log('E2E OK');

import {createHash} from 'node:crypto';
import {readFileSync, statSync} from 'node:fs';
const BASE = 'http://127.0.0.1:19999';
const realFetch = globalThis.fetch;
globalThis.fetch = (path, options) => realFetch(path.startsWith('/api/') ? BASE + path.slice(4) : path.startsWith('/') ? BASE + path : path, options);
globalThis.location = {port: '19999', hash: ''};
globalThis.localStorage = {getItem: () => null, setItem() {}, removeItem() {}};
globalThis.window = {noticeTimer: null};
globalThis.document = {getElementById: () => ({textContent: '', className: '', hidden: true, scrollIntoView() {}}),
  querySelector: () => null, querySelectorAll: () => []};
const {importer, startImport} = await import('/workspace/frontend/src/hooks/importer.js');
const {session} = await import('/workspace/frontend/src/stores/session.js');
const {loadImports} = await import('/workspace/frontend/src/hooks/importer.js');
const login = await (await fetch('/auth/login', {method: 'POST', headers: {'Content-Type': 'application/json'},
  body: JSON.stringify({email: 'analyst@e2e.test', password: 'Research2026!'})})).json();
session.token = login.token;
await loadImports();
const path = '/tmp/big.csv';
const content = readFileSync(path);
const file = new File([content], 'big.csv', {type: 'text/csv'});
if (file.size !== content.length) throw new Error('File size mismatch');
console.log('uploading', content.length, 'bytes in', importer.config.chunk_size, 'byte chunks');
const created = await startImport(file, {name: '百万行补录', filename: 'big.csv', description: '峰值验证'});
const expected = createHash('sha256').update(content).digest('hex');
if (created.file_sha256 !== expected) throw new Error('sha mismatch');
let state;
for (let i = 0; i < 1200; i++) {
  await new Promise(r => setTimeout(r, 250));
  state = await (await fetch('/imports/' + created.id, {headers: {Authorization: 'Bearer ' + session.token}})).json();
  if (i % 20 === 0) console.log('progress', state.state, state.phase, 'received', state.received_chunks + '/' + state.total_chunks, 'rows', state.processed_rows);
  if (['ready', 'rejected', 'failed'].includes(state.state)) break;
}
console.log('final', state.state, 'rows', state.unique_rows, 'dups', state.duplicate_rows, 'peakMiB', (state.peak_rss_kib / 1024).toFixed(1), 'error', state.error);
if (state.state !== 'ready' || state.unique_rows !== 1000000) throw new Error('big import failed');
console.log('BIG E2E OK');

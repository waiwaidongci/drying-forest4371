import {importsApi} from '../api/research.js';
import {session} from '../stores/session.js';
import {sha256} from '../utils/sha256.js';
import {esc} from '../utils/format.js';
import {toast} from './actions.js';

const SLICE = 4 * 1024 * 1024;

export const importer = {
  active: null,
  dismissed: new Set(),
  uploading: false,
  error: '',
  items: [],
  config: {chunk_size: SLICE, max_file_size: 2 * 1024 * 1024 * 1024},
  form: {name: '', filename: '', description: ''}
};

const stateLabels = {receiving: '接收中', verifying: '校验中', ready: '已可研究', rejected: '已拒绝', failed: '校验中断'};
const phaseLabels = {receiving: '等待分片', queued: '排队等待', hashing: '全文件校验', parsing: '解析与判重', publishing: '写入样本', done: '结束'};

export function importStateLabel(item) {
  if (item.state === 'verifying') return '校验中 · ' + (phaseLabels[item.phase] || '');
  return stateLabels[item.state] || item.state;
}

export function importBadge(item) {
  const tone = item.state === 'ready' ? 'completed' : ['rejected', 'failed'].includes(item.state) ? 'failed' : 'running';
  return '<span class="status ' + tone + '">' + esc(importStateLabel(item)) + '</span>';
}

function progress(item) {
  const percent = Math.max(0, Math.min(100, Number(item.percent ?? item.percent_processed ?? 0)));
  return '<div class="import-progress" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="' + percent + '"><i style="width:' + percent + '%"></i></div>';
}

function reasons(item) {
  if (item.state !== 'rejected' && item.state !== 'failed') return '';
  const list = (item.reasons || []).map(r => '<li><code>' + (r.line ? '第 ' + r.line + ' 行' : '整份文件') + '</code>' +
    (r.reference ? ' <b>' + esc(r.reference) + '</b>' : '') + ' ' + esc(r.message) + '</li>').join('');
  return '<div class="import-reasons"><strong>' + (item.state === 'rejected' ? '拒绝原因' : '中断原因') + '</strong><p>' +
    esc(item.error || '整份文件未导入') + '</p>' + (list ? '<ul>' + list + '</ul>' : '') +
    '<small>被拒绝或中断的文件不产生来源与样本；可核对后重新导回。</small></div>';
}

function summary(item) {
  const bits = ['分片 ' + item.received_chunks + ' / ' + item.total_chunks + '（' + item.percent_received + '%）'];
  if (item.state !== 'receiving') {
    bits.push('已解析 ' + item.processed_rows + ' 行');
    if (item.unique_rows) bits.push('唯一编号 ' + item.unique_rows);
    if (item.duplicate_rows) bits.push('同号同值合并 ' + item.duplicate_rows);
    if (item.invalid_rows) bits.push('非法行 ' + item.invalid_rows);
    if (item.phase === 'publishing') bits.push('已写入 ' + item.published_rows + ' / ' + item.unique_rows);
  }
  bits.push('峰值内存 ' + (item.peak_rss_kib ? (item.peak_rss_kib / 1024).toFixed(1) + ' MiB' : '—'));
  return '<small>' + esc(bits.join(' · ')) + '</small>';
}

function itemCard(item, detailed = false) {
  const source = item.state === 'ready' && item.source_id
    ? '<p class="fine">已生成来源 #' + item.source_id + '；逐行校验与全文件校验和一致后，才从接收中转为可研究。</p>' : '';
  return '<article class="import-card" data-import="' + item.id + '">' +
    '<header><div><strong>' + esc(item.source_name) + '</strong><small>' + esc(item.source_filename) + ' · ' +
    (item.file_size / 1024 / 1024).toFixed(2) + ' MiB · #' + item.id + '</small></div>' + importBadge(item) + '</header>' +
    progress(item) +
    '<p class="fine">' + esc(importStateLabel(item)) + ' · 总进度 ' + Math.max(0, Math.min(100, Number(item.percent ?? 0))) + '%</p>' +
    summary(item) + source + (detailed ? reasons(item) : '') + '</article>';
}

export function importPanel() {
  const active = importer.active;
  if (!active) {
    return '<h2>分片导入新来源</h2><p class="fine">补录文件超过内存时，按 ' +
      (importer.config.chunk_size / 1024 / 1024) + ' MiB 分片续传：乱序、重传与并发补片均核对原文件，整份校验通过才生成来源。</p>' +
      '<form id="import-form"><label>来源名称<input name="name" required minlength="2" maxlength="160" value="' + esc(importer.form.name) + '" placeholder="十一月补录研究"></label>' +
      '<label>文件名<input name="filename" required minlength="3" maxlength="160" value="' + esc(importer.form.filename) + '" placeholder="drying-forest-202611-04.csv"></label>' +
      '<label>说明<textarea name="description" maxlength="2000">' + esc(importer.form.description) + '</textarea></label>' +
      '<label>补录 CSV<input id="import-file" type="file" accept=".csv,text/csv" required></label>' +
      '<p class="fine">表头需含 <code>reference</code> 及 moisture / thickness / temperature / airflow / minutes；允许引号内换行。同号同值合并，同号异值整份拒绝。</p>' +
      '<button type="submit">开始分片导入 →</button></form>' +
      (importer.error ? '<p class="notice error" style="position:static">' + esc(importer.error) + '</p>' : '') +
      '<div id="import-live" aria-live="polite"></div>';
  }
  const item = active;
  const actions = [];
  if (item.state === 'receiving' || item.state === 'failed') {
    if (item.received_chunks < item.total_chunks) actions.push('<button id="import-resume" type="button">选择原文件继续补片</button>');
    actions.push('<button id="import-complete" type="button" class="secondary">尝试提交整份校验</button>');
    actions.push('<button id="import-cancel" type="button" class="secondary">取消导入</button>');
  } else if (item.state === 'verifying' && item.stale) {
    actions.push('<button id="import-retry" type="button">校验中断后重试完成</button>');
  } else if (item.state === 'rejected') {
    actions.push('<button id="import-reset" type="button">核对原文件后重新导入</button>');
  } else if (item.state === 'ready') {
    actions.push('<a class="text-link" href="#source">查看新来源样本 ↗</a>');
    actions.push('<button id="import-reset" type="button" class="secondary">再导入一份</button>');
  }
  return '<h2>正在导入</h2>' + itemCard(item, true) +
    (['receiving', 'failed'].includes(item.state) ? '<p class="fine">关闭页面后可用同一文件继续补片；相同分片重传不会重复计数，相同片号内容不同会报冲突。</p>' : '') +
    (importer.error ? '<p class="notice error" style="position:static">' + esc(importer.error) + '</p>' : '') +
    '<div class="import-actions">' + actions.join('') + '</div>' +
    (importer.uploading ? '<p class="fine"><span class="spinner"></span> 正在上传分片；进度由服务端记录，可离开本页稍后回来。</p>' : '');
}

export function importHistory() {
  const items = importer.items.filter(item => !importer.active || item.id !== importer.active.id);
  if (!items.length) return '';
  return '<h2>近期导入</h2><div class="import-list">' + items.map(item => itemCard(item)).join('') + '</div>';
}

export async function loadImports() {
  try {
    importer.config = await importsApi.config();
    importer.items = await importsApi.list();
    const focus = importer.active
      ? importer.items.find(item => item.id === importer.active.id) || importer.active
      : importer.items.find(item => ['receiving', 'verifying', 'failed'].includes(item.state) && !importer.dismissed.has(item.id));
    if (focus) importer.active = await importsApi.get(focus.id);
  } catch (error) {
    importer.error = error.message;
  }
}

function chunkSize() {
  return Number(importer.config.chunk_size) || SLICE;
}

async function readSlice(file, index) {
  const size = chunkSize();
  return new Uint8Array(await file.slice(index * size, Math.min(file.size, (index + 1) * size)).arrayBuffer());
}

async function digestFile(file) {
  const total = Math.max(1, Math.ceil(file.size / chunkSize()));
  const digest = sha256();
  for (let index = 0; index < total; index++) {
    digest.update(await readSlice(file, index));
    if (index % 16 === 0) await new Promise(resolve => setTimeout(resolve, 0));
  }
  return digest.hex();
}

async function put(index, bytes) {
  const chunkDigest = sha256();
  chunkDigest.update(bytes);
  const response = await fetch('/api/imports/' + importer.active.id + '/chunks/' + index, {
    method: 'PUT',
    headers: {'Content-Type': 'application/octet-stream', 'X-Chunk-Sha256': chunkDigest.hex(),
      ...(session.token ? {Authorization: 'Bearer ' + session.token} : {})},
    body: bytes
  });
  if (!response.ok) {
    let body = {};
    try { body = await response.json(); } catch { /* 保持回退文案 */ }
    throw new Error(typeof body.detail === 'string' ? body.detail : '分片 ' + index + ' 上传失败');
  }
  return response.json();
}

async function uploadMissing(file) {
  const item = importer.active;
  const done = new Set(item.received_indexes || []);
  importer.uploading = true;
  try {
    for (let index = 0; index < item.total_chunks; index++) {
      if (done.has(index)) continue;
      await put(index, await readSlice(file, index));
      await new Promise(resolve => setTimeout(resolve, 0));
    }
    await importsApi.complete(item.id);
    importer.active = await importsApi.get(item.id);
    toast('分片已全部到齐，正在校验整份文件');
  } catch (error) {
    importer.error = error.message;
    throw error;
  } finally {
    importer.uploading = false;
  }
}

/** 校验哈希并创建会话；分片在后台上传，页面轮询服务端进度。 */
export async function startImport(file, meta) {
  importer.error = '';
  const max = Number(importer.config.max_file_size);
  if (max && file.size > max) throw new Error('文件超过允许的 ' + (max / 1024 / 1024) + ' MiB 上限');
  const digest = await digestFile(file);
  const created = await importsApi.create({...meta, file_size: file.size, file_sha256: digest,
    total_chunks: Math.max(1, Math.ceil(file.size / chunkSize()))});
  importer.active = await importsApi.get(created.id);
  uploadMissing(file).catch(() => {});
  return importer.active;
}

export async function resumeUpload(file) {
  const item = importer.active;
  if (!item) throw new Error('没有进行中的导入');
  const digest = await digestFile(file);
  if (digest !== item.file_sha256) {
    throw new Error('所选文件与原文件不一致（SHA-256 不同），请换回原文件；相同片号内容不同会被拒绝');
  }
  importer.error = '';
  uploadMissing(file).catch(() => {});
}

export function resetImport() {
  if (importer.active) importer.dismissed.add(importer.active.id);
  importer.active = null;
  importer.error = '';
  importer.form = {name: '', filename: '', description: ''};
}

export function rememberFile(file) {
  importer.form.filename = file.name;
}

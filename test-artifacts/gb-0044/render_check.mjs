globalThis.location = {port: '19999', hash: ''};
globalThis.localStorage = {getItem: () => null, setItem() {}, removeItem() {}};
const stubs = new Map();
globalThis.document = {getElementById: id => stubs.get(id) || {textContent: '', className: '', hidden: true, scrollIntoView() {}},
  querySelector: () => null, querySelectorAll: () => []};
globalThis.window = {noticeTimer: null};
const { importer, importPanel, importHistory, importStateLabel } = await import('/workspace/frontend/src/hooks/importer.js');
const base = {id: 7, source_name: '补录<来源>', source_filename: 'a&b.csv', file_size: 42 * 1024 * 1024,
  total_chunks: 11, received_chunks: 4, percent_received: 36.4, percent: 36.4, percent_processed: 36.4,
  processed_rows: 0, unique_rows: 0, duplicate_rows: 0, invalid_rows: 0, published_rows: 0,
  peak_rss_kib: 0, reasons: [], error: '', phase: 'receiving'};
for (const state of ['receiving', 'verifying', 'ready', 'rejected', 'failed']) {
  importer.active = {...base, state, stale: state === 'verifying' && true,
    reasons: state === 'rejected' ? [{line: 12, reference: 'HX-<1>', message: '样本编号 HX-<1> 出现不同取值'}] : [],
    error: state === 'rejected' ? '1 个样本编号出现不同取值，整份文件未导入' : '',
    source_id: state === 'ready' ? 3 : null};
  const html = importPanel();
  if (!html.includes('import-progress')) throw new Error('missing progress for ' + state);
  if (state === 'rejected' && (!html.includes('拒绝原因') || !html.includes('&lt;1&gt;'))) throw new Error('reason rendering');
  if (html.includes('<script')) throw new Error('unsafe html');
  console.log(state, 'panel ok', importStateLabel(importer.active), html.length);
}
importer.active = null;
console.log('empty panel ok', importPanel().includes('分片导入新来源'));
importer.items = [{...base, state: 'ready', source_id: 9, percent: 100}];
console.log('history ok', importHistory().includes('近期导入'));
console.log('RENDER OK');

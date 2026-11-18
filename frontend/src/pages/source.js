import {domain} from '../constants/domain.js';
import {research} from '../stores/research.js';
import {session} from '../stores/session.js';
import {importsApi, sourcesApi} from '../api/research.js';
import {table} from '../components/common/table.js';
import {sourceOptions} from '../components/common/business.js';
import {esc,formObject} from '../utils/format.js';
import {action,toast,bind} from '../hooks/actions.js';
import {importer,importHistory,importPanel,loadImports,rememberFile,resetImport,resumeUpload,startImport} from '../hooks/importer.js';

export async function sourcePage(){
  research.records=research.sourceId?await sourcesApi.records(research.sourceId):[];
  const selected=research.sources.find(s=>s.id==research.sourceId);
  let imports='';
  if(session.writable){await loadImports();imports='<section class="import-sheet">'+importPanel()+importHistory()+'</section>';}
  return '<section class="page-heading"><p class="eyebrow">02 / SOURCE & EVIDENCE</p><h1>先核对'+esc(domain.record)+'，再解释结果。</h1><p>样本只追加，任务保留创建时的输入快照；后续来源修改不会改写历史计算。</p></section><div class="split"><section><label>来源'+esc(domain.source)+'<select id="source-select">'+sourceOptions(research.sources,research.sourceId)+'</select></label><p class="file-note">'+esc(selected?.filename||'尚未创建来源')+' · '+research.records.length+' 条记录</p>'+table(research.records.map(r=>({reference:r.reference,...r.values})),[{key:'reference',label:'样本编号'},...domain.fields.map(f=>({key:f[0],label:f[1]}))],{limit:15,offset:research.page*15})+'<div class="pagination"><button id="prev" '+(research.page===0?'disabled':'')+'>上一页</button><span>'+(research.page+1)+' / '+Math.max(1,Math.ceil(research.records.length/15))+'</span><button id="next" '+((research.page+1)*15>=research.records.length?'disabled':'')+'>下一页</button></div>'+(session.writable&&selected?'<details><summary>修改来源说明</summary><form id="edit-source"><label>来源名称<input name="name" value="'+esc(selected.name)+'" required></label><label>文件名<input name="filename" value="'+esc(selected.filename)+'" required></label><label>说明<textarea name="description">'+esc(selected.description)+'</textarea></label><button type="submit">更新来源说明</button></form></details>':'')+imports+'</section><aside class="entry-sheet">'+(session.writable?'<h2>追加'+esc(domain.record)+'</h2><form id="record-form"><label>样本编号<input name="reference" required placeholder="'+domain.code+'-NEW-001"></label>'+domain.fields.map(([key,label,type])=>'<label>'+esc(label)+'<input name="'+key+'" type="'+(type==='number'?'number':'text')+'" '+(type==='number'?'step="any"':'')+' required value="'+esc(domain.sample[key])+'"></label>').join('')+'<button type="submit">保存样本 →</button></form><details><summary>新建'+esc(domain.source)+'</summary><form id="new-source"><label>来源名称<input name="name" required placeholder="十一月补充研究"></label><label>文件名<input name="filename" required value="'+domain.slug+'-new.csv"></label><label>业务说明<textarea name="description" required>独立补充采集，供复核研究。</textarea></label><button type="submit">创建来源</button></form></details>':'<h2>阅读权限</h2><p>当前可查看全部业务样本和结果。来源录入由负责人或分析师完成。</p>')+'</aside></div>'
}

let poller=null;
let pendingFile=null;

export function bindSource(refresh){
  bind('#source-select','change',async e=>{research.sourceId=Number(e.target.value);research.page=0;await refresh()});
  bind('#prev','click',async()=>{research.page--;await refresh()});
  bind('#next','click',async()=>{research.page++;await refresh()});
  action('#record-form',async form=>{const data=formObject(form),values={};for(const [key,,type]of domain.fields)values[key]=type==='number'?Number(data[key]):data[key];await sourcesApi.add(research.sourceId,[{reference:data.reference,values}]);await refresh();toast('业务样本已保存')});
  action('#new-source',async form=>{const s=await sourcesApi.create(formObject(form));research.sourceId=s.id;research.page=0;await refresh();toast('新来源已创建，请追加样本')});
  action('#edit-source',async form=>{await sourcesApi.update(research.sourceId,formObject(form));await refresh();toast('来源说明已更新；旧任务快照保留')});
  bindImport(refresh);
}

function bindImport(refresh){
  clearTimeout(poller);
  const fileInput=document.querySelector('#import-file');
  fileInput?.addEventListener('change',()=>{pendingFile=fileInput.files[0]||null;if(pendingFile)rememberFile(pendingFile)});
  action('#import-form',async form=>{
    const data=formObject(form);
    if(!pendingFile)throw Error('请选择要导入的 CSV 文件');
    await startImport(pendingFile,{name:data.name,filename:data.filename,description:data.description});
    pendingFile=null;
    await refresh();
  });
  bind('#import-resume','click',()=>{
    const chooser=document.createElement('input');
    chooser.type='file';chooser.accept='.csv,text/csv';
    chooser.onchange=async()=>{
      if(!chooser.files[0])return;
      try{await resumeUpload(chooser.files[0]);toast('补片已提交，正在重新校验')}catch(error){toast(error.message,true)}
      await refresh();
    };
    chooser.click();
  });
  bind('#import-complete','click',async()=>{
    try{await importsApi.complete(importer.active.id);toast('已提交整份校验，等待结果')}catch(error){toast(error.message,true)}
    await refresh();
  });
  bind('#import-retry','click',async()=>{
    try{await importsApi.retry(importer.active.id);toast('已重新提交校验')}catch(error){toast(error.message,true)}
    await refresh();
  });
  bind('#import-cancel','click',async()=>{
    try{await importsApi.cancel(importer.active.id);resetImport();toast('导入已取消，未产生来源')}catch(error){toast(error.message,true)}
    await refresh();
  });
  bind('#import-reset','click',async()=>{resetImport();await refresh()});
  if(importer.uploading||(importer.active&&['receiving','verifying'].includes(importer.active.state))){
    poller=setTimeout(refresh,1500);
  }
}

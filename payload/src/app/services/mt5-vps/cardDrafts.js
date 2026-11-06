// UI drafts only: no level, order identity, execution state or trading command.
const fs=require('fs');
const path=require('path');
const FIELDS=['risk','stopOffsetPts','maxLot','takeProfitPts','pointSize','zoneMultiplier'];
function clean(value) {
  const result={};
  for(const key of FIELDS)if(value && Object.hasOwn(value,key)) {
    const text=String(value[key]);
    if(text.length<=32 && (text==='' || Number.isFinite(Number(text)) && Number(text)>=0))result[key]=key==='zoneMultiplier' ? Number(text)===2 ? 2 : 1 : text;
  }
  return result;
}
function createDraftStore(directory) {
  const file=path.join(directory,'index-card-drafts.json');let values={},timer,dirty=false;
  try {if(fs.statSync(file).size<=4096) {
    const saved=JSON.parse(fs.readFileSync(file,'utf8'));
    values=Object.fromEntries(['USTEC','SPX500'].map(ticker=>[ticker,clean(saved?.[ticker])]));
  }}catch { /* New or invalid drafts use normal defaults. */ }
  function flush() {
    clearTimeout(timer);timer=null;
    if(!dirty)return;
    try {
      fs.mkdirSync(directory,{recursive:true});
      fs.writeFileSync(file+'.incoming',JSON.stringify(values),{mode:0o600});
      fs.renameSync(file+'.incoming',file);dirty=false;
    }catch { /* Draft failure cannot change execution. */ }
  }
  return {get:ticker=>['USTEC','SPX500'].includes(ticker) ? clean(values[ticker]) : {},flush,
    set(ticker,value) {
      if(!['USTEC','SPX500'].includes(ticker))return;
      const next=clean(value);if(JSON.stringify(clean(values[ticker]))===JSON.stringify(next))return;
      values[ticker]=next;dirty=true;clearTimeout(timer);timer=setTimeout(flush,150);
    }};
}
function restoreDraft(current,remembered,locked=false) {
  return {...current,...(!locked ? clean(remembered) : {})};
}
module.exports={createDraftStore,clean,restoreDraft};

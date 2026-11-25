const {performance}=require('node:perf_hooks');
const {TRADING_SYMBOLS}=require('./tradingSymbols');

function createEntryMilestones({log, wall=Date.now, clock=()=>performance.now()}={}) {
  const pending=new Map();
  const timingSummaries=new Map();
  const terminal=new Set(['completed','stopped','unknown']);
  const validAccount=account=>/^[1-9][0-9]{0,19}$/.test(account);
  function start(payload) {
    const id=payload?.requestId, symbol=payload?.ticker, age=wall()-payload?.clickAt;
    if(typeof id!=='string' || !/^[A-Za-z0-9_.:-]{1,100}$/.test(id)
        || !TRADING_SYMBOLS.includes(symbol) || !Number.isFinite(payload.clickAt)
        || !Number.isFinite(age) || age<0 || age>60000
        || pending.has(id))return;
    pending.set(id,{symbol,click:clock()-age,accounts:new Map()});
    if(pending.size>64)pending.delete(pending.keys().next().value);
  }
  function emit(entryId,row,stage,account) {
    try { log({kind:'chunk-entry-milestone',entryId,symbol:row.symbol,stage,
      ...(account ? {account} : {}),elapsedMs:Math.round(Math.max(0,clock()-row.click))}); }
    catch {} // Logging cannot change the result of a trade.
  }
  function observeTiming(entry) {
    if(!terminal.has(entry?.phase) || typeof entry.id!=='string' || !/^[A-Za-z0-9_.:-]{1,100}$/.test(entry.id))return;
    const existing=timingSummaries.get(entry.id) || new Map();
    for(const [account,details] of Object.entries(entry.accounts || {})) {
      const timing=details?.entryTimingSummary;
      if(!validAccount(account) || timing?.schema!=='entry-timing-summary-v1')continue;
      try {
        const outcome=entry.lanePhases?.[account] || details.entryStatus || entry.phase;
        const record={kind:'chunk-entry-timing-summary',entryId:entry.id,symbol:entry.symbol,account,
          admittedAtMs:entry.createdAt,outcome,filledVolume:details.filledVolume,targetVolume:details.targetVolume,timing};
        const signature=JSON.stringify(record);
        if(signature===existing.get(account) || Buffer.byteLength(signature)>8192)continue;
        log(record);
        existing.set(account,signature);
      } catch {} // Observability is independent of order and protection state.
    }
    if(existing.size)timingSummaries.set(entry.id,existing);
    if(timingSummaries.size>64)timingSummaries.delete(timingSummaries.keys().next().value);
  }
  function observe(entry) {
    observeTiming(entry);
    const row=pending.get(entry?.id);
    if(!row)return;
    if(entry.cancelledBeforeAdmission===true) {
      emit(entry.id,row,'cancelled-before-admission');pending.delete(entry.id);return;
    }
    if(entry.symbol!==row.symbol || !Number.isFinite(entry.createdAt) || entry.createdAt<=0)return;
    if(!row.accepted){row.accepted=true;emit(entry.id,row,'accepted');}
    const accounts=Object.entries(entry.accounts || {});
    for(const [account,state] of accounts) {
      if(!validAccount(account) || !Number.isFinite(state?.filledVolume) || state.filledVolume<=0)continue;
      const previous=row.accounts.get(account) || 0;
      if(!previous)emit(entry.id,row,'first-fill',account);
      const full=Number.isFinite(state.targetVolume) && state.targetVolume>0 && state.filledVolume>=state.targetVolume-1e-8;
      if(full && previous<2)emit(entry.id,row,'target-filled',account);
      row.accounts.set(account,full || previous===2 ? 2 : 1);
    }
    if(entry.phase==='completed' && accounts.length && accounts.every(([account])=>row.accounts.get(account)===2)) {
      emit(entry.id,row,'completed');pending.delete(entry.id);
    } else if(entry.phase==='stopped') {
      emit(entry.id,row,'stopped');pending.delete(entry.id);
    }
  }
  function observeMany(entries) {
    if(Array.isArray(entries))for(const entry of entries)observe(entry);
    return entries;
  }
  function result(id,outcome) {
    if(outcome?.entry)observe(outcome.entry);
    else if(outcome?.status==='rejected')pending.delete(id);
  }
  return {start,observeMany,result};
}

module.exports={createEntryMilestones};

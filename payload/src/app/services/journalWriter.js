const {Worker,isMainThread,parentPort}=require('node:worker_threads');
const {isEntryPlan,usesJournalChanges}=require('./entryPlanJournal');
const now=()=>Number(process.hrtime.bigint())/1e6;

if(!isMainThread) {
  const fs=require('node:fs');
  const {writeDurable}=require('./durableFile');
  const {loadEntryPlans,changeBatch,appendEntryPlanBatch}=require('./entryPlanJournal');
  const files=new Map();
  const tails=new Map();
  parentPort.on('message',message=>{
    const tail=(tails.get(message.file) || Promise.resolve()).catch(()=>{}).then(async()=>{
      try {
        const started=now(),timings={journalWorkerQueueMs:started-message.postedAt};
        if (usesJournalChanges(message.file)) {
          let state=files.get(message.file);
          if (!state) {
            const {validBytes,size}=loadEntryPlans(message.file);
            state={validBytes,size};
            files.set(message.file,state);
          }
          timings.journalWorkerLoadMs=now()-started;
          if (!message.prepare) {
            const serializing=now(),text=changeBatch(message.changes);
            timings.journalSerializeMs=now()-serializing;
            const writing=now();
            // Keep ordered writes off the process-wide asynchronous file queue.
            appendEntryPlanBatch(message.file,state,text);
            timings.journalDurableMs=now()-writing;
          }
          parentPort.postMessage({id:message.id,timings,finishedAt:now()});
          return;
        }
        let rows=files.get(message.file);
        if(!rows || message.replace) {
          const values=message.replace || (fs.existsSync(message.file) ? JSON.parse(fs.readFileSync(message.file,'utf8')) : []);
          if(!Array.isArray(values))throw Error('Invalid durable journal');
          rows=new Map(values.map(row=>[row.id,row]));files.set(message.file,rows);
        }
        timings.journalWorkerLoadMs=now()-started;
        if(message.prepare) {
          parentPort.postMessage({id:message.id,timings,finishedAt:now()});return;
        }
        for(const row of message.changes || [])rows.set(row.id,row);
        const serializing=now(),text=JSON.stringify([...rows.values()]);
        timings.journalSerializeMs=now()-serializing;
        const writing=now();
        await writeDurable(message.file,text);
        timings.journalDurableMs=now()-writing;
        parentPort.postMessage({id:message.id,timings,finishedAt:now()});
      }catch(error){files.delete(message.file);parentPort.postMessage({id:message.id,error:error.message});}
    });
    tails.set(message.file,tail);
    tail.finally(()=>{if(tails.get(message.file)===tail)tails.delete(message.file);});
  });
} else {
  let serial=0;
  const workers=new Map();
  function journalWorker(file='') {
    const kind=isEntryPlan(file) ? 'entry-plans' : usesJournalChanges(file) ? 'mt5-commands' : 'default';
    const existing=workers.get(kind);
    if(existing)return existing;
    const current=new Worker(__filename),pending=new Map();
    const slot={current,pending};
    workers.set(kind,slot);
    const failed=error=>{
      if(workers.get(kind)===slot)workers.delete(kind);
      for(const row of pending.values())row.reject(error);
      pending.clear();
    };
    current.on('message',message=>{
      const row=pending.get(message.id);if(!row)return;
      pending.delete(message.id);
      if(!pending.size)current.unref();
      message.error ? row.reject(Error(message.error)) : row.resolve({...message.timings,
        journalAckMs:now()-message.finishedAt,journalTotalMs:now()-row.postedAt});
    });
    current.on('error',failed);
    current.on('exit',code=>failed(Error('Durable journal worker stopped: '+code)));
    current.unref();
    return slot;
  }
  const warm=file=>journalWorker(file).current;
  function request(file,changes,replace,prepare=false) {
    // Full history rewrites must not hold up durable entry plans.
    const {current,pending}=journalWorker(file),id=++serial;
    // Only the changed records cross threads for command and receipt updates.
    return new Promise((resolve,reject)=>{
      const postedAt=now();
      pending.set(id,{resolve,reject,postedAt});current.ref();
      try{current.postMessage({id,file,changes,replace,prepare,postedAt});}
      catch(error){pending.delete(id);if(!pending.size)current.unref();reject(error);}
    });
  }
  const writeJournal=(file,changes,replace)=>request(file,changes,replace);
  // Read/cache only. No file is created and no durable intent is bypassed.
  const prepareJournal=file=>request(file,undefined,undefined,true);
  module.exports={writeJournal,prepareJournal,warm};
}

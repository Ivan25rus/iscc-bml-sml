const {Worker,isMainThread,parentPort}=require('node:worker_threads');
const now=()=>Number(process.hrtime.bigint())/1e6;

if(!isMainThread) {
  const fs=require('node:fs');
  const {writeDurable}=require('./durableFile');
  const files=new Map();
  const tails=new Map();
  parentPort.on('message',message=>{
    const tail=(tails.get(message.file) || Promise.resolve()).catch(()=>{}).then(async()=>{
      try {
        const started=now(),timings={journalWorkerQueueMs:started-message.postedAt};
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
  let worker,serial=0;
  const pending=new Map();
  function warm() {
    if(worker)return worker;
    const current=new Worker(__filename);worker=current;current.unref();
    const failed=error=>{
      if(worker===current)worker=null;
      for(const [id,row] of pending)if(row.worker===current){pending.delete(id);row.reject(error);}
    };
    current.on('message',message=>{
      const row=pending.get(message.id);if(!row)return;
      pending.delete(message.id);
      if(!pending.size)current.unref();
      message.error ? row.reject(Error(message.error)) : row.resolve({...message.timings,
        journalAckMs:now()-message.finishedAt,journalTotalMs:now()-row.postedAt});
    });
    current.on('error',failed);
    current.on('exit',code=>{if(worker===current)failed(Error('Durable journal worker stopped: '+code));});
    current.unref();
    return current;
  }
  function request(file,changes,replace,prepare=false) {
    // Only the changed records cross threads. Serialize the complete retained
    // history in the worker, preserving the existing atomic JSON file format.
    const current=warm(),id=++serial;
    return new Promise((resolve,reject)=>{
      const postedAt=now();
      pending.set(id,{resolve,reject,worker:current,postedAt});current.ref();
      try{current.postMessage({id,file,changes,replace,prepare,postedAt});}
      catch(error){pending.delete(id);if(!pending.size)current.unref();reject(error);}
    });
  }
  const writeJournal=(file,changes,replace)=>request(file,changes,replace);
  // Read/cache only. No file is created and no durable intent is bypassed.
  const prepareJournal=file=>request(file,undefined,undefined,true);
  module.exports={writeJournal,prepareJournal,warm};
}

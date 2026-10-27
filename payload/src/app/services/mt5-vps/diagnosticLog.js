const fs=require('fs');
const path=require('path');

// Best-effort diagnostics only. Trading intents/results use their separate
// durable journals; this queue must never replace or delay those writes.
function createDiagnosticLog({io=fs.promises,maxPendingBytes=512*1024}={}) {
  const files=new Map();
  return {append(file,row) {
    const line=JSON.stringify(row)+'\n',bytes=Buffer.byteLength(line);
    let state=files.get(file);
    if(!state){state={bytes:0,tail:Promise.resolve(),dropped:0};files.set(file,state);}
    if(state.bytes+bytes>maxPendingBytes){state.dropped++;return Promise.resolve(false);}
    state.bytes+=bytes;
    const task=state.tail.then(async()=>{
      try {
        await io.mkdir(path.dirname(file),{recursive:true,mode:0o700});
        let size=0;
        try {size=(await io.stat(file)).size;}catch(error){if(error.code!=='ENOENT')throw error;}
        if(size>1024*1024)await io.rename(file,file+'.1');
        const dropped=state.dropped;state.dropped=0;
        const text=dropped ? JSON.stringify({...row,droppedDiagnostics:dropped})+'\n' : line;
        await io.appendFile(file,text,{mode:0o600});
        return true;
      } catch {return false;}
      finally {state.bytes-=bytes;}
    });
    state.tail=task;
    return task;
  }};
}
const diagnosticLog=createDiagnosticLog();
module.exports={createDiagnosticLog,diagnosticLog};

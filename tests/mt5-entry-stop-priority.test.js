const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {MT5VPSAdapter}=require('../payload/src/app/services/mt5-vps/client');
const {CommandJournal}=require('../payload/src/app/services/mt5-vps/commandJournal');
const capabilities=['chunked-breakout','independent-account-engines-v5','entry-account-continue-v1','entry-stop-priority-v1'];
function fixture(t) {
  const directory=fs.mkdtempSync(path.join(os.tmpdir(),'iscc-stop-priority-'));t.after(()=>fs.rmSync(directory,{recursive:true,force:true}));
  const adapter=new MT5VPSAdapter({enabled:true,account:'900000002',server:'broker'});t.after(()=>adapter.close());
  adapter.capabilities=capabilities.slice();adapter.commandJournal=new CommandJournal(path.join(directory,'commands.json'));
  const f={adapter,calls:[],reply:(_route,payload)=>({version:39,capabilities,
    entry:{id:payload.entryId,phase:'stopped',controlContract:'entry-stop-priority-v1'}})};
  adapter.request=async(route,payload,options)=>{f.calls.push({route,payload,options});return f.reply(route,payload);};
  return f;
}
const turn=()=>new Promise(resolve=>setImmediate(resolve));

test('Stop captures both continuation IDs before their delayed responses and persists the exact revocations before POST',async t=>{
  const f=fixture(t),pending=[];
  f.reply=(route,payload)=>{
    if(route==='/entry-plan-continue')return new Promise(resolve=>pending.push(resolve));
    assert.equal(route,'/entry-plan-stop');
    assert.deepEqual(payload.cancelContinueIds,['continue-one','continue-two','queued-before-main']);
    const saved=f.adapter.chunkEntryJournal.records.find(row=>row.id===payload.id);
    assert.equal(saved.result.status,'unknown');assert.deepEqual(saved.payload.cancelContinueIds,payload.cancelContinueIds);
    return {version:39,capabilities,entry:{id:payload.entryId,phase:'stopped'}};
  };
  const first=f.adapter.continueChunkEntry('parent','900000001',{requestId:'continue-one'});
  const second=f.adapter.continueChunkEntry('parent','900000002',{requestId:'continue-two'});
  while(pending.length<2)await turn();
  assert.equal((await f.adapter.stopChunkEntry('parent',{cancelContinueIds:['queued-before-main']})).status,'ok');
  assert.deepEqual(f.calls.map(row=>row.route),['/entry-plan-continue','/entry-plan-continue','/entry-plan-stop']);
  for(const resolve of pending)resolve({version:39,capabilities,entry:{id:'parent',phase:'running'}});
  assert.ok((await Promise.all([first,second])).every(row=>row.status==='ok'));
});

test('A stop after a new explicit continuation uses a new semantic ID instead of returning the old successful stop receipt',async t=>{
  const f=fixture(t);
  await f.adapter.stopChunkEntry('parent');
  const old=f.calls[0].payload.id;
  await f.adapter.continueChunkEntry('parent','900000001',{requestId:'resume-after-stop'});
  await f.adapter.stopChunkEntry('parent');
  const stops=f.calls.filter(row=>row.route==='/entry-plan-stop');
  assert.equal(stops.length,2);assert.notEqual(stops[1].payload.id,old);
  assert.deepEqual(stops[1].payload.cancelContinueIds,['resume-after-stop']);
  await f.adapter.stopChunkEntry('parent');assert.equal(f.calls.filter(row=>row.route==='/entry-plan-stop').length,2);
});

test('Unknown stop keeps its exact ID and revocations on explicit retry and blocks another new continuation',async t=>{
  const f=fixture(t);let fail=true;
  f.reply=(_route,payload)=>{if(fail)throw Error('lost reply');return {version:39,capabilities,entry:{id:payload.entryId,phase:'stopped'}};};
  assert.equal((await f.adapter.stopChunkEntry('parent',{cancelContinueIds:['old-resume']})).status,'unknown');
  const first=f.calls[0].payload;
  assert.equal((await f.adapter.continueChunkEntry('parent','900000001',{requestId:'new-resume'})).status,'rejected');
  assert.equal(f.calls.length,1);fail=false;
  assert.equal((await f.adapter.stopChunkEntry('parent')).status,'ok');
  assert.equal(f.calls[1].payload.id,first.id);assert.deepEqual(f.calls[1].payload.cancelContinueIds,first.cancelContinueIds);
  assert.equal((await f.adapter.continueChunkEntry('parent','900000001',{requestId:'new-resume'})).status,'ok');
});

test('Unconfirmed new receiver capability and malformed revocations fail before transmission',async t=>{
  const f=fixture(t);
  for(const cancelContinueIds of ['x',[null],['bad id'],Array(2049).fill('x')])
    assert.equal((await f.adapter.stopChunkEntry('parent',{cancelContinueIds})).status,'rejected');
  f.adapter.capabilities=f.adapter.capabilities.filter(value=>value!=='entry-stop-priority-v1');
  assert.equal((await f.adapter.stopChunkEntry('parent',{cancelContinueIds:['resume']})).status,'rejected');
  assert.equal(f.calls.length,0);
  assert.equal((await f.adapter.stopChunkEntry('parent')).status,'ok');
  assert.equal(Object.hasOwn(f.calls[0].payload,'cancelContinueIds'),false,'Old receiver stop requests retain their protocol');
});

test('Exact stop revocation settles a lost continuation and permits a new explicit account continuation',async t=>{
  const f=fixture(t);f.adapter.capabilities.push('entry-control-recovery-v1');
  f.reply=(route,payload)=>{
    if(payload.id==='lost-resume')throw Error('lost continuation reply');
    return {version:39,capabilities:f.adapter.capabilities,entry:{id:payload.entryId,phase:'stopped'},
      ...(route==='/entry-plan-stop' ? {entryControlReceipt:{schema:'entry-control-recovery-v1',id:payload.id,
        entryId:payload.entryId,cancelContinueIds:payload.cancelContinueIds}} : {})};
  };
  assert.equal((await f.adapter.continueChunkEntry('parent','900000001',{requestId:'lost-resume'})).status,'unknown');
  assert.equal((await f.adapter.stopChunkEntry('parent')).status,'ok');
  assert.equal(f.adapter.chunkEntryJournal.records.find(row=>row.id==='lost-resume').result.status,'rejected');
  assert.equal((await f.adapter.continueChunkEntry('parent','900000001',{requestId:'new-resume'})).status,'ok');
});

test('A malformed stop revocation cannot release an unknown continuation',async t=>{
  const f=fixture(t);f.adapter.capabilities.push('entry-control-recovery-v1');
  f.reply=(route,payload)=>{
    if(route==='/entry-plan-continue')throw Error('lost continuation reply');
    return {version:39,capabilities:f.adapter.capabilities,entry:{id:payload.entryId,phase:'stopped'},
      entryControlReceipt:{schema:'entry-control-recovery-v1',id:'wrong-id',entryId:payload.entryId,cancelContinueIds:payload.cancelContinueIds}};
  };
  await f.adapter.continueChunkEntry('parent','900000001',{requestId:'unresolved'});
  assert.equal((await f.adapter.stopChunkEntry('parent')).status,'unknown');
  assert.equal(f.adapter.chunkEntryJournal.records.find(row=>row.id==='unresolved').result.status,'unknown');
});

test('A late continuation reply cannot overwrite its durably confirmed revocation',async t=>{
  const f=fixture(t);f.adapter.capabilities.push('entry-control-recovery-v1');let finish;
  f.reply=(route,payload)=>route==='/entry-plan-continue' ? new Promise(resolve=>finish=resolve)
    : {version:39,capabilities:f.adapter.capabilities,entry:{id:payload.entryId,phase:'stopped'},
      entryControlReceipt:{schema:'entry-control-recovery-v1',id:payload.id,entryId:payload.entryId,cancelContinueIds:payload.cancelContinueIds}};
  const continuation=f.adapter.continueChunkEntry('parent','900000001',{requestId:'in-flight'});
  while(!finish)await turn();
  assert.equal((await f.adapter.stopChunkEntry('parent')).status,'ok');
  finish({version:39,capabilities:f.adapter.capabilities,entry:{id:'parent',phase:'running'}});
  assert.equal((await continuation).status,'rejected');
  assert.equal(f.adapter.chunkEntryJournal.records.find(row=>row.id==='in-flight').result.status,'rejected');
});

test('Delivery window starts after durable local preparation and keeps native command TTL separate',async t=>{
  const f=fixture(t);f.adapter.capabilities.push('entry-intent-delivery-v1');
  const original=Date.now;let clock=original();Date.now=()=>clock;
  try {
    await f.adapter.stopChunkEntry('warm-up');
    const journal=f.adapter.chunkEntryJournal,save=journal.save.bind(journal);
    journal.save=async rows=>{if(rows?.some(row=>row.result.status==='unknown'))clock+=3100;return save(rows);};
    f.reply=(_route,payload)=>{
      assert.equal(payload.expiresAt-clock,58500);assert.equal(payload.intentTtlMs,60000);
      return {version:39,capabilities:f.adapter.capabilities,entry:{id:payload.entryId,phase:'stopped'}};
    };
    assert.equal((await f.adapter.stopChunkEntry('after-disk-stall')).status,'ok');
  }finally{Date.now=original;}
});

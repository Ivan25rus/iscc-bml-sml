const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('fs');
const os=require('os');
const path=require('path');
const {MT5VPSAdapter}=require('../payload/src/app/services/mt5-vps/client');
const {CommandJournal}=require('../payload/src/app/services/mt5-vps/commandJournal');
const {buildEntryRequest}=require('../payload/src/app/services/mt5-vps/chunkEntryClient');

const config={enabled:true,account:'900000002',server:'Example-MT5',spxSymbol:'SPX500',ustecSymbol:'USTEC',
  followers:[{account:'900000001',multiplier:.7,enabled:true},{account:'152657',multiplier:1,enabled:false}],
  timeExit:{enabled:true,time:'15:00'}};
const plan={ticker:'USTEC',orderSide:'buy',referencePrice:30002,stopPrice:29990,takeProfitPts:2400,tickSize:.01,
  totalQty:1,chunkVolume:.2,maxGap:2,riskUsd:12.5,level:30000,stopMode:'level',stopOffsetPts:1000};

test('Fill priority resolves per instrument and is captured with a required receiver capability',()=>{
  const {resolveLevelOrderDefaults}=require('../payload/src/app/services/levelOrder/strategy');
  const settings={defaults:{fillPriority:'risk'},symbols:[{ticker:'SPX500',fillPriority:'volume'}]};
  assert.equal(resolveLevelOrderDefaults(settings,'SPX500').fillPriority,'volume');
  assert.equal(resolveLevelOrderDefaults(settings,'USTEC').fillPriority,'risk');
  const adapter={config,capabilities:['independent-account-lanes-v4','atomic-entry-admission-v1'],brokerSymbol:t=>t};
  const base={...plan,level:30002,entryMode:'side-zone',zoneRadius:2,zoneMultiplier:1,executionPolicy:'hybrid-market-limit-v1',chunkPercent:25};
  for(const fillPriority of ['risk','volume']) {
    const request=buildEntryRequest(adapter,{...base,fillPriority});
    assert.equal(request.fillPriority,fillPriority);
    assert.ok(request.requiredCapabilities.includes('entry-fill-priority-v1'));
  }
  assert.throws(()=>buildEntryRequest(adapter,{...base,fillPriority:'bad'}));
});

test('Explicit continuation captures one account and keeps an uncertain reply without fallback',async()=>{
  const {adapter,calls}=fixture();
  adapter.request=async(route,payload,options)=>{
    calls.push({route,payload,options});
    if(route==='/health')return {connected:true,tradingEnabled:true,capabilities:['chunked-breakout','entry-account-continue-v1']};
    throw Error('reply lost');
  };
  const result=await adapter.continueChunkEntry('existing-parent','900000001');
  assert.equal(result.status,'unknown');
  const mutations=calls.filter(row=>row.payload);
  assert.equal(mutations.length,1);assert.equal(mutations[0].route,'/entry-plan-continue');
  assert.equal(mutations[0].payload.entryId,'existing-parent');assert.equal(mutations[0].payload.account,'900000001');
  assert.equal(mutations[0].payload.volume,undefined);
  adapter.close();
});

test('Continuation fails before sending if the receiver lacks its capability',async()=>{
  const {adapter,calls}=fixture();
  assert.equal((await adapter.continueChunkEntry('existing-parent','900000001')).status,'rejected');
  assert.equal(calls.filter(row=>row.payload).length,0);adapter.close();
});

test('Independent account execution is captured only after the receiver advertises its contract',()=>{
  const adapter=new MT5VPSAdapter(config);
  const hybrid={...plan,referencePrice:30000,entryMode:'side-zone',zoneRadius:6,zoneMultiplier:1,
    executionPolicy:'hybrid-market-limit-v1',chunkPercent:20};
  assert.equal(buildEntryRequest(adapter,hybrid).accountExecution,undefined);
  adapter.capabilities=['chunked-breakout','atomic-entry-admission-v1','independent-account-entry-v2'];
  const request=buildEntryRequest(adapter,hybrid);
  assert.equal(request.accountExecution,'independent-v2');
  assert.ok(require('../payload/src/app/services/mt5-vps/entryAdmission').requiredCapabilities(request).includes('independent-account-entry-v2'));
  assert.equal(buildEntryRequest(adapter,plan).accountExecution,undefined);
  adapter.capabilities.push('parallel-account-entry-v3');
  const parallel=buildEntryRequest(adapter,hybrid);
  assert.equal(parallel.accountExecution,'independent-v3');
  assert.ok(require('../payload/src/app/services/mt5-vps/entryAdmission').requiredCapabilities(parallel).includes('parallel-account-entry-v3'));
  adapter.capabilities.push('independent-account-lanes-v4');
  const lanes=buildEntryRequest(adapter,hybrid);
  assert.equal(lanes.accountExecution,'independent-v4');
  assert.ok(require('../payload/src/app/services/mt5-vps/entryAdmission').requiredCapabilities(lanes).includes('independent-account-lanes-v4'));
});

test('Fast entry progress reads do not depend on failed broad management',async()=>{
  const adapter=new MT5VPSAdapter(config),calls=[];
  adapter.capabilities=['atomic-entry-admission-v1','entry-progress-fast-read-v1'];
  adapter.managementState=async()=>{throw Error('Broad management unavailable');};
  adapter.request=async(route,payload)=>{calls.push({route,payload});return {entries:[]};};
  assert.deepEqual(await adapter.listChunkEntries(),[]);
  assert.deepEqual(calls,[{route:'/entry-plans',payload:undefined}]);
  adapter.close();
});
function fixture(extra={}) {
  const adapter=new MT5VPSAdapter({...config,...extra}),calls=[];
  adapter.request=async(route,payload,options)=>{
    calls.push({route,payload,options});
    if (route==='/health') return {account:900000002,server:'Example-MT5',tradingEnabled:true,capabilities:['chunked-breakout']};
    if (route==='/entry-plans') return {entries:[]};
    return {entry:{id:payload.entryId || payload.id,phase:route==='/entry-plan-stop' ? 'stopped' : 'armed'}};
  };
  return {adapter,calls};
}

test('Atomic admission skips health and never falls back or repeats after a lost reply',async()=>{
  for(const lost of [false,true]) {
    const {adapter,calls}=fixture();
    adapter.capabilities=['chunked-breakout','atomic-entry-admission-v1'];
    adapter.request=async(route,payload,options)=>{
      calls.push({route,payload,options});
      if(lost)throw Error('lost response');
      return {version:39,capabilities:adapter.capabilities,admissionContract:'atomic-entry-admission-v1',
        entry:{id:payload.id,phase:'armed'}};
    };
    const result=await adapter.armChunkEntry(plan,{requestId:'atomic-'+lost});
    assert.equal(result.status,lost ? 'unknown' : 'ok');
    assert.equal((await adapter.armChunkEntry(plan,{requestId:'atomic-'+lost})).status,result.status);
    assert.deepEqual(calls.map(row=>row.route),['/entry-plan-admit']);
    assert.equal(calls[0].options.execution,true);
    assert.deepEqual(calls[0].payload.requiredCapabilities,['chunked-breakout']);
  }
});

test('New receivers reuse shared management plans while exact unknown-arm reads stay separate',async()=>{
  const {adapter,calls}=fixture();let reads=0;
  adapter.capabilities=['chunked-breakout','atomic-entry-admission-v1'];
  adapter.managementState=async options=>{
    reads++;assert.equal(options.maxAgeMs,1000);
    adapter.managementStateAt=Date.now();adapter.managementStateView={entryPlans:[]};
    return adapter.managementStateView;
  };
  assert.deepEqual(await adapter.listChunkEntries(),[]);
  assert.equal(reads,1);assert.equal(calls.length,0);
});

test('Additional new-entry risk cap rejects before networking and never enables a risk manager',async()=>{
  const {adapter,calls}=fixture({entryRiskLimitUsd:10});
  assert.equal((await adapter.armChunkEntry(plan)).status,'rejected');
  assert.equal(calls.length,0);
  assert.equal(buildEntryRequest(adapter,{...plan,riskUsd:10}).entryRiskLimitUsd,10);
  adapter.config.entryRiskLimitUsd=0;
  assert.equal(buildEntryRequest(adapter,plan).entryRiskLimitUsd,undefined);
});

test('Chunk entry builder fixes absolute stop, scales captured followers and captures one New York deadline',()=>{
  const {adapter}=fixture();
  const now=Date.parse('2026-10-17T02:00:00Z');
  const request=buildEntryRequest(adapter,plan,{requestId:'entry-1',cardPayload:{cid:'LT-1',token:'validation-only'}},now);
  assert.equal(request.id,'entry-1');assert.equal(request.symbol,'USTEC');assert.equal(request.trigger,30002);
  assert.equal(request.sl,29990);assert.equal(request.tp,30026);assert.equal(request.volume,1);assert.equal(request.chunkVolume,.2);
  assert.equal(request.maxGap,2);assert.equal(request.riskUsd,12.5);assert.equal(request.tickSize,.01);
  assert.deepEqual(request.mirrorAccounts,[{account:'900000001',multiplier:.7}]);
  assert.equal(request.closeAtUtc,Date.parse('2026-10-17T19:00:00Z'));assert.equal(request.expiresAt,now+3000);
  assert.equal(request.cid,'LT-1');assert.equal(request.token,undefined);assert.equal(request.stopMode,'level');
  const sell=buildEntryRequest(adapter,{...plan,ticker:'SPX',orderSide:'sell',stopPrice:30014},{requestId:'entry-2'},now);
  assert.equal(sell.symbol,'SPX500');assert.equal(sell.tp,29978);assert.equal(sell.sl,30014);
});

test('Unbounded plans, invalid stops and nonpositive settings never reach a network request',async()=>{
  const {adapter,calls}=fixture({timeExit:{enabled:false,time:'15:00'}});
  assert.match((await adapter.armChunkEntry(plan)).reason,/включите закрытие по времени/);
  adapter.config.timeExit.enabled=true;
  for (const change of [{chunkVolume:0},{maxGap:-1},{riskUsd:0},{stopPrice:30002},{totalQty:NaN},{tickSize:-1},{orderSide:'other'},{takeProfitPts:-1}])
    assert.equal((await adapter.armChunkEntry({...plan,...change})).status,'rejected');
  assert.equal(calls.length,0);
});

test('Percent metadata preserves the computed lot size and is immutable for a command ID',async()=>{
  const {adapter,calls}=fixture();
  const percentPlan={...plan,chunkPercent:25,chunkVolume:.2};
  const built=buildEntryRequest(adapter,percentPlan,{requestId:'percent-entry'});
  assert.equal(built.chunkPercent,25);assert.equal(built.chunkVolume,.2);
  assert.equal(buildEntryRequest(adapter,plan,{requestId:'legacy-entry'}).chunkPercent,undefined);
  for(const chunkPercent of [0,1.99,100.01,NaN,Infinity])
    assert.equal((await adapter.armChunkEntry({...plan,chunkPercent})).status,'rejected');
  assert.equal(calls.length,0);
  assert.equal((await adapter.armChunkEntry(percentPlan,{requestId:'percent-entry'})).status,'ok');
  assert.equal(calls[1].payload.chunkPercent,25);
  assert.equal((await adapter.armChunkEntry({...percentPlan,chunkPercent:30},{requestId:'percent-entry'})).status,'rejected');
  assert.equal(calls.length,2,'changing the percent must never replay an accepted command');
});

test('Arming uses fresh capability, existing execution transport, and coalesces repeated clicks before health completes',async()=>{
  const {adapter,calls}=fixture();let finish;
  const initial=adapter.request;
  adapter.request=async(...args)=>{if(args[0]==='/health') await new Promise(resolve=>{finish=resolve;});return initial(...args);};
  const first=adapter.armChunkEntry(plan,{requestId:'same-click',clickAt:Date.now()-50});
  const duplicate=adapter.armChunkEntry(plan,{requestId:'same-click'});
  finish();
  const result=await first;
  assert.equal(result.status,'ok');assert.equal((await duplicate).status,'ok');
  assert.deepEqual(calls.map(row=>row.route),['/health','/entry-plan']);assert.equal(calls[1].options.execution,true);
  assert.ok(result.timings.macBeforeSendMs>=50);
  assert.equal((await adapter.armChunkEntry({...plan,maxGap:3},{requestId:'same-click'})).status,'rejected');
  assert.equal(calls.length,2);
});

test('Unknown arm is retained across app restart, never replayed or reconciled through ordinary receipts, and can be stopped',async()=>{
  const folder=fs.mkdtempSync(path.join(os.tmpdir(),'iscc-chunk-entry-'));
  try {
    const file=path.join(folder,'commands.json');
    const {adapter,calls}=fixture();adapter.commandJournal=new CommandJournal(file);
    const original=adapter.request;
    adapter.request=async(...args)=>{if(args[0]==='/entry-plan'){calls.push({route:args[0]});throw Error('lost reply');}return original(...args);};
    const result=await adapter.armChunkEntry(plan,{requestId:'persisted-entry'});
    assert.equal(result.status,'unknown');assert.equal(calls.filter(row=>row.route==='/entry-plan').length,1);
    const record=JSON.parse(fs.readFileSync(file+'.entry-plans.json','utf8'))[0];
    assert.equal(record.id,'persisted-entry');assert.equal(record.result.status,'unknown');
    assert.equal(fs.existsSync(file),false,'ordinary command journal is never written by entry-plan commands');
    const restarted=fixture();restarted.adapter.commandJournal=new CommandJournal(file);
    assert.equal((await restarted.adapter.armChunkEntry(plan,{requestId:'persisted-entry'})).status,'unknown');
    assert.equal(restarted.calls.length,0,'durable unknown is returned without another health, POST or receipt');
    assert.equal((await restarted.adapter.stopChunkEntry('persisted-entry')).status,'ok');
    assert.deepEqual(restarted.calls.map(row=>row.route),['/entry-plan-stop']);
    assert.equal(restarted.calls[0].payload.id,'stop-persisted-entry');
    assert.equal((await restarted.adapter.stopChunkEntry('persisted-entry')).status,'ok');
    assert.equal(restarted.calls.length,1);
  } finally {fs.rmSync(folder,{recursive:true,force:true});}
});

test('Malformed mutation replies stay unknown; read-only list is the explicit persisted-plan recovery route',async()=>{
  const {adapter,calls}=fixture();
  adapter.request=async(route,payload)=>{
    calls.push({route,payload});
    if(route==='/health')return {connected:true,capabilities:['chunked-breakout']};
    if(route==='/entry-plans')return {entries:[{id:'uncertain-entry',phase:'paused',symbol:'USTEC'}]};
    return {entry:{id:'another-entry',phase:'armed'}};
  };
  assert.equal((await adapter.armChunkEntry(plan,{requestId:'uncertain-entry'})).status,'unknown');
  assert.equal((await adapter.armChunkEntry(plan,{requestId:'uncertain-entry'})).status,'unknown');
  assert.equal((await adapter.listChunkEntries())[0].id,'uncertain-entry');
  assert.deepEqual(calls.map(row=>row.route),['/health','/entry-plan','/entry-plans','/entry-plan?id=uncertain-entry']);
  assert.equal(calls[2].payload,undefined);
  adapter.request=async route=>route==='/health' ? {capabilities:['chunked-breakout']}
    : {entry:{id:'bad-phase',phase:'unexpected-phase'}};
  assert.equal((await adapter.armChunkEntry(plan,{requestId:'bad-phase'})).status,'unknown');
  adapter.request=async()=>({entries:[{id:'bad-phase',phase:'unexpected-phase'}]});
  await assert.rejects(()=>adapter.listChunkEntries(),/некорректный список/);
});

test('Primary-only admission, fresh capability and recovery/enrollment locks preserve account safety',async()=>{
  for (const extra of [{routePrefix:'/accounts/900000001'},{}]) {
    const {adapter,calls}=fixture(extra);
    if (!extra.routePrefix) adapter.connectionRestarting=true;
    assert.equal((await adapter.armChunkEntry(plan)).status,'rejected');
    assert.equal((await adapter.stopChunkEntry('entry-1')).status,'rejected');
    assert.equal(calls.length,0);
  }
  const {adapter,calls}=fixture();adapter.capabilities=['chunked-breakout'];adapter.capabilitiesAt=Date.now();
  adapter.request=async route=>{calls.push({route});return {connected:true,capabilities:[]};};
  assert.equal((await adapter.armChunkEntry(plan)).status,'rejected');assert.deepEqual(calls.map(row=>row.route),['/health']);
  adapter.accountEnrollmentBusy=true;
  assert.equal((await adapter.armChunkEntry(plan)).status,'rejected');assert.equal(calls.length,1);
});

test('Unknown stop never repeats automatically; another explicit click uses the same immutable stop ID and can confirm it',async()=>{
  const folder=fs.mkdtempSync(path.join(os.tmpdir(),'iscc-chunk-stop-'));
  try {
    const {adapter,calls}=fixture();const initial=adapter.request;
    const file=path.join(folder,'commands.json');adapter.commandJournal=new CommandJournal(file);
    let sends=0;
    adapter.request=async(...args)=>{
      if(args[0]==='/entry-plan-stop') {
        sends++;
        const intent=JSON.parse(fs.readFileSync(file+'.entry-plans.json','utf8'))[0];
        assert.equal(intent.id,'stop-entry-1');assert.equal(intent.result.status,'unknown');
        if(sends===1){calls.push({route:args[0],payload:args[1]});throw Error('timeout');}
      }
      return initial(...args);
    };
    assert.equal((await adapter.stopChunkEntry('entry-1')).status,'unknown');
    await new Promise(resolve=>setImmediate(resolve));assert.equal(sends,1);
    assert.equal((await adapter.stopChunkEntry('entry-1')).status,'ok');
    assert.equal((await adapter.stopChunkEntry('entry-1')).status,'ok');
    assert.deepEqual(calls.map(row=>row.route),['/entry-plan-stop','/entry-plan-stop']);
    assert.equal(calls[0].payload.entryId,'entry-1');assert.equal(calls[0].payload.id,'stop-entry-1');
    assert.equal(calls[1].payload.id,calls[0].payload.id);
    assert.equal(calls[1].payload.entryId,calls[0].payload.entryId);
    assert.equal(sends,2);
    assert.equal(JSON.parse(fs.readFileSync(file+'.entry-plans.json','utf8'))[0].result.status,'ok');
  } finally {fs.rmSync(folder,{recursive:true,force:true});}
});

test('A restarted client may explicitly stop again, while concurrent stop clicks still share one request',async()=>{
  const folder=fs.mkdtempSync(path.join(os.tmpdir(),'iscc-chunk-stop-restart-'));
  try {
    const file=path.join(folder,'commands.json'),first=fixture();
    first.adapter.commandJournal=new CommandJournal(file);
    first.adapter.request=async route=>{
      if(route==='/health')return {capabilities:['chunked-breakout']};
      throw Error('unknown stop delivery');
    };
    assert.equal((await first.adapter.stopChunkEntry('entry-2')).status,'unknown');
    const restarted=fixture();restarted.adapter.commandJournal=new CommandJournal(file);
    const original=restarted.adapter.request;let finish;
    restarted.adapter.request=async(...args)=>{
      if(args[0]==='/entry-plan-stop')await new Promise(resolve=>{finish=resolve;});
      return original(...args);
    };
    const pending=restarted.adapter.stopChunkEntry('entry-2');
    const duplicate=restarted.adapter.stopChunkEntry('entry-2');
    while(!finish)await new Promise(resolve=>setImmediate(resolve));
    finish();assert.equal((await pending).status,'ok');assert.equal((await duplicate).status,'ok');
    assert.deepEqual(restarted.calls.map(row=>row.route),['/entry-plan-stop']);
    assert.equal(restarted.calls[0].payload.id,'stop-entry-2');
  } finally {fs.rmSync(folder,{recursive:true,force:true});}
});

test('Listing reuses only a fresh management batch and falls back to a read after expiry without mutating plans',async()=>{
  const {adapter,calls}=fixture();
  const entries=[{id:'cached-entry',phase:'running',hasOpenPositions:true,positionsConfirmed:true,
    livePositions:[{account:'900000002',ticket:'71',volume:.2}]}];
  adapter.managementStateView={entryPlans:entries};adapter.managementStateAt=Date.now();
  assert.deepEqual(await adapter.listChunkEntries(),entries);assert.equal(calls.length,0);
  adapter.managementStateAt=Date.now()-1001;
  const missing=await adapter.listChunkEntries();assert.equal(missing.length,1);assert.equal(missing[0].id,'cached-entry');
  assert.match(missing[0]._readError,/проверяем/);assert.notEqual(missing[0].positionLifecycle,'closed');
  assert.deepEqual(calls.map(row=>row.route),['/entry-plans']);
  assert.equal(calls[0].payload,undefined);assert.deepEqual(adapter.managementStateView.entryPlans,entries);
  adapter.managementStateAt=Date.now()+1000;
  await adapter.listChunkEntries();assert.equal(calls.length,2,'future timestamps are never treated as fresh');
});

test('An authenticated explicit server rejection is durable rejected, and never automatically rearms',async()=>{
  const folder=fs.mkdtempSync(path.join(os.tmpdir(),'iscc-chunk-rejected-'));
  try {
    const file=path.join(folder,'commands.json'),{adapter,calls}=fixture();
    adapter.commandJournal=new CommandJournal(file);
    adapter.request=async(route,payload)=>{
      calls.push({route,payload});
      if(route==='/health')return {capabilities:['chunked-breakout']};
      return {entryResult:{status:'rejected',reason:'Broker quote is stale'}};
    };
    const result=await adapter.armChunkEntry(plan,{requestId:'rejected-entry'});
    assert.equal(result.status,'rejected');assert.equal(result.reason,'Broker quote is stale');
    assert.equal((await adapter.armChunkEntry(plan,{requestId:'rejected-entry'})).status,'rejected');
    assert.deepEqual(calls.map(row=>row.route),['/health','/entry-plan']);
    assert.equal(JSON.parse(fs.readFileSync(file+'.entry-plans.json','utf8'))[0].result.status,'rejected');
    const restarted=fixture();restarted.adapter.commandJournal=new CommandJournal(file);
    assert.equal((await restarted.adapter.armChunkEntry(plan,{requestId:'rejected-entry'})).status,'rejected');
    assert.equal(restarted.calls.length,0);
  } finally {fs.rmSync(folder,{recursive:true,force:true});}
});

test('Malformed or ambiguous entryResult replies remain unknown, never confirmed rejections',async()=>{
  for (const response of [{entryResult:{status:'rejected'}},{entryResult:{status:'ok',reason:'invalid'}},
    {entryResult:{status:'rejected',reason:' ' }},{entryResult:{status:'rejected',reason:'bad'},entry:{id:'bad'}}]) {
    const {adapter}=fixture();
    adapter.request=async route=>route==='/health' ? {capabilities:['chunked-breakout']} : response;
    assert.equal((await adapter.armChunkEntry(plan,{requestId:'malformed-entry'})).status,'unknown');
  }
});

test('An explicitly rejected stop can be retried only by another user invocation with the same ID and a renewed deadline',async()=>{
  const folder=fs.mkdtempSync(path.join(os.tmpdir(),'iscc-chunk-stop-rejected-'));
  const savedNow=Date.now;
  try {
    const file=path.join(folder,'commands.json'),{adapter,calls}=fixture();
    adapter.commandJournal=new CommandJournal(file);let sends=0,now=100000;
    Date.now=()=>now;
    adapter.request=async(route,payload)=>{
      calls.push({route,payload});
      if(route==='/health')return {capabilities:['chunked-breakout']};
      sends++;
      assert.equal(JSON.parse(fs.readFileSync(file+'.entry-plans.json','utf8'))[0].result.status,'unknown');
      return sends===1 ? {entryResult:{status:'rejected',reason:'Command expired'}}
        : {entry:{id:payload.entryId,phase:'stopped',accounts:{},children:[]}};
    };
    assert.equal((await adapter.stopChunkEntry('late-entry')).status,'rejected');
    assert.equal(sends,1);now+=5000;
    assert.equal((await adapter.stopChunkEntry('late-entry')).status,'ok');
    assert.equal(sends,2);assert.equal(calls[0].payload.id,calls[1].payload.id);
    assert.equal(calls[0].payload.entryId,calls[1].payload.entryId);
    assert.equal(calls[1].payload.expiresAt-calls[0].payload.expiresAt,5000);
  } finally {Date.now=savedNow;fs.rmSync(folder,{recursive:true,force:true});}
});

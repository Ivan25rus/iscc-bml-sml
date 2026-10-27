const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('fs');
const os=require('os');
const path=require('path');
const {MT5VPSAdapter}=require('../payload/src/app/services/mt5-vps/client');
const {CommandJournal}=require('../payload/src/app/services/mt5-vps/commandJournal');
const {ENTRY_PROTECTION_CAPABILITY:cap,isEntryPlanRow,entryPlanMetadata,entryProtectionBlock,protectionChanges,protectionView,managementBlock}=require('../payload/src/app/services/mt5-vps/managementPolicy');
const primary='900000002',follower='900000001',server='Example-MT5',policy='hybrid-market-limit-v1';
const copy=value=>JSON.parse(JSON.stringify(value));
function fixture(t,{journalFile}={}) {
  const folder=fs.mkdtempSync(path.join(os.tmpdir(),'iscc-parent-protection-'));
  t.after(()=>fs.rmSync(folder,{recursive:true,force:true}));
  const config={account:primary,server,enabled:true,spxSymbol:'SPX500',ustecSymbol:'USTEC',followers:[{account:follower,multiplier:3,enabled:false}]};
  const adapter=new MT5VPSAdapter(config);
  t.after(()=>adapter.close());
  adapter.commandJournal=new CommandJournal(journalFile || path.join(folder,'commands.json'));
  const plan={id:'entry-1',executionPolicy:policy,side:'buy',accounts:[primary,follower],phase:'running',averagePrice:25003,sl:24987,tp:25060,protectionReady:true};
  const row={ticket:'71',symbol:'USTEC',type:'buy',__isPosition:true,open_price:25000,lots:.3,sl:24984,tp:25050,entryPlan:plan};
  const remote={...copy(row),ticket:'81',entryPlan:{...copy(plan),averagePrice:25004,sl:24988}};
  const view=(account,rows)=>({account,server,connected:true,capabilities:[cap,'modify','close','partial-close'],orders:rows});
  const parent={id:plan.id,executionPolicy:policy,symbol:'USTEC',side:'buy',phase:'running',accounts:{[primary]:{},[follower]:{}},children:[{state:'confirmed'}],positionsConfirmed:true,hasOpenPositions:true};
  const state={...view(primary,[copy(row)]),accountStates:{[primary]:view(primary,[copy(row)]),[follower]:view(follower,[remote])},entryPlans:[parent]};
  const calls=[],receipts={};
  let reply;
  adapter.request=async(route,payload,options)=>{
    calls.push({route,payload:payload && copy(payload),options});
    if(route==='/management'){assert.equal(payload,undefined);return copy(state);}
    if(route.startsWith('/receipt?id=')){assert.equal(payload,undefined);return {result:receipts[decodeURIComponent(route.split('=')[1])] || null};}
    assert.ok(['/entry-plan-protection','/entry-plan-close','/modify'].includes(route));
    if(reply)return reply(route,payload,options);
    const accounts=route==='/entry-plan-close' ? Object.keys(parent.accounts) : [payload.account,...(payload.mirrorAccounts || []).map(row=>row.account)];
    return {result:{id:payload.id,entryId:payload.entryId,status:'ok',accountResults:Object.fromEntries(accounts.map(account=>[account,{status:'ok',averagePrice:25005}]))},
      entry:{...copy(parent),hasOpenPositions:false,positionsConfirmed:true,positionLifecycle:'closed'}};
  };
  return {adapter,config,row,plan,parent,state,calls,receipts,setReply:value=>{reply=value;},posts:()=>calls.filter(call=>call.payload)};
}
test('Close skips a broken unrelated receipt and retains exact pre-send timing',async t=>{
  const f=fixture(t),journal=f.adapter.commandJournal;
  journal.records.push({id:'old-unrelated',ticket:'entry:old',operation:'entry-close',payload:{entryId:'old'},result:{status:'unknown'}});
  const request=f.adapter.request;
  f.adapter.request=async(route,...args)=>{
    if(route.startsWith('/receipt?id='))throw Error('Unrelated receipt must not be requested');
    return request(route,...args);
  };
  const result=await f.adapter.closeEntryPosition({id:f.plan.id,symbol:'USTEC'});
  assert.equal(result.status,'ok');assert.equal(f.posts().length,1);
  assert.equal(f.calls[0].route,'/management');
  assert.equal(journal.records[0].result.status,'unknown');
  for(const key of ['managementReadMs','receiptReconciliationMs','journalBeforeSendMs','totalMs']) {
    assert.ok(Number.isFinite(result.timings.macPreparation[key]),key);
    assert.ok(result.timings.macPreparation[key]>=0,key);
  }
  assert.ok(result.timings.macPreparation.totalMs>=result.timings.macPreparation.managementReadMs);
});
test('Whole-parent protection uses captured accounts and fresh shared levels; no ticket, size, or current config multiplier reaches VPS',async t=>{
  const f=fixture(t);
  const result=await f.adapter.modifyEntryProtection(f.row,{sl:'24990'},{account:primary});
  assert.equal(result.status,'ok');
  const [post]=f.posts();
  assert.equal(post.route,'/entry-plan-protection');assert.equal(post.options.execution,true);
  assert.deepEqual({...post.payload,id:'id',expiresAt:1},{id:'id',expiresAt:1,entryId:'entry-1',symbol:'USTEC',operation:'modify',account:primary,sl:24990,mirrorAccounts:[{account:follower}]});
  assert.equal(f.calls[0].route,'/management');
  const saved=JSON.parse(fs.readFileSync(f.adapter.commandJournal.file));
  assert.equal(saved[0].ticket,'entry:entry-1');assert.equal(saved[0].operation,'entry-protection');
  assert.equal(saved[0].result.status,'ok');assert.equal(f.adapter.managementSnapshot,null);
});
test('A separately selected follower uses primary transport and server weighted breakeven without a client-calculated SL',async t=>{
  const f=fixture(t),target={...copy(f.state.accountStates[follower].orders[0]),singleAccount:true};
  const result=await f.adapter.modifyEntryProtection(target,{tp:'25080'},{account:follower,operation:'breakeven'});
  assert.equal(result.status,'ok');
  const request=f.posts()[0].payload;
  assert.equal(request.account,follower);assert.equal(request.operation,'breakeven');assert.equal(request.tp,25080);
  for(const key of ['sl','ticket','mirrorAccounts','averagePrice'])assert.equal(Object.hasOwn(request,key),false,key);
  assert.ok(f.calls.every(call=>!call.route.startsWith('/accounts/')));
});
test('Follower plus mirror includes the primary only once and uses no accounts outside the captured parent',async t=>{
  const f=fixture(t),target=copy(f.state.accountStates[follower].orders[0]);
  assert.equal((await f.adapter.modifyEntryProtection(target,{tp:25080},{account:follower})).status,'ok');
  assert.deepEqual(f.posts()[0].payload.mirrorAccounts,[{account:primary}]);
});
test('Missing capability or disconnected captured account blocks the entire mirrored protection before POST',async t=>{
  for(const [account,key,value] of [[primary,'capabilities',['modify']],[follower,'capabilities',['modify']],[follower,'connected',false]]) {
    const f=fixture(t);f.state.accountStates[account][key]=value;
    assert.equal((await f.adapter.modifyEntryProtection(f.row,{sl:24990})).status,'rejected');assert.equal(f.posts().length,0);
  }
  const f=fixture(t);f.state.accountStates[follower].connected=false;
  assert.equal((await f.adapter.modifyEntryProtection({...f.row,singleAccount:true},{sl:24990})).status,'ok','Explicit individual protection needs only the selected account');
});
test('Missing, malformed, changed and unready ownership never falls back to a child modify',async t=>{
  const mutations=[row=>delete row.entryPlan,row=>row.entryPlan=null,row=>row.entryPlan.id='other',row=>row.entryPlan.accounts=[primary],row=>row.entryPlan.accounts=[primary,primary],row=>row.entryPlan.side='sell',row=>row.entryPlan.executionPolicy='old',row=>row.entryPlan.protectionReady=false,row=>row.entryPlan.averagePrice=0];
  for(const mutate of mutations) {
    const f=fixture(t);mutate(f.state.accountStates[primary].orders[0]);
    assert.equal((await f.adapter.modifyEntryProtection(f.row,{sl:24990})).status,'rejected');assert.equal(f.posts().length,0);
  }
  const f=fixture(t);f.adapter.config.routePrefix=`/accounts/${follower}`;
  assert.equal((await f.adapter.modifyEntryProtection(f.row,{sl:24990})).status,'rejected');assert.equal(f.calls.length,0);
});
test('Invalid protection fields and breakeven SL are rejected without transmitting a partial command',async t=>{
  for(const [changes,operation] of [[{sl:0},'modify'],[{sl:''},'modify'],[{sl:' '},'modify'],[{sl:null},'modify'],[{sl:true},'modify'],[{sl:NaN},'modify'],[{sl:-1},'modify'],[{price:25000},'modify'],[{sl:25003},'breakeven'],[{},'modify'],[{tp:Infinity},'modify']]) {
    const f=fixture(t);assert.equal((await f.adapter.modifyEntryProtection(f.row,changes,{operation})).status,'rejected');assert.equal(f.posts().length,0);
  }
  const f=fixture(t);assert.equal((await f.adapter.modifyEntryProtection(f.row,{tp:0})).status,'ok');assert.equal(f.posts()[0].payload.tp,0);
});
test('Unknown whole-parent results block protection and close across child tickets and process restarts even with a known primary outcome',async t=>{
  const f=fixture(t);
  f.setReply(async(_route,payload)=>({result:{id:payload.id,entryId:payload.entryId,status:'unknown',primaryAccount:primary,accountResults:{[primary]:{status:'ok'},[follower]:{status:'unknown'}}}}));
  assert.equal((await f.adapter.modifyEntryProtection(f.row,{sl:24990})).status,'unknown');
  assert.equal((await f.adapter.closeEntryPosition({id:f.plan.id,symbol:'USTEC'})).status,'unknown');
  f.adapter.commandJournal=new CommandJournal(f.adapter.commandJournal.file);
  assert.equal((await f.adapter.modifyEntryProtection(f.row,{tp:25090})).status,'unknown');
  assert.equal(f.posts().length,1);
  assert.ok(f.calls.some(call=>call.route.startsWith('/receipt?id=')));
});
test('Fresh primary receipt reconciliation releases a known parent outcome without replaying the original mutation',async t=>{
  const f=fixture(t);let original;
  f.setReply(async(_route,payload)=>{original=payload;throw Error('Reply lost');});
  assert.equal((await f.adapter.modifyEntryProtection(f.row,{sl:24990})).status,'unknown');
  f.receipts[original.id]={id:original.id,entryId:f.plan.id,status:'ok',accountResults:{[primary]:{status:'ok'},[follower]:{status:'ok'}}};
  f.setReply(null);
  assert.equal((await f.adapter.modifyEntryProtection(f.row,{tp:25080})).status,'ok');
  assert.equal(f.posts().length,2);assert.notEqual(f.posts()[1].payload.id,original.id);
  assert.equal(f.posts()[1].payload.sl,undefined);
});
test('Malformed late parent receipts cannot release uncertainty or authorize a new protection or close',async t=>{
  for(const operation of ['entry-protection','entry-close']) {
    const f=fixture(t);let original;
    f.setReply(async(_route,payload)=>{original=payload;throw Error('Reply lost');});
    const invoke=()=>operation==='entry-close' ? f.adapter.closeEntryPosition({id:f.plan.id,symbol:'USTEC'}) : f.adapter.modifyEntryProtection(f.row,{sl:24990});
    assert.equal((await invoke()).status,'unknown');
    const valid={id:original.id,entryId:f.plan.id,status:'ok',accountResults:{[primary]:{status:'ok'},[follower]:{status:'ok'}}};
    for(const receipt of [{...valid,id:'other'},{...valid,entryId:'other'},
      {...valid,accountResults:{[primary]:{status:'ok'}}},
      {...valid,accountResults:{[primary]:{status:'ok'},[follower]:{status:'unknown'}}},
      {...valid,accountResults:{[primary]:{status:'ok'},'999999':{status:'ok'}}}]) {
      f.receipts[original.id]=receipt;
      assert.equal((await invoke()).status,'unknown');assert.equal(f.posts().length,1);
      assert.equal(f.adapter.commandJournal.records[0].result.status,'unknown');
    }
    assert.equal(f.adapter.validateCommandReceipt(original.id,valid),valid);
    assert.equal(f.adapter.validateCommandReceipt('ordinary-id',{status:'ok'}).status,'ok');
    assert.equal(f.adapter.validateCommandReceipt(original.id,{id:original.id,entryId:f.plan.id,status:'rejected'}).status,'rejected');
  }
});
test('Concurrent protection and close for different children share one in-flight parent lock; rejected duplicates never release its owner',async t=>{
  const f=fixture(t);let release;
  f.setReply((_route,payload)=>new Promise(resolve=>release=()=>resolve({result:{id:payload.id,entryId:payload.entryId,status:'ok',accountResults:{[primary]:{status:'ok'},[follower]:{status:'ok'}}}})));
  const first=f.adapter.modifyEntryProtection(f.row,{sl:24990});
  while(!release)await new Promise(resolve=>setImmediate(resolve));
  assert.equal((await f.adapter.modifyEntryProtection({...f.row,ticket:'72'},{tp:25080})).status,'unknown');
  assert.equal((await f.adapter.closeEntryPosition({id:f.plan.id,symbol:'USTEC'})).status,'unknown');
  assert.equal((await f.adapter.modifyEntryProtection(f.row,{sl:24991})).status,'unknown');
  assert.equal(f.posts().length,1);assert.equal(f.adapter.inFlight.has('entry:entry-1'),true);
  release();assert.equal((await first).status,'ok');assert.equal(f.adapter.inFlight.size,0);
});
test('Whole-parent intent is durable before transmission, and incomplete or wrong positive acknowledgements remain unknown without retry',async t=>{
  for(const kind of ['missing-follower','wrong-entry','wrong-id']) {
    const f=fixture(t);
    f.setReply(async(_route,payload)=>{
      const [saved]=JSON.parse(fs.readFileSync(f.adapter.commandJournal.file));assert.equal(saved.result.status,'unknown');assert.equal(saved.payload.entryId,f.plan.id);
      return {result:{id:kind==='wrong-id'?'other':payload.id,entryId:kind==='wrong-entry'?'other':payload.entryId,status:'ok',accountResults:{[primary]:{status:'ok'}}}};
    });
    assert.equal((await f.adapter.modifyEntryProtection(f.row,{sl:24990})).status,'unknown');
    assert.equal((await f.adapter.modifyEntryProtection(f.row,{sl:24991})).status,'unknown');assert.equal(f.posts().length,1);
  }
});
test('A missing or unwritable durable journal cannot send a parent mutation',async t=>{
  const f=fixture(t);f.adapter.commandJournal=null;
  assert.equal((await f.adapter.modifyEntryProtection(f.row,{sl:24990})).status,'rejected');assert.equal(f.posts().length,0);
  const g=fixture(t);g.adapter.commandJournal.save=()=>{throw Error('disk unavailable');};
  assert.equal((await g.adapter.modifyEntryProtection(g.row,{sl:24990})).status,'unknown');assert.equal(g.posts().length,0);
});
test('Unflagged stale UI data never silently modifies a newly discovered hybrid child; standard ticket modifications retain their payload',async t=>{
  const f=fixture(t),stale={...f.row};delete stale.entryPlan;
  assert.equal((await f.adapter.modifyOrder(stale,{sl:24990})).status,'rejected');assert.equal(f.posts().length,0);
  delete f.state.accountStates[primary].orders[0].entryPlan;f.adapter.managementSnapshot=null;f.adapter.managementStateView=null;
  assert.equal((await f.adapter.modifyOrder(stale,{sl:24990})).status,'ok');
  assert.equal(f.posts()[0].route,'/modify');assert.equal(f.posts()[0].payload.ticket,'71');assert.equal(f.posts()[0].payload.sl,24990);
});
test('Whole-parent close uses fresh captured accounts, ignores ticket/checkbox sizing, and returns only the matching confirmed parent snapshot',async t=>{
  const f=fixture(t);
  const result=await f.adapter.closeEntryPosition({id:f.plan.id,symbol:'USTEC',singleAccount:true,accounts:{[primary]:{}}});
  assert.equal(result.status,'ok');assert.equal(result.entry.id,f.plan.id);assert.equal(result.entry.hasOpenPositions,false);
  const post=f.posts()[0];assert.equal(post.route,'/entry-plan-close');
  assert.deepEqual({...post.payload,id:'id',expiresAt:1},{id:'id',expiresAt:1,entryId:f.plan.id,symbol:'USTEC'});
  assert.equal(f.adapter.commandJournal.records[0].ticket,'entry:entry-1');assert.equal(f.adapter.commandJournal.records[0].operation,'entry-close');
  assert.deepEqual(f.adapter.commandJournal.records[0].payload.capturedAccounts,[follower,primary]);
});
test('Whole-parent close requires exact known ownership and every captured account before sending',async t=>{
  const mutations=[f=>f.state.entryPlans=[],f=>f.parent.symbol='SPX500',f=>f.parent.executionPolicy='legacy',f=>f.parent.phase='unknown',f=>f.parent.positionsConfirmed=false,f=>f.parent.hasOpenPositions=false,f=>f.parent.children=[{state:'unknown'}],f=>f.parent.protectionPending='pending',f=>f.state.accountStates[follower].connected=false,f=>f.state.accountStates[follower].capabilities=[]];
  for(const mutate of mutations) {
    const f=fixture(t);mutate(f);assert.equal((await f.adapter.closeEntryPosition({id:f.plan.id,symbol:'USTEC'})).status,'rejected');assert.equal(f.posts().length,0);
  }
});
test('Unknown close blocks new protection; a wrong returned parent can never optimistically release its card',async t=>{
  const f=fixture(t);f.setReply(async(_route,payload)=>({result:{id:payload.id,entryId:payload.entryId,status:'unknown'}}));
  assert.equal((await f.adapter.closeEntryPosition({id:f.plan.id,symbol:'USTEC'})).status,'unknown');
  assert.equal((await f.adapter.modifyEntryProtection(f.row,{sl:24990})).status,'unknown');assert.equal(f.posts().length,1);
  const g=fixture(t);g.setReply(async(_route,payload)=>({result:{id:payload.id,entryId:payload.entryId,status:'ok',accountResults:{[primary]:{status:'ok'},[follower]:{status:'ok'}}},entry:{...copy(g.parent),id:'other',hasOpenPositions:false}}));
  assert.equal((await g.adapter.closeEntryPosition({id:g.plan.id,symbol:'USTEC'})).entry,undefined);
});
test('Policy previews whole weighted average and shared levels while retaining dirty-field and explicit zero TP intent',t=>{
  const f=fixture(t);assert.equal(protectionView(f.row).open_price,25003);assert.equal(protectionView(f.row).sl,24987);
  assert.deepEqual(protectionChanges(f.row,{sl:'24987',tp:'25060',price:'9'},new Set(['sl','tp','price'])),{});
  assert.deepEqual(protectionChanges(f.row,{tp:'0'},new Set(['tp'])),{tp:'0'});
  assert.equal(isEntryPlanRow({...f.row,entryPlan:null}),true);assert.throws(()=>entryPlanMetadata({...f.row,entryPlan:null}));
});
test('UI protection policy requires whole-parent readiness and blocks unknown parent receipts on every child/account',t=>{
  const f=fixture(t),options={row:f.row,account:primary,linked:true,capabilities:[cap],accountStates:f.state.accountStates};
  assert.equal(entryProtectionBlock(options),'');
  assert.ok(entryProtectionBlock({...options,commands:[{ticket:'entry:entry-1',result:{status:'unknown',primaryAccount:primary,accountResults:{[primary]:{status:'ok'}}}}]}));
  assert.ok(managementBlock({...options,row:{...f.row,ticket:'72'},linked:false,commands:[{ticket:'entry:entry-1',result:{status:'unknown'}}]}));
  f.state.accountStates[follower].connected=false;
  assert.ok(entryProtectionBlock(options));assert.equal(entryProtectionBlock({...options,linked:false}),'');
  assert.ok(entryProtectionBlock({...options,row:{...f.row,entryPlan:{...f.plan,protectionReady:false}}}));
  assert.equal(entryProtectionBlock({...options,row:{ticket:'ordinary'}}),'');
});

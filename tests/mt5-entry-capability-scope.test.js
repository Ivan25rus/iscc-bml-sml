const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const {MT5VPSAdapter}=require('../payload/src/app/services/mt5-vps/client');
const {buildEntryRequest}=require('../payload/src/app/services/mt5-vps/chunkEntryClient');
const config={account:'900000002',server:'Example-MT5',spxSymbol:'SPX500',ustecSymbol:'USTEC',timeExit:{enabled:true,time:'15:00'}};
const native=['chunked-breakout','atomic-entry-admission-v1','independent-account-lanes-v4'];
const coordinator=[...native,'independent-account-engines-v5','entry-initial-sizing-only-v1','minute-chart-bars-v1','entry-exact-parent-protection-v1','management-active-v1'];
const account=id=>({account:String(id),server:config.server,connected:true,evidenceReady:true,checkedAt:Date.now(),orders:[],capabilities:[...native]});
const management=()=>({...config,capabilities:[...coordinator],accountStates:{900000002:account(900000002),900000001:account(900000001)}});
test('Account settings retain coordinator enrollment and balance risk capabilities',async()=>{
  const state=management();state.capabilities.push('account-enrollment','entry-balance-risk-v1');
  const primary={remoteAccounts:['900000001','900000003'],managementState:async()=>state};
  const source=fs.readFileSync(require.resolve('../reference/mt5-service-manifest.js.txt'),'utf8');
  const start=source.indexOf("  ipcMain.handle('mt5-vps:trade-settings'"),end=source.indexOf("  ipcMain.handle('mt5-vps:orders'",start);
  let handler;
  const scope={ipcMain:{handle:(_name,fn)=>handler=fn},api:{mt5VPS:{getAdapter:()=>primary}},readConfig:()=>config,
    acceptManagement(){},followers:()=>[],require:name=>{assert.equal(name,'./timeExit');return {timeExitConfig:()=>({})};}};
  vm.createContext(scope);vm.runInContext(source.slice(start,end),scope);
  const result=await handler();assert.equal(result.status,'ok');
  assert.ok(result.capabilities.includes('account-enrollment'));assert.ok(result.capabilities.includes('entry-balance-risk-v1'));
  assert.equal(result.accounts.at(-1),'900000003');
});
function adapter(t,options={}){const a=new MT5VPSAdapter({...config,...options});t.after(()=>a.close());return a;}

for(const watch of [undefined,[{account:'900000002',ticket:'9',symbol:'USTEC'}]])test('Management preserves coordinator entry protocol'+(watch?' with exact watch':''),async t=>{
  const a=adapter(t),calls=[];a.capabilities=[...coordinator];
  a.request=async(route,payload)=>{calls.push({route,payload});return management();};
  await a.managementState({maxAgeMs:0,watch});
  assert.deepEqual(a.capabilities,coordinator);assert.equal(calls.length,1);assert.equal(calls[0].payload,undefined);
  for(const ticker of ['USTEC','SPX500'])for(const side of ['buy','sell']) {
    const direction=side==='buy'?1:-1;
    const plan={ticker,orderSide:side,referencePrice:30000,level:30000,stopPrice:30000-direction*16,
      tickSize:.01,stopMode:'entry',stopOffsetPts:1600,totalQty:.8,chunkVolume:.2,riskUsd:13,
      maxGap:2,entryMode:'side-zone',zoneRadius:2,zoneMultiplier:1,executionPolicy:'hybrid-market-limit-v1',chunkPercent:25};
    assert.equal(buildEntryRequest(a,plan).accountExecution,'independent-v5');
  }
});

test('The real background manifest keeps root capabilities separate from follower worker capabilities',t=>{
  const primary=adapter(t),follower=adapter(t,{account:'900000001',routePrefix:'/accounts/900000001'});
  const source=fs.readFileSync(require.resolve('../reference/mt5-service-manifest.js.txt'),'utf8');
  const start=source.indexOf('  function acceptManagement(state) {'),end=source.indexOf('  const warmthTimer',start);
  const scope={api:{mt5VPS:{getAdapter:()=>primary}},forAccount:id=>String(id)==='900000002'?primary:follower};
  vm.createContext(scope);vm.runInContext(source.slice(start,end),scope);
  scope.acceptManagement(management());
  assert.deepEqual(primary.capabilities,coordinator);assert.deepEqual(follower.capabilities,native);
  assert.ok(primary.managementSnapshot);assert.ok(follower.managementSnapshot);
});

test('A real endpoint downgrade replaces the full list rather than unioning stale capabilities',t=>{
  const a=adapter(t);a.capabilities=[...coordinator];
  a.acceptManagementState(account(900000002),{...config,capabilities:[...native]});
  assert.deepEqual(a.capabilities,native);assert.equal(a.capabilities.includes('independent-account-engines-v5'),false);
});

test('A foreign capability envelope cannot update snapshots or entry routing',t=>{
  const a=adapter(t);a.capabilities=[...coordinator];
  assert.throws(()=>a.acceptManagementState(account(900000002),{...config,account:'900000001',capabilities:native}),/источник возможностей/);
  assert.equal(a.managementSnapshot,null);assert.deepEqual(a.capabilities,coordinator);
});

test('An unready exact account proof remains rejected without weakening freshness checks',t=>{
  const a=adapter(t);a.capabilities=[...coordinator];
  assert.throws(()=>a.acceptManagementState({...account(900000002),evidenceReady:false},management()),/Проверяем состояние/);
  assert.equal(a.managementSnapshot,null);assert.deepEqual(a.capabilities,coordinator);
});

for(const selected of ['900000002','900000001'])test('Positions UI receives root chart/protection capabilities for account '+selected,async()=>{
  const state=management();state.receipts={};
  const journal={records:[],reconcile:async()=>{}};
  let reads=0;
  const primary={config,remoteAccounts:['900000001'],commandJournal:journal,managementState:async()=>{
    if(++reads===1){const error=Error('Native view pending');error.code='MT5_VIEW_PENDING';throw error;}return state;
  }};
  const connection={config:{...config,account:selected},commandJournal:journal,maintainExecutionWarmth:async()=>{}};
  const source=fs.readFileSync(require.resolve('../reference/mt5-service-manifest.js.txt'),'utf8');
  const start=source.indexOf("  ipcMain.handle('mt5-vps:orders'"),end=source.indexOf("  ipcMain.handle('mt5-vps:minute-bars'",start);
  let handler;
  const scope={ipcMain:{handle:(_name,fn)=>handler=fn},forAccount:()=>connection,
    api:{mt5VPS:{getAdapter:()=>primary}},acceptManagement(){},readConfig:()=>config,followers:()=>[],
    require:name=>{assert.equal(name,'./timeExit');return {timeExitConfig:()=>({})};},Date,Map,setTimeout};
  vm.createContext(scope);vm.runInContext(source.slice(start,end),scope);
  const value=await handler(null,selected);
  assert.equal(value.status,'ok');assert.equal(String(value.account),selected);
  assert.deepEqual(value.capabilities,coordinator);
  assert.ok(value.capabilities.includes('minute-chart-bars-v1'));
  assert.ok(value.capabilities.includes('entry-exact-parent-protection-v1'));
  assert.equal(value.accountStates[selected].capabilities.includes('minute-chart-bars-v1'),false);
  assert.equal(reads,2,'One bounded UI handoff consumes the already-started native read');
});

for(const scenario of ['pending','network','unknown-receipt'])test('UI ready handoff stays bounded for '+scenario,async()=>{
  let calls=0,handler;
  const read=async()=>{calls++;const error=Error('Fixture '+scenario);if(scenario!=='network')error.code='MT5_VIEW_PENDING';throw error;};
  const connection={config,commandJournal:{records:scenario==='unknown-receipt'?[{id:'uncertain',result:{status:'unknown'}}]:[]}};
  const primary={managementState:read,request:read};
  const source=fs.readFileSync(require.resolve('../reference/mt5-service-manifest.js.txt'),'utf8');
  const start=source.indexOf("  ipcMain.handle('mt5-vps:orders'"),end=source.indexOf("  ipcMain.handle('mt5-vps:minute-bars'",start);
  const scope={ipcMain:{handle:(_name,fn)=>handler=fn},forAccount:()=>connection,api:{mt5VPS:{getAdapter:()=>primary}},setTimeout};
  vm.createContext(scope);vm.runInContext(source.slice(start,end),scope);
  const value=await handler(null,'900000002');assert.equal(value.status,'rejected');assert.equal(calls,scenario==='pending'?2:1);
});

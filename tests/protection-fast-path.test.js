const test=require('node:test'),assert=require('node:assert/strict');
const fs=require('fs'),os=require('os'),path=require('path');
const {createHash}=require('crypto');
const {MT5VPSAdapter}=require('../payload/src/app/services/mt5-vps/client');
const {CommandJournal}=require('../payload/src/app/services/mt5-vps/commandJournal');
const {protectionReceiptView}=require('../payload/src/app/services/mt5-vps/protectionReceiptView');
const {readErrorLabel}=require('../payload/src/app/services/mt5-vps/entryStatus');
const account='900000002',server='test',peer='900000001';
const digest=id=>createHash('sha256').update(`${id}:${account}`).digest('hex');
const receiptContext=target=>({target,account,server});
function nativeOutcome(id,positions,independent=false) {
  const localId=independent ? 'account-protection:'+digest(id) : id;
  return {status:'ok',averagePrice:25000,positions,...(independent ? {controlId:localId} : {}),
    protectionProof:{schema:'entry-protection-proof-v1',commandId:'parent-protection:'+digest(localId),
      account,server,symbol:'USTEC',checkedAt:Date.now(),positions}};
}
function fixture(t){
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),'iscc-protection-fast-'));t.after(()=>fs.rmSync(dir,{recursive:true,force:true}));
  const adapter=new MT5VPSAdapter({account:900000002,server:'test',spxSymbol:'SPX500',ustecSymbol:'USTEC'});t.after(()=>adapter.close());
  adapter.commandJournal=new CommandJournal(path.join(dir,'commands.json'));
  const caps=['hybrid-position-protection-v1','entry-account-controls-v1','entry-exact-parent-protection-v1'];
  adapter.capabilities=caps;
  const row={ticket:'71',symbol:'USTEC',type:'buy',__isPosition:true,lots:.8,open_price:25000,sl:24984,tp:25102,
    entryPlan:{id:'parent',side:'buy',executionPolicy:'hybrid-market-limit-v1',accounts:['900000002'],phase:'completed',
      sl:24984,tp:25102,averagePrice:25000,protectionReady:true,independentAccountControls:true}};
  adapter.managementStateView={accountStates:{'900000002':{account:'900000002',server:'test',connected:true,evidenceReady:true,capabilities:caps,orders:[row]}}};
  const calls=[];
  adapter.request=async(route,payload)=>{
    calls.push({route,payload});assert.equal(route,'/entry-plan-protection');
    assert.equal(JSON.parse(fs.readFileSync(adapter.commandJournal.file)).at(-1).result.status,'unknown');
    return {result:{id:payload.id,entryId:'parent',status:'ok',accountResults:{'900000002':{status:'ok'}}}};
  };
  return {adapter,row,calls};
}
test('Exact protection sends one durable intent with captured ownership and performs no full-list GET',async t=>{
  const f=fixture(t);const result=await f.adapter.modifyEntryProtection(f.row,{tp:25110});
  assert.equal(result.status,'ok',result.reason);assert.equal(f.calls.length,1);
  assert.deepEqual(f.calls[0].payload.capturedAccounts,['900000002']);
  assert.deepEqual(f.calls[0].payload.capturedTarget,{account:'900000002',ticket:'71',volume:.8,fillPrice:25000,sl:24984,tp:25102});
  assert.equal(result.timings.macPreparation.managementSource,'exact-parent-vps-proof');
});
test('Bad selected volumes or parent metadata cannot create a fast protection intent',async t=>{
  for(const change of [{lots:NaN},{tp:-1},{ticket:'other'},{entryPlan:{}}]){
    const f=fixture(t);const result=await f.adapter.modifyEntryProtection({...f.row,...change},{tp:25110});
    assert.equal(result.status,'rejected');assert.equal(f.calls.length,0);
  }
});
test('Exact protection dispatch ignores display disconnect, stale quotes and a reconnecting transport',async t=>{
  const f=fixture(t),view=f.adapter.managementStateView.accountStates['900000002'];
  Object.assign(view,{connected:false,evidenceReady:false,quote:{at:1,bid:1}});
  f.adapter.connectionRestarting=true;
  const result=await f.adapter.modifyEntryProtection(f.row,{sl:24990});
  assert.equal(result.status,'ok',result.reason);assert.equal(f.calls.length,1);
  assert.equal(f.calls[0].payload.sl,24990);
});
test('A second explicit protection intent can use retained routing identity after a failed display read',async t=>{
  const f=fixture(t);
  assert.equal((await f.adapter.modifyEntryProtection(f.row,{tp:25110})).status,'ok');
  assert.equal(f.adapter.managementStateView,null);
  assert.equal(f.adapter.protectionDispatchState.accountStates['900000002'].orders,undefined);
  const result=await f.adapter.modifyEntryProtection({...f.row,tp:25110},{sl:24990});
  assert.equal(result.status,'ok',result.reason);assert.equal(f.calls.length,2);
  assert.equal(f.calls[1].payload.capturedTarget.tp,25110);
});
test('A lost protection response exposes the durable id without replaying its POST',async t=>{
  const f=fixture(t);let sends=0;
  f.adapter.request=async()=>{sends++;throw Error('offline');};
  const result=await f.adapter.modifyEntryProtection(f.row,{sl:24990});
  assert.equal(result.status,'unknown');assert.equal(result.id,f.adapter.commandJournal.records[0].id);
  assert.equal(result.entryId,'parent');assert.equal(sends,1);
  assert.equal(f.adapter.commandJournal.records[0].result.status,'unknown');
});
test('Exact protection still rejects wrong account identity and unsupported native protection',async t=>{
  for(const change of [{server:'other'},{account:'42'},{capabilities:[]}]){
    const f=fixture(t);Object.assign(f.adapter.managementStateView.accountStates['900000002'],change);
    const result=await f.adapter.modifyEntryProtection(f.row,{sl:24990});
    assert.equal(result.status,'rejected');assert.equal(f.calls.length,0);
  }
});
test('Exact protection policy keeps unresolved command barriers while allowing disconnected display evidence',t=>{
  const f=fixture(t),{exactEntryProtectionBlock,entryProtectionBlock}=require('../payload/src/app/services/mt5-vps/managementPolicy');
  const options={row:f.row,account:'900000002',linked:true,capabilities:f.adapter.capabilities,
    accountStates:{'900000002':{connected:false,evidenceReady:false,capabilities:f.adapter.capabilities}}};
  assert.equal(exactEntryProtectionBlock(options),'');assert.notEqual(entryProtectionBlock(options),'');
  for(const commands of [[{ticket:'71',result:{status:'unknown'}}],[{ticket:'entry:parent',result:{status:'unknown'}}]])
    assert.match(exactEntryProtectionBlock({...options,commands}),/неподтверждённая/);
  assert.match(exactEntryProtectionBlock({...options,rules:[{ticket:'71',state:'firing'}]}),/неподтверждённая/);
  assert.match(exactEntryProtectionBlock({...options,timeRules:[{ticket:'71',state:'unknown'}]}),/неподтверждённая/);
  assert.notEqual(exactEntryProtectionBlock({...options,row:{...f.row,entryPlan:{...f.row.entryPlan,protectionReady:false}}}),'');
});
test('Exact receipt projection edits only proven matching positions and leaves volume and execution status untouched',t=>{
  const {row}=fixture(t),context=receiptContext(row);
  const position={ticket:'71',volume:.8,fillPrice:25000,sl:24984,tp:25110};
  const result={id:'command',entryId:'parent',status:'ok',accountResults:{[account]:nativeOutcome('command',[position])}};
  const original=JSON.stringify(row),updated=protectionReceiptView([row],result,context)[0];
  assert.equal(updated.tp,25110);assert.equal(updated.entryPlan.tp,25110);assert.equal(updated.lots,.8);assert.equal(updated.entryPlan.phase,'completed');
  assert.equal(JSON.stringify(row),original);
  for(const changed of [{status:'unknown'},{id:'other'},{entryId:'other'}])assert.equal(protectionReceiptView([row],{...result,...changed},context)[0],row);
  assert.equal(protectionReceiptView([{...row,lots:.4}],result,context)[0].tp,25102);
  assert.equal(protectionReceiptView([row],result,{...context,server:'wrong-server'})[0],row);
  assert.equal(protectionReceiptView([row],result,{...context,checkedAt:result.accountResults[account].protectionProof.checkedAt+1})[0],row);
});
test('A peer rejection keeps its own outcome while an independent exact selected-account proof updates TP',t=>{
  const {row}=fixture(t);row.entryPlan.accounts=[account,peer];
  const id='partial-result',own=nativeOutcome(id,[{ticket:'71',volume:.8,fillPrice:25000,sl:24984,tp:0}],true);
  const result={id,entryId:'parent',status:'rejected',independentAccountControls:true,
    accountResults:{[account]:own,[peer]:{status:'rejected',reason:'Previous protection is unconfirmed'}}};
  const foreign={...row,ticket:'72',entryPlan:{...row.entryPlan,id:'different-parent'}};
  const pending={...row,ticket:'73',__isPosition:false};
  const rows=[row,foreign,pending],context=receiptContext(row);
  const updated=protectionReceiptView(rows,result,context);
  assert.equal(updated[0].tp,0);assert.equal(updated[0].sl,row.sl);
  assert.equal(updated[1],foreign);assert.equal(updated[2],pending);
  assert.equal(result.status,'rejected');assert.equal(result.accountResults[peer].status,'rejected');
  for(const changed of [{status:'unknown'},{controlId:'other'},{noPosition:true},
    {protectionProof:{...own.protectionProof,commandId:'other'}},{protectionProof:{...own.protectionProof,symbol:'SPX500'}},
    {protectionProof:{...own.protectionProof,account:peer}}]) {
    const different={...result,accountResults:{...result.accountResults,[account]:{...own,...changed}}};
    assert.equal(protectionReceiptView(rows,different,context),rows);
  }
  assert.equal(protectionReceiptView(rows,result,receiptContext({...row,ticket:'999'})),rows);
});
test('A single-account save updates its exact position without requiring receipts from unselected peers',t=>{
  const {row}=fixture(t);row.entryPlan.accounts=[account,peer];
  const id='selected-only',own=nativeOutcome(id,[{ticket:'71',volume:.8,fillPrice:25000,sl:24990,tp:25102}],true);
  const result={id,entryId:'parent',status:'ok',independentAccountControls:true,accountResults:{[account]:own}};
  const context=receiptContext({...row,singleAccount:true}),rows=[row];
  assert.equal(protectionReceiptView(rows,result,context)[0].sl,24990);
  assert.equal(protectionReceiptView(rows,{...result,accountResults:{...result.accountResults,[peer]:{status:'ok'}}},context),rows);
});
test('A confirmed control and a later read warning remain distinct; an uncertain new command retains its warning',()=>{
  const entry={id:'parent',phase:'completed',_readError:'Нет свежего статуса VPS',_lastConfirmedControl:{entryId:'parent',operation:'entry-protection',status:'ok'}};
  assert.match(readErrorLabel(entry),/Защита подтверждена/);
  assert.equal(readErrorLabel({...entry,phase:'unknown'}),'Нет связи с VPS');
  assert.equal(readErrorLabel({...entry,protectionPending:'new-command'}),'Нет связи с VPS');
});
for(const edited of ['sl','tp'])test(`A ${edited}-only native receipt retains both confirmed parent prices for the next explicit save`,async t=>{
  const f=fixture(t),sl=edited==='sl'?24990:24984,tp=edited==='tp'?25110:25102;
  const position={ticket:'71',volume:.8,fillPrice:25000,sl,tp};
  const id='confirmed-first-save',outcome={...nativeOutcome(id,[position]),[edited]:edited==='sl'?sl:tp};
  const result={id,entryId:'parent',status:'ok',accountResults:{[account]:outcome}};
  const row=protectionReceiptView([f.row],result,receiptContext(f.row))[0];
  assert.equal(row.entryPlan.sl,sl);assert.equal(row.entryPlan.tp,tp);
  assert.equal(require('../payload/src/app/services/mt5-vps/managementPolicy').entryPlanMetadata(row).id,'parent');
  const next=await f.adapter.modifyEntryProtection(row,edited==='sl'?{tp:25120}:{sl:24995});
  assert.equal(next.status,'ok',next.reason);assert.equal(f.calls.length,1);
  assert.equal(f.calls[0].payload.capturedTarget.sl,sl);assert.equal(f.calls[0].payload.capturedTarget.tp,tp);
});
test('Native protection projection accepts a removed take and refuses a mixed or missing parent proof',t=>{
  const f=fixture(t),position={ticket:'71',volume:.8,fillPrice:25000,sl:24990,tp:0};
  const id='confirmed',outcome={...nativeOutcome(id,[position]),sl:24990};
  const result={id,entryId:'parent',status:'ok',accountResults:{[account]:outcome}};
  assert.equal(protectionReceiptView([f.row],result,receiptContext(f.row))[0].entryPlan.tp,0);
  for(const positions of [[{...position,tp:undefined}],[position,{...position,ticket:'72',tp:25102}]]){
    const changed={...result,accountResults:{'900000002':{...outcome,positions,protectionProof:{...outcome.protectionProof,positions}}}};
    assert.equal(protectionReceiptView([f.row],changed,receiptContext(f.row))[0],f.row);
  }
});
test('Identical exact watched reads share in-flight work; a different ticket and a later request require fresh work',async t=>{
  const f=fixture(t),releases=[],calls=[];
  f.adapter.request=(route)=>{calls.push(route);return new Promise(resolve=>releases.push(()=>resolve({account:900000002,server:'test',accountStates:{}})));};
  f.adapter.cacheManagementResponse=(value)=>value;
  const watch=[{account:'900000002',ticket:'71',symbol:'USTEC'}];
  const first=f.adapter.managementState({watch}),same=f.adapter.managementState({watch:structuredClone(watch)}),other=f.adapter.managementState({watch:[{...watch[0],ticket:'72'}]});
  assert.equal(calls.length,2);releases.splice(0).forEach(fn=>fn());await Promise.all([first,same,other]);
  const later=f.adapter.managementState({watch});assert.equal(calls.length,3);releases.splice(0).forEach(fn=>fn());await later;
});

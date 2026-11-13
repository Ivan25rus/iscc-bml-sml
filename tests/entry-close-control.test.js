const test=require('node:test');
const assert=require('node:assert/strict');
const {EntryCloseControl,pendingEntryCloseConfirmation}=require('../payload/src/app/services/mt5-vps/entryCloseControl');
const open=()=>({id:'entry-a',symbol:'USTEC',side:'buy',executionPolicy:'hybrid-market-limit-v1',
  hasOpenPositions:true,positionsConfirmed:true,phase:'completed'});
test('Deferred or missing position fields retain the positively observed close control',()=>{
  const c=new EntryCloseControl(),entry=open();assert.deepEqual(c.state(entry),{visible:true,disabled:false,checking:false});
  assert.deepEqual(c.state({...entry,hasOpenPositions:null,positionsConfirmed:false,lifecycleDeferred:true}),{visible:true,disabled:false,checking:true});
  const missing={...entry};delete missing.hasOpenPositions;delete missing.positionsConfirmed;
  assert.deepEqual(c.state(missing),{visible:true,disabled:false,checking:true});
  assert.deepEqual(c.state(entry),{visible:true,disabled:false,checking:false});
});
test('Read errors retain a close action for the existing fresh backend check',()=>{
  const c=new EntryCloseControl();c.state(open());
  for(const patch of [{_readError:'No current status'},{phase:'unknown'},{hasOpenPositions:false,positionsConfirmed:true}])
    assert.deepEqual(c.state({...open(),...patch}),{visible:true,disabled:false,checking:true});
});
test('Busy, unknown close receipts and pending protection keep the circle visible while blocking duplicate actions',()=>{
  for(const patch of [{busy:true},{stopping:true},{closeUnknown:true}]){
    const c=new EntryCloseControl();assert.deepEqual(c.state(open(),patch),{visible:true,disabled:true,checking:false});
  }
  for(const patch of [{protectionPending:'one'},{closurePending:'two'}]){
    const c=new EntryCloseControl();assert.equal(c.state({...open(),...patch}).visible,true);
    assert.equal(c.state({...open(),...patch}).disabled,true);
  }
});
test('Definite full-close proof removes presentation evidence',()=>{
  for(const patch of [{positionLifecycle:'closed'},{positionsClosed:true}]){
    const c=new EntryCloseControl();c.state(open());
    assert.equal(c.state({...open(),...patch}).visible,false);assert.equal(c.knownOpen.size,0);
    assert.equal(c.state({...open(),hasOpenPositions:null,positionsConfirmed:false}).visible,false);
  }
});
test('Independent account controls retain a fresh close action after an unknown peer receipt',()=>{
  const c=new EntryCloseControl(),entry={...open(),independentAccountControls:true,phase:'unknown',
    accounts:{'900000001':{closurePending:'old'}}};
  assert.deepEqual(c.state(entry,{closeUnknown:true}),{visible:true,disabled:false,checking:true});
  assert.equal(c.state(entry,{closeUnknown:true,busy:true}).disabled,true);
  assert.equal(c.state({...entry,closurePending:'legacy'},{closeUnknown:true}).disabled,true);
});
test('Historical fills alone cannot invent a live close control',()=>{
  const c=new EntryCloseControl();
  assert.equal(c.state({...open(),hasOpenPositions:null,positionsConfirmed:false,accounts:{'900000001':{filledVolume:.8}}}).visible,false);
  assert.equal(c.state(null).visible,false);
  assert.equal(c.state({...open(),executionPolicy:'ordinary-limit'}).visible,false);
});
test('Presentation evidence is isolated by entry ID and broker symbol',()=>{
  const c=new EntryCloseControl();c.state(open());
  assert.equal(c.state({...open(),id:'entry-b',hasOpenPositions:null}).visible,false);
  assert.equal(c.state({...open(),symbol:'SPX500',hasOpenPositions:null}).visible,false);
});
test('Presentation bookkeeping never overwrites broker uncertainty',()=>{
  const c=new EntryCloseControl();c.state(open());
  const entry={...open(),hasOpenPositions:null,positionsConfirmed:false,_readError:'unavailable'};
  const before=JSON.stringify(entry);c.state(entry);assert.equal(JSON.stringify(entry),before);
});
test('Releasing a card clears its retained handle',()=>{
  const c=new EntryCloseControl();c.state(open());c.forget('entry-a');
  assert.equal(c.state({...open(),hasOpenPositions:null}).visible,false);
  c.state(open());c.clear();assert.equal(c.knownOpen.size,0);
});

function closeReceipt() {
  const entry={...open(),accounts:{900000001:{},900000003:{},900000004:{},900000005:{}},children:[
    {positions:{900000004:{ticket:'501',volume:.7}}}
  ]};
  const pending={status:'unknown',reason:'Broker has not confirmed every owned position fully closed',positions:[
    {status:'ok',operation:'close',ticket:'501',retcode:10009,providerOrderId:'601',filledVolume:.7,fillPrice:0}
  ]};
  const result={id:'close-id',entryId:entry.id,status:'unknown',independentAccountControls:true,
    accountResults:{900000001:{status:'ok'},900000003:{status:'ok'},900000004:pending,900000005:{status:'ok'}}};
  return {entry,result,pending};
}

test('Known completed accounts and an accepted ticket awaiting full-exit proof are reported separately',()=>{
  const {entry,result,pending}=closeReceipt();
  assert.deepEqual(pendingEntryCloseConfirmation(entry,result),{
    caption:'Закрыто 3 из 4',detail:'Закрыто 3 из 4 · 900000004 сверяется',
    accounts:'900000001 · закрыто\n900000003 · закрыто\n900000004 · сверяется\n900000005 · закрыто'
  });
  pending.reason='Absent owned position has no definite broker full-exit proof: waiting for MT5';
  result.accountResults['900000005'].noPosition=true;
  assert.equal(pendingEntryCloseConfirmation(entry,result).detail,'Закрыто 2 из 3 · 900000004 сверяется');
});

test('A rejected, mismatched, or partly accepted close remains uncertain rather than accepted for confirmation',()=>{
  for(const mutate of [
    ({result})=>{result.entryId='another-entry';},
    ({result})=>{result.closePercent=50;},
    ({result})=>{delete result.accountResults['900000001'];},
    ({pending})=>{pending.status='rejected';},
    ({pending})=>{pending.reason='Broker connection lost';},
    ({pending})=>{pending.positions[0].filledVolume=.3;},
    ({pending})=>{pending.positions[0].ticket='502';},
    ({pending})=>{pending.positions[0].retcode=10025;},
    ({pending})=>{pending.positions[0].providerOrderId='0';},
    ({entry})=>{entry.children.push({positions:{900000004:{ticket:'502',volume:.2}}});}
  ]) {
    const fixture=closeReceipt();mutate(fixture);
    assert.equal(pendingEntryCloseConfirmation(fixture.entry,fixture.result),null);
  }
});

const test=require('node:test');
const assert=require('node:assert/strict');
const {ids,resolve}=require('../selection');
const catalog=require('../features.json');
const {createFeaturePolicy}=require('../module/featurePolicy');
const {createBmlSml}=require('../module');

test('every nonempty feature combination keeps the catalog dependency contract',()=>{
 assert.deepEqual(ids,catalog.features.map(row=>row.id));
 for(let mask=1;mask<1<<ids.length;mask++){
  const requested=ids.filter((id,index)=>mask&(1<<index)),selected=resolve(requested);
  for(const row of catalog.features)if(selected.features.includes(row.id))
   for(const dependency of row.requires)assert.ok(selected.features.includes(dependency),row.id+' needs '+dependency);
  assert.equal(selected.mt5Core,catalog.features.some(row=>row.mt5Core&&selected.features.includes(row.id)));
 }
});
test('levels and entry remain independent, and invalid choices cannot enable an addon',()=>{
 assert.deepEqual(resolve(['levels-to-lt']).features,['levels','levels-to-lt']);
 assert.deepEqual(resolve(['bml-sml']).features,['bml-sml']);
 for(const value of [[],['unknown'],['levels','levels'],null])assert.throws(()=>resolve(value));
});
test('management without partial closing permits a full exit and rejects a reduced exit',()=>{
 const policy=createFeaturePolicy(['management']);
 assert.ok(policy.allow('mt5-vps:manage',{action:'close',target:{lots:2},volume:2}));
 for(const args of [{volume:1},{percent:50},{volume:0},{volume:NaN}])
  assert.equal(policy.allow('mt5-vps:manage',{action:'close',target:{lots:2},...args}),false);
 assert.ok(policy.allow('mt5-vps:manage',{action:'entry-close'}));
 assert.ok(policy.allow('mt5-vps:manage',{action:'cancel-close'}));
 assert.ok(policy.allow('mt5-vps:manage',{action:'cancel-time-exit'}));
 assert.equal(policy.allow('mt5-vps:manage',{action:'schedule-close'}),false);
 assert.equal(policy.allow('mt5-vps:save-followers'),false);
 assert.equal(policy.allow('mt5-vps:save-time-exit'),false);
});
test('partial exit does not implicitly expose a new price or time rule',()=>{
 const policy=createFeaturePolicy(['partial-close']);
 assert.ok(policy.allow('mt5-vps:manage',{action:'close',target:{lots:2},percent:50}));
 assert.equal(policy.allow('mt5-vps:manage',{action:'schedule-close'}),false);
 assert.equal(policy.allow('mt5-vps:save-time-exit'),false);
 assert.ok(createFeaturePolicy(['price-close']).allow('mt5-vps:manage',{action:'schedule-close'}));
 assert.ok(policy.allow('mt5-vps:manage',{action:'entry-partial-close',closePercent:50}));
 assert.equal(createFeaturePolicy(['management']).allow('mt5-vps:manage',{action:'entry-partial-close',closePercent:50}),false);
 assert.equal(policy.allow('mt5-vps:manage',{action:'entry-take',take:{operation:'arm'}}),false);
 assert.ok(createFeaturePolicy(['price-close']).allow('mt5-vps:manage',{action:'entry-take',take:{operation:'arm'}}));
 assert.ok(createFeaturePolicy(['management']).allow('mt5-vps:manage',{action:'entry-take',take:{operation:'cancel'}}));
});
test('unselected new entry is blocked but stopping an existing plan remains available',async()=>{
 const calls=[],addon=createBmlSml({features:['management'],enabled:()=>true,
  getAdapter:()=>({stopChunkEntry:id=>{calls.push(id);return 'stopped';}})});
 assert.equal((await addon.arm({action:'MB',entryMode:'side-zone'})).status,'rejected');
 assert.equal(addon.stop('existing-plan'),'stopped');assert.deepEqual(calls,['existing-plan']);
});
test('levels-only renderer does not load or mount trading dialogs',()=>{
 require('../module/renderer').mount({features:['levels-to-lt'],ipc:{invoke:()=>assert.fail('Unexpected IPC')},
  toolbar:{append:()=>assert.fail('Unexpected trading UI')}});
});
test('selected management retains quote events and does not expose partial-take actions',async()=>{
 const name=require.resolve('../payload/src/app/services/mt5-vps/management');
 const original=require(name),calls=[];let mounted;
 require.cache[name].exports={mountManagement:(...args)=>mounted=args};
 try{
  const ipc={invoke:(...args)=>{calls.push(['invoke',...args]);return Promise.resolve({status:'ok'});},
   on:(...args)=>calls.push(['on',...args]),removeListener:(...args)=>calls.push(['remove',...args])};
  const getInstrumentInfo=()=>({bid:100,ask:101});
  require('../module/renderer').mount({features:['management'],ipc,toolbar:{},getInstrumentInfo});
  const [selected,,options]=mounted;assert.equal(options.getInstrumentInfo,getInstrumentInfo);
  assert.equal(options.partialClose,false);assert.equal(options.priceClose,false);
  const callback=()=>{};selected.on('instrument:updated',callback);selected.removeListener('instrument:updated',callback);
  assert.equal((await selected.invoke('mt5-vps:manage',{action:'entry-take',take:{operation:'arm'}})).status,'rejected');
  assert.equal(calls.filter(row=>row[0]==='invoke').length,0);
  await selected.invoke('mt5-vps:manage',{action:'entry-close'});
  assert.deepEqual(calls.map(row=>row[0]),['on','remove','invoke']);
 }finally{require.cache[name].exports=original;}
});

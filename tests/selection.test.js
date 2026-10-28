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

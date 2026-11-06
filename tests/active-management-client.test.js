const test=require('node:test'),assert=require('node:assert/strict');
const {MT5VPSAdapter}=require('../payload/src/app/services/mt5-vps/client');
test('Compact management is selected only with the authenticated coordinator capability; legacy and account routes remain full',()=>{
  const adapter=new MT5VPSAdapter({enabled:false}),watch=[{account:'900000002',ticket:'71',symbol:'USTEC'}];
  assert.equal(adapter.managementRoute(),'/management');
  const legacy=adapter.managementRoute(watch);assert.ok(legacy.startsWith('/management?watch='));
  adapter.capabilities=['management-active-v1'];assert.equal(adapter.managementRoute(),'/management?view=active');
  assert.equal(adapter.managementRoute(watch),legacy+'&view=active');
  adapter.config.routePrefix='/accounts/900000001';assert.equal(adapter.managementRoute(),'/management');adapter.close();
});
test('Explicit history uses the full read endpoint and leaves current position proof untouched',async()=>{
  const adapter=new MT5VPSAdapter({enabled:false}),current={orders:[]},calls=[];
  adapter.capabilities=['management-active-v1'];adapter.managementStateView=current;
  adapter.request=async(...args)=>{calls.push(args);return {timeRules:[{state:'completed'}]};};
  assert.equal((await adapter.managementHistory()).timeRules.length,1);assert.deepEqual(calls,[['/management']]);
  assert.equal(adapter.managementStateView,current);adapter.close();
});
test('Quote stage timestamps isolate source, publisher, delivery and Mac processing without changing acceptance',()=>{
  const c=require('../payload/src/app/services/mt5-vps/quoteContinuity').createQuoteContinuity();
  const row=c.observe(10000,13000,500,false,1,{publishedAt:10010,writeStartedAt:10020,processingDelayMs:7,frameWorkMs:2,
    quoteTiming:{nativeMs:4,rpcMs:10,token:'never retained'}});
  assert.equal(row.captureToPublishMs,10);assert.equal(row.publishToWriteMs,10);assert.equal(row.writeToArrivalMs,2980);
  assert.equal(row.processingDelayMs,7);assert.equal(row.frameWorkMs,2);assert.equal(row.accepted,false);
  assert.deepEqual(row.sourceTiming,{nativeMs:4,rpcMs:10});
});

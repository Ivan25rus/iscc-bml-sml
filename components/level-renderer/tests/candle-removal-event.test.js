const test=require('node:test'),assert=require('node:assert/strict');
const {removalScope}=require('../module/candle-source-event');
const {CandleLevelSync}=require('../module/candleLevels');
const event={event:'http_request',dir:'out',method:'PUT',host:'charts-storage.tradingview.com',
  path:'/charts-storage/user/sources?chart_id=pane123&layout_id=working&jwt=not-logged'};
test('storage deletion uses layout_id, never pane ID or untrusted path metadata',()=>{
  assert.equal(removalScope(event),'working');
  assert.equal(removalScope({...event,path:event.path.replace('/sources?','/sources/user123?')}),'working');
  for(const change of [{method:'GET'},{dir:'in'},{event:'message'},{host:'tradingview.com.evil.test'},
    {path:'//evil.test/charts-storage/user/sources?layout_id=working'},
    {path:'/charts-storage/user/sources?chart_id=working'},
    {path:'/charts-storage/user/sources?layout_id=working&layout_id=other'},
    {path:'/other?layout_id=working'}])assert.equal(removalScope({...event,...change}),null);
});
test('explicit deletion survives a loading inventory and uses durable owned MT5 removal',async()=>{
  const symbol='CAPITALCOM:NAS100',config={mappings:[{enabled:true,sourceSymbol:symbol,mt5Symbol:'USTEC',sourceLayout:'working'}]};
  const calls=[],saved=[];const sync=new CandleLevelSync({getConfig:()=>config,persist:async x=>saved.push(x),
    getAdapter:()=>({capabilities:['candle-levels'],request:async(route,payload)=>{
      if(payload){assert.ok(saved.at(-1).records[0].removed);calls.push(payload);}
      return {candleLevels:{snapshotApplied:true,levels:[]}};
    }})});
  const source={symbol,serverUpdateTime:10,state:{type:'LineToolHorzRay',points:[{time_t:1793803440,price:28691.7,interval:'1'}],state:{}}};
  sync.acceptSource('old',source,{scope:'working'});const row=sync.records.get('old');row.phase='ready';row.price=28692.68;
  assert.equal(sync.acceptRemovalEvent('unknown',event),false);
  assert.equal(sync.acceptRemovalEvent('old',{...event,path:event.path.replace('layout_id=working','layout_id=other')}),false);
  assert.equal(sync.acceptRemovalEvent('old',event),true);
  sync.acceptSnapshot({sources:[['old',source]],series:[],inventory:{scope:'working',complete:false,symbols:[]}});
  assert.equal(sync.records.get('old').removed,true,'Delayed saved drawing cannot undo the ordered deletion');
  await sync.tick();assert.deepEqual(calls,[{id:row.id,revision:row.revision+1,action:'delete'}]);
  assert.equal(sync.records.get('old').phase,'removed');
});

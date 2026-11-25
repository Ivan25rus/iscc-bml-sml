const {test}=require('node:test');
const assert=require('node:assert/strict');
const {createChunkAdmission}=require('../payload/src/app/services/levelOrder/chunkAdmission');
const {calculateChunkSizing,resolveLevelOrderDefaults}=require('../payload/src/app/services/levelOrder/strategy');

function fixture(overrides={}) {
  const sent=[];
  const payload={ticker:'USTEC',provider:'mt5-vps',instrumentType:'FX',action:'BS',level:25000,entryPrice:25001,
    riskUsd:20,stopOffsetPts:1600,stopMode:'entry',requestId:'fixed-arm-1'};
  const deps={getAdapter:()=>({armChunkEntry:async(plan,options)=>{sent.push({plan,options});return {status:'ok',entry:{id:options.requestId}};}}),
    getGroups:()=>[{ticker:'USTEC',levels:[25000],maxOffset:6}],
    getConfig:()=>({defaults:{chunkPercent:20,chunkMaxGap:2}}),
    getSnapshot:async()=>({provider:'mt5-vps',symbol:'USTEC',quoteUpdatedAt:Date.now(),quote:{bid:24999.8,ask:25000},metadata:{tickSize:.01,quantityStep:.1,contractSize:1}}),
    orderCalculator:{qty:()=>1},validateTrade:()=>({ok:true}),...overrides};
  return {service:createChunkAdmission(deps),payload,sent};
}

test('chunk admission fixes LT, uses trusted broker specifications/settings and one risk budget',async()=>{
  const f=fixture();
  const result=await f.service.arm({...f.payload,tickSize:1,chunkVolume:10,maxGap:100});
  assert.equal(result.status,'ok');assert.equal(f.sent.length,1);
  assert.equal(f.sent[0].plan.stopPrice,24985);assert.equal(f.sent[0].plan.totalQty,1);
  assert.equal(f.sent[0].plan.chunkVolume,.2);assert.equal(f.sent[0].plan.maxGap,2);
  assert.equal(f.sent[0].plan.chunkPercent,20);
  assert.equal(f.sent[0].plan.tickSize,.01);assert.equal(f.sent[0].plan.riskUsd,20);
});

test('part percentage scales with the primary risk-sized total and rounds down to the broker step',async()=>{
  for(const [total,percent,expected,count] of [[1,20,.2,5],[2,20,.4,5],[.7,20,.1,7],[.7,50,.3,3],[1,100,1,1],[5,2,.1,50]]) {
    const f=fixture({orderCalculator:{qty:()=>total}});
    const result=await f.service.arm({...f.payload,chunkPercent:percent,chunkVolume:100,minLot:1});
    assert.equal(result.status,'ok');
    const plan=f.sent[0].plan;
    assert.equal(plan.totalQty,total);assert.equal(plan.chunkPercent,percent);assert.equal(plan.chunkVolume,expected);
    assert.equal(plan.stopPrice,24985);assert.equal(plan.riskUsd,20);
    assert.equal(calculateChunkSizing({totalQty:total,chunkPercent:percent,quantityStep:.1}).chunkCount,count);
  }
});

test('invalid percentages and unrepresentable broker quantities never arm or enlarge a part',async()=>{
  for(const chunkPercent of [0,1,1.99,100.01,-20,NaN,Infinity,'bad','']) {
    const f=fixture();
    assert.equal((await f.service.arm({...f.payload,chunkPercent})).status,'rejected');
    assert.equal(f.sent.length,0);
  }
  for(const [total,percent,step,minimum] of [[.1,20,.1,.1],[5.1,2,.1,.1],[1,20,.1,.3],[1,20,0,.1],[1,20,NaN,.1],[.75,20,.1,.1]]) {
    const f=fixture({orderCalculator:{qty:()=>total},getSnapshot:async()=>({provider:'mt5-vps',symbol:'USTEC',quoteUpdatedAt:Date.now(),
      quote:{bid:24999.8,ask:25000},metadata:{tickSize:.01,quantityStep:step,minQty:minimum,contractSize:1}})});
    assert.equal((await f.service.arm({...f.payload,chunkPercent:percent})).status,'rejected');
    assert.equal(f.sent.length,0);
  }
});

test('new percent settings replace legacy lots and per-symbol percentages win independently',async()=>{
  const config={defaults:{chunkVolume:.9,chunkPercent:25,chunkMaxGap:3},
    symbols:[{ticker:'ustec',chunkVolume:.4,chunkPercent:50,chunkMaxGap:2}]};
  assert.equal(resolveLevelOrderDefaults(config,'USTEC').chunkPercent,50);
  assert.equal(resolveLevelOrderDefaults(config,'SPX500').chunkPercent,25);
  assert.equal(resolveLevelOrderDefaults({defaults:{chunkVolume:.9},symbols:[{ticker:'USTEC',chunkVolume:.4}]},'USTEC').chunkPercent,20);
  assert.equal(resolveLevelOrderDefaults({defaults:{chunkPercent:'invalid'}},'USTEC').chunkPercent,NaN);
  const f=fixture({getConfig:()=>config});
  assert.equal((await f.service.arm(f.payload)).status,'ok');
  assert.equal(f.sent[0].plan.chunkPercent,50);assert.equal(f.sent[0].plan.chunkVolume,.5);
  const invalid=fixture({getConfig:()=>({defaults:{chunkPercent:101}})});
  assert.equal((await invalid.service.arm(invalid.payload)).status,'rejected');assert.equal(invalid.sent.length,0);
});

test('stale quote, inactive LT, wrong provider and failed trade rule never arm',async()=>{
  const cases=[
    {getGroups:()=>[]},
    {getSnapshot:async()=>({provider:'mt5-vps',symbol:'USTEC',quoteUpdatedAt:Date.now()-2000,quote:{bid:24999,ask:25000}})},
    {validateTrade:()=>({ok:false,reason:'Risk restriction'})}
  ];
  for(const overrides of cases){const f=fixture(overrides);assert.equal((await f.service.arm(f.payload)).status,'rejected');assert.equal(f.sent.length,0);}
  const f=fixture();assert.equal((await f.service.arm({...f.payload,provider:'dwx'})).status,'rejected');assert.equal(f.sent.length,0);
});

test('same simultaneous ID coalesces without sending a second arm',async()=>{
  let release;const f=fixture({getSnapshot:()=>new Promise(resolve=>{release=()=>resolve({provider:'mt5-vps',symbol:'USTEC',quoteUpdatedAt:Date.now(),quote:{bid:24999,ask:25000},metadata:{tickSize:.01,quantityStep:.1,contractSize:1}});})});
  const first=f.service.arm(f.payload),second=f.service.arm(f.payload);
  assert.equal(f.service.busy,true);release();
  const results=await Promise.all([first,second]);assert.equal(results[0].status,'ok');assert.equal(f.sent.length,1);assert.equal(f.service.busy,false);
});

test('An open index position requires a separate card and an unoccupied LT',async()=>{
  for(const ticker of ['USTEC','SPX500']) {
    const sent=[];
    const level=ticker==='USTEC' ? 25000 : 7000;
    const adapter={brokerSymbol:symbol=>symbol,config:{account:'1',server:'broker'},
      chunkLifecycleEntries:new Map([['original',{symbol:ticker,level,phase:'completed',hasOpenPositions:true}]]),
      armChunkEntry:async plan=>{sent.push(plan);return {status:'ok'};}};
    const f=fixture({getAdapter:()=>adapter,
      readAccountOrders:async account=>({account,server:'broker',orders:[{symbol:ticker,__isPosition:true,lots:1}]}),
      getGroups:()=>[{ticker,levels:[level,level+10],maxOffset:6}],
      getSnapshot:async()=>({provider:'mt5-vps',symbol:ticker,metadata:{tickSize:.01,quantityStep:.1,contractSize:1,maxQty:100}})});
    const input={...f.payload,ticker,action:'MB',entryMode:'side-zone',level};
    assert.match((await f.service.arm(input)).reason,/Другой ЛТ/);
    assert.match((await f.service.arm({...input,separateLt:true})).reason,/уже закреплён/);
    assert.equal((await f.service.arm({...input,level:level+10,separateLt:true})).status,'ok');
    assert.deepEqual(sent.map(plan=>[plan.level,plan.referencePrice,plan.separateLt]),[[level+10,level+10,true]]);
  }
});

test('A partial management view uses direct MT5 evidence before classifying an old SPX fill',async()=>{
  for(const [caseName, expected] of [['flat','ok'],['live','rejected'],['missing','rejected'],['fresh','ok']]) {
    const sent=[],now=Date.now(),ticker='SPX500',level=7000;
    const view=(account,orders)=>({account,server:'broker',connected:true,orders,checkedAt:now});
    const previous={symbol:ticker,level,phase:'completed',accounts:{main:{filledVolume:1}},updatedAt:now-60000};
    const native={account:'main',server:'broker',linkedAccounts:['copy'],accountStates:{
      main:view('main',[])}};
    if(caseName==='fresh')native.accountStates.copy=view('copy',[]);
    const reads=[];
    const adapter={brokerSymbol:symbol=>symbol,config:{account:'main',server:'broker'},remoteAccounts:['copy'],
      managementStateView:native,managementStateAt:now,
      chunkLifecycleEntries:new Map([['original',previous]]),
      armChunkEntry:async plan=>{sent.push(plan);return {status:'ok'};}};
    const f=fixture({getAdapter:()=>adapter,readAccountOrders:async account=>{
      reads.push(account);
      if(caseName==='missing' && account==='copy')throw Error('MT5 disconnected');
      return {account,server:'broker',orders:account==='copy' ? [{symbol:ticker,__isPosition:caseName==='live',lots:1}] : []};
    },getGroups:()=>[{ticker,levels:[level],maxOffset:6}],
      getSnapshot:async()=>({provider:'mt5-vps',symbol:ticker,metadata:{tickSize:.01,quantityStep:.1,contractSize:1,maxQty:100}})});
    const result=await f.service.arm({...f.payload,ticker,action:'MB',entryMode:'side-zone',level});
    assert.equal(result.status,expected,caseName);
    assert.deepEqual(reads,caseName==='fresh' ? [] : ['main','copy'],caseName);
    assert.equal(sent.length,expected==='ok' ? 1 : 0,caseName);
    if(caseName==='live') {
      assert.match(result.reason,/Другой ЛТ/);
      assert.equal(result.positionEvidence.symbol,ticker);
    }
    if(caseName==='missing')assert.match(result.reason,/Состояние позиции MT5 уточняется/);
  }
});

test('Position verification reads one broker symbol and preserves every account check',async()=>{
  for(const scenario of ['flat','open-with-missing-peer','missing','stale','wrong-symbol','wrong-account','fresh-cache']) {
    const now=Date.now(),ticker='SPX500',brokerSymbol='SPX500.z',level=7000,requests=[],sent=[];
    const view=account=>({account,server:'broker',connected:true,orders:[],checkedAt:now});
    const proof=(account,extra={})=>({account,server:'broker',symbol:brokerSymbol,checkedAt:now,hasPosition:false,...extra});
    const copy=scenario==='missing' || scenario==='open-with-missing-peer' ? null
      : proof('2',scenario==='stale' ? {checkedAt:now-3000} : scenario==='wrong-symbol' ? {symbol:'USTEC'}
        : scenario==='wrong-account' ? {account:'3'} : {});
    const adapter={config:{account:'1',server:'broker'},capabilities:['position-presence-v1'],remoteAccounts:['2'],
      brokerSymbol:symbol=>symbol===ticker ? brokerSymbol : symbol,
      managementStateAt:now,managementStateView:{account:'1',server:'broker',
        accountStates:{1:view('1'),...(scenario==='fresh-cache' ? {2:view('2')} : {})}},
      chunkLifecycleEntries:new Map([['older',{symbol:brokerSymbol,level,phase:'completed',hasOpenPositions:true,updatedAt:now-10000}]]),
      request:async route=>{
        requests.push(route);
        return {account:'1',server:'broker',symbol:brokerSymbol,positionStates:{
          1:proof('1',{hasPosition:scenario==='open-with-missing-peer'}),2:copy}};
      },armChunkEntry:async plan=>{sent.push(plan);return {status:'ok'};}};
    const f=fixture({getAdapter:()=>adapter,readAccountOrders:async()=>assert.fail('Legacy position list requested'),
      getGroups:()=>[{ticker,levels:[level],maxOffset:6}],
      getSnapshot:async()=>({provider:'mt5-vps',symbol:ticker,metadata:{tickSize:.01,quantityStep:.1,contractSize:1,maxQty:100}})});
    const result=await f.service.arm({...f.payload,ticker,action:'MB',entryMode:'side-zone',level});
    if(scenario==='fresh-cache'){
      assert.deepEqual(requests,[]);
    } else {
      assert.deepEqual(requests,['/position-presence?symbol=SPX500.z&accounts=1%2C2']);
      assert.equal(result.timings.positionVerificationMode,'symbol');
    }
    assert.equal(sent.length,['flat','fresh-cache'].includes(scenario) ? 1 : 0,scenario);
    if(scenario==='open-with-missing-peer')assert.match(result.reason,/Другой ЛТ/);
    if(['missing','stale','wrong-symbol','wrong-account'].includes(scenario))assert.match(result.reason,/уточняется/);
  }
});

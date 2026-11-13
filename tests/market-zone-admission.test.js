const {test} = require('node:test');
const assert = require('node:assert/strict');
const {createChunkAdmission} = require('../payload/src/app/services/levelOrder/chunkAdmission');
const {MT5VPSAdapter} = require('../payload/src/app/services/mt5-vps/client');
const {buildEntryRequest} = require('../payload/src/app/services/mt5-vps/chunkEntryClient');

function fixture(overrides = {}) {
  const sent = [], rules = [];
  const payload = {ticker:'USTEC',provider:'mt5-vps',instrumentType:'FX',action:'MB',entryMode:'side-zone',
    level:25000,entryPrice:24999,zoneMultiplier:2,riskUsd:50,stopOffsetPts:1600,stopMode:'entry',requestId:'zone-arm-1'};
  const service = createChunkAdmission({
    getAdapter:() => ({armChunkEntry:async plan => {sent.push(plan);return {status:'ok',entry:{id:'zone-arm-1'}};}}),
    getGroups:() => [{ticker:'USTEC',levels:[25000],maxOffset:6}],getConfig:() => ({defaults:{chunkPercent:20,chunkMaxGap:2}}),
    getSnapshot:async () => ({provider:'mt5-vps',symbol:'USTEC',quoteUpdatedAt:Date.now(),quote:{bid:24970,ask:24970.2},
      metadata:{tickSize:.01,quantityStep:.1,minQty:.1,maxQty:10,contractSize:1}}),
    orderCalculator:{qty:({riskUsd,stopPts,tickSize,lot})=>riskUsd/(stopPts*tickSize*lot)},
    validateTrade:order => {rules.push(order);return {ok:true};},...overrides});
  return {service,payload,sent,rules};
}

test('market arm accepts lost LT and a reversal, takes play from saved groups and requests the complete risk-sized volume', async () => {
  const f = fixture();
  const result = await f.service.arm({...f.payload,zoneRadius:100,chunkPercent:2,chunkVolume:10,maxGap:100});
  assert.equal(result.status,'ok');
  const plan = f.sent[0];
  assert.equal(plan.zoneRadius,12); assert.equal(plan.referencePrice,25000);
  assert.equal(plan.zoneLower,25000); assert.equal(plan.zoneUpper,25012); assert.equal(plan.stopPrice,24984);
  assert.equal(plan.totalQty,3.1); assert.equal(plan.chunkVolume,.6); assert.equal(plan.chunkPercent,20);
  assert.equal(plan.maxGap,12); assert.equal(plan.riskUsd,50);
  assert.equal(plan.executionPolicy,'hybrid-market-limit-v1');
  assert.equal(f.rules[0].type,'market'); assert.equal(f.rules[0].price,25012);
});

test('primary balance is read again for each new entry and replaces the card amount',async()=>{
  const plans=[], balances=[100000,105000];
  const adapter={config:{account:900000003,server:'Example-MT5',primaryRiskByAccount:{900000003:{mode:'balance',percent:.2}}},
    async request(route,body) {
      assert.equal(route,'/health');
      assert.equal(body,undefined);
      return {account:900000003,server:'Example-MT5',tradingEnabled:true,accountInfo:{balance:balances.shift(),equity:99000}};
    },
    async armChunkEntry(plan){plans.push(plan);return {status:'ok'};}};
  const f=fixture({getAdapter:()=>adapter});
  await Promise.all([f.service.arm({...f.payload,riskUsd:13}),f.service.arm({...f.payload,riskUsd:13})]);
  assert.equal((await f.service.arm({...f.payload,requestId:'second',riskUsd:13})).status,'ok');
  assert.deepEqual(plans.map(plan=>plan.riskUsd),[200,210]);
  assert.deepEqual(plans.map(plan=>plan.totalQty),[12.5,13.1]);
  assert.equal(plans[0].balanceRisk.account,'900000003');
  assert.equal(plans[0].balanceRisk.balance,100000);
  assert.equal(balances.length,0);
});

test('native primary risk arms without a balance HTTP read and keeps the captured constraints',async()=>{
  const config={account:900000003,server:'Example-MT5',primaryRiskByAccount:{900000003:{mode:'balance',percent:.2}},
    followers:[{account:'900000004',enabled:true,multiplier:1,riskPercent:.2}],timeExit:{enabled:true,time:'15:59'}};
  const adapter={config,capabilities:['atomic-entry-admission-v1','entry-primary-balance-risk-v1',
    'independent-account-engines-v5','entry-initial-sizing-only-v1','entry-balance-risk-v1'],
    brokerSymbol:s=>s,request:()=>assert.fail('separate balance HTTP request'),
    armChunkEntry:async plan=>({status:'ok',request:buildEntryRequest(adapter,plan)})};
  const f=fixture({getAdapter:()=>adapter,getQuantityLimit:()=>100});
  const result=await f.service.arm({...f.payload,maxLot:.6});
  assert.equal(result.status,'ok');
  assert.deepEqual(result.request.primaryRiskRequest,{account:'900000003',percent:.2,quantityStep:.1,minQty:.1,
    maxQty:10,pointValue:1,maxLot:.6,quantityLimit:100});
  assert.equal(result.request.riskUsd,undefined);
  assert.equal(result.request.volume,undefined);
  assert.ok(result.request.requiredCapabilities.includes('entry-primary-balance-risk-v1'));
  assert.equal(result.request.mirrorAccounts[0].riskPercent,.2);
});

test('a click keeps its account, exit and size settings while instrument metadata is pending',async()=>{
  const adapter=new MT5VPSAdapter({account:'900000003',server:'fixture',ustecSymbol:'USTEC',spxSymbol:'SPX500',
    followers:[{account:'900000001',enabled:true,multiplier:1}],timeExit:{enabled:true,time:'15:59'},entryRiskLimitUsd:60});
  adapter.capabilities=['atomic-entry-admission-v1','chunked-breakout','market-zone-entry','market-zone-entry-stop',
    'market-side-zone-entry','hybrid-market-limit-v1','independent-account-engines-v5','entry-initial-sizing-only-v1','entry-balance-risk-v1'];
  const posts=[];
  adapter.request=async(route,request)=>{
    assert.equal(route,'/entry-plan-admit');posts.push(request);
    return {admissionContract:'atomic-entry-admission-v1',capabilities:adapter.capabilities,entry:{id:request.id,phase:'armed'}};
  };
  let release,defaults={chunkPercent:20};
  const f=fixture({getAdapter:()=>adapter,getConfig:()=>({defaults}),getSnapshot:()=>new Promise(resolve=>{release=resolve;})});
  const reply=f.service.arm(f.payload);
  adapter.config.followers=[{account:'900000004',enabled:true,multiplier:1,riskPercent:.2}];
  adapter.config.timeExit.time='16:30';adapter.config.entryRiskLimitUsd=5;defaults={chunkPercent:50};
  release({provider:'mt5-vps',symbol:'USTEC',metadata:{tickSize:.01,quantityStep:.1,minQty:.1,maxQty:10,contractSize:1}});
  assert.equal((await reply).status,'ok');
  assert.deepEqual(posts[0].mirrorAccounts,[{account:'900000001',multiplier:1}]);
  assert.equal(posts[0].closeLocalTime,'15:59');assert.equal(posts[0].chunkPercent,20);assert.equal(posts[0].entryRiskLimitUsd,60);
  const next=f.service.arm({...f.payload,requestId:'after-switch',riskUsd:4});
  release({provider:'mt5-vps',symbol:'USTEC',metadata:{tickSize:.01,quantityStep:.1,minQty:.1,maxQty:10,contractSize:1}});
  assert.equal((await next).status,'ok');
  assert.deepEqual(posts[1].mirrorAccounts,[{account:'900000004',multiplier:1,riskPercent:.2}]);
  assert.equal(posts[1].closeLocalTime,'16:30');assert.equal(posts[1].chunkPercent,50);assert.equal(posts[1].entryRiskLimitUsd,5);
});

test('primary percent cannot use an unconfirmed balance or the card amount as a fallback',async()=>{
  const config={account:900000003,server:'Example-MT5',primaryRiskByAccount:{900000003:{mode:'balance',percent:.2}}};
  const valid={account:900000003,server:'Example-MT5',tradingEnabled:true,accountInfo:{balance:100000}};
  for(const response of [{...valid,account:900000002},{...valid,accountInfo:{equity:100000}},{...valid,tradingEnabled:false}]){
    const f=fixture({getAdapter:()=>({config,request:async()=>response,armChunkEntry:()=>assert.fail('sent a trade')})});
    assert.equal((await f.service.arm(f.payload)).status,'rejected');
  }
  const f=fixture({getAdapter:()=>({config:{...config,account:900000002},request:()=>assert.fail('read another account'),
    armChunkEntry:async plan=>{assert.equal(plan.riskUsd,13);return {status:'ok'};}})});
  assert.equal((await f.service.arm({...f.payload,riskUsd:13})).status,'ok');
});

test('approved primary credit supports a zero balance without using equity or card risk',async()=>{
  const config={account:900000004,server:'Example-MT5',primaryRiskByAccount:{900000004:{mode:'balance',percent:.2}}};
  const accountInfo={balance:0,credit:5000,equity:6100,riskBase:5000,riskIncludesCredit:true};
  const health={account:900000004,server:'Example-MT5',tradingEnabled:true};
  const f=fixture({getAdapter:()=>({config,request:async()=>({...health,accountInfo}),
    armChunkEntry:async plan=>{assert.equal(plan.riskUsd,10);assert.equal(plan.balanceRisk.credit,5000);
      assert.equal(plan.balanceRisk.balance,0);return {status:'ok'};}})});
  assert.equal((await f.service.arm(f.payload)).status,'ok');
  for(const info of [{...accountInfo,riskIncludesCredit:false},{...accountInfo,credit:undefined},
    {...accountInfo,riskBase:6100}]) {
    const rejected=fixture({getAdapter:()=>({config,request:async()=>({...health,accountInfo:info}),
      armChunkEntry:()=>assert.fail('sent an entry with unverified funds')})});
    assert.equal((await rejected.service.arm(rejected.payload)).status,'rejected');
  }
});

test('market admission never accepts forged bands, missing LT, invalid multipliers or missing specifications', async () => {
  for (const overrides of [{getGroups:()=>[]},{validateTrade:()=>({ok:false,reason:'Risk restriction'})},
    {getSnapshot:async()=>({provider:'mt5-vps',symbol:'USTEC',metadata:{}})}]) {
    const f = fixture(overrides);
    assert.equal((await f.service.arm(f.payload)).status,'rejected'); assert.equal(f.sent.length,0);
  }
  for (const payload of [{zoneMultiplier:3},{entryMode:'bad'},{action:'BS'},{provider:'dwx'},
    {level:24990},{takeProfitPts:1}]) {
    const f = fixture();
    assert.equal((await f.service.arm({...f.payload,...payload})).status,'rejected'); assert.equal(f.sent.length,0);
  }
});

test('Freshly delivered SPX snapshots can arm a fixed BML/SML plan despite an old native tick; only the VPS waits or trades',async()=>{
  for(const action of ['MB','MS']) {
    const f=fixture({getGroups:()=>[{ticker:'SPX500',levels:[7725.63],maxOffset:1.5}],
      getSnapshot:async()=>({provider:'mt5-vps',symbol:'SPX500',quoteUpdatedAt:Date.now(),
        quote:{bid:7725.5,ask:7725.7,nativeCapturedAt:Date.now()-5000,nativeAgeMs:5000},
        metadata:{tickSize:.01,quantityStep:.1,minQty:.1,maxQty:10,contractSize:1}})});
    const payload={...f.payload,ticker:'SPX500',level:7725.63,entryPrice:7725.63,action,zoneMultiplier:1,stopOffsetPts:300};
    const results=await Promise.all([f.service.arm(payload),f.service.arm(payload)]);
    assert.ok(results.every(result=>result.status==='ok'));assert.equal(f.sent.length,1);
    assert.equal(f.sent[0].referencePrice,7725.63);assert.equal(f.sent[0].riskUsd,50);
    assert.equal(f.sent[0].executionPolicy,'hybrid-market-limit-v1');
    assert.equal(f.sent[0].nativeAgeMs,undefined,'Native age must never become a client execution bypass');
    assert.equal(f.rules.length,1,'Normal trade-rule validation remains mandatory');
  }
});

test('No Mac quote is needed, while provider identity, broker specifications and risk rules remain mandatory',async()=>{
  const good={provider:'mt5-vps',symbol:'USTEC',quoteUpdatedAt:Date.now(),
    quote:{bid:25000,ask:25000.2,nativeAgeMs:5000},
    metadata:{tickSize:.01,quantityStep:.1,minQty:.1,maxQty:10,contractSize:1}};
  for(const change of [value=>value.provider='dwx',value=>value.symbol='SPX500',value=>value.metadata={},
    value=>value.metadata.quantityStep=0,value=>value.metadata.contractSize=0]) {
    const snapshot=structuredClone(good);change(snapshot);
    const f=fixture({getSnapshot:async()=>snapshot});
    assert.equal((await f.service.arm(f.payload)).status,'rejected');assert.equal(f.sent.length,0);
  }
  const f=fixture({getSnapshot:async()=>good,validateTrade:()=>({ok:false,reason:'Risk restriction'})});
  assert.equal((await f.service.arm(f.payload)).status,'rejected');assert.equal(f.sent.length,0);
});

test('BML and SML admit using specifications with absent, stale or invalid Mac display quotes',async()=>{
  for(const quote of [undefined,{}, {bid:0,ask:NaN}, {bid:1,ask:2}])for(const action of ['MB','MS']) {
    let validated;
    const f=fixture({getSnapshot:async()=>({provider:'mt5-vps',symbol:'USTEC',quote,quoteUpdatedAt:0,
      metadata:{tickSize:.01,quantityStep:.1,minQty:.1,maxQty:10,contractSize:1}}),
      validateTrade:(order,displayQuote,options)=>{validated=options;return {ok:true};}});
    assert.equal((await f.service.arm({...f.payload,action})).status,'ok');
    assert.deepEqual(validated,{deferMarketPrice:true});
    assert.equal(f.sent[0].riskUsd,50);assert.equal(f.sent[0].referencePrice,25000);
  }
});

test('Conditional admission defers only the market-price rule, preserving stop and quantity limits',()=>{
  const {buildTradeRules}=require('./host-trade-rules.cjs');
  const rules=buildTradeRules({rules:{maxOrderPriceDeviation:{maxPriceDeviationPct:.5},minStopPoints:{default:6},maxQty:{default:2}}});
  const order={side:'buy',price:25000,sl:16,qty:1};
  assert.equal(rules.validate(order,{}).ok,false);
  assert.equal(rules.validate(order,{}, {deferMarketPrice:true}).ok,true);
  assert.equal(rules.validate({...order,sl:1},{},{deferMarketPrice:true}).ok,false);
  assert.equal(rules.validate({...order,qty:3},{},{deferMarketPrice:true}).ok,false);
});

test('market and breakout directions cannot be mixed or silently defaulted', async () => {
  for (const [action,entryMode] of [['MB',undefined],['BS','side-zone'],['MS','breakout'],['LB','side-zone'],['MB','zone'],['MB','boundary']]) {
    const f = fixture(); assert.equal((await f.service.arm({...f.payload,action,entryMode})).status,'rejected');
    assert.equal(f.sent.length,0);
  }
});

test('zone wire request retains the fixed entry band, complete volume, account scales and one deadline', async () => {
  const f = fixture(); await f.service.arm(f.payload);
  const adapter = new MT5VPSAdapter({enabled:true,account:'900000002',server:'Example-MT5',ustecSymbol:'USTEC',
    timeExit:{enabled:true,time:'15:00'},followers:[{account:'900000001',enabled:true,multiplier:.7}]});
  const request = buildEntryRequest(adapter,f.sent[0],{requestId:'wire-zone'},Date.parse('2026-10-19T12:00:00Z'));
  assert.equal(request.entryMode,'side-zone'); assert.equal(request.zoneRadius,12); assert.equal(request.zoneMultiplier,2);
  assert.equal(request.executionPolicy,'hybrid-market-limit-v1');
  assert.equal(request.maxGap,12);
  assert.equal(request.trigger,25000); assert.equal(request.sl,24984); assert.equal(request.volume,3.1);
  assert.equal(request.chunkVolume,.6); assert.equal(request.chunkPercent,20); assert.equal(request.closeAtUtc,Date.parse('2026-10-19T19:00:00Z'));
  assert.deepEqual(request.mirrorAccounts,[{account:'900000001',multiplier:.7}]);
});

test('BML and SML remain armable outside the band with any unrelated gap setting', async () => {
  for (const action of ['MB','MS']) for (const zoneMultiplier of [1,2]) for (const gap of [0,.01,1000,'invalid']) {
    const f=fixture({getConfig:()=>({defaults:{chunkMaxGap:gap}})});
    assert.equal((await f.service.arm({...f.payload,action,zoneMultiplier})).status,'ok');
    assert.equal(f.sent[0].zoneRadius,6*zoneMultiplier);assert.equal(f.sent[0].maxGap,6*zoneMultiplier);
    assert.equal(f.sent[0].totalQty,3.1);assert.equal(f.sent[0].stopOffsetPts,1600);
    const adapter=new MT5VPSAdapter({enabled:true,account:'900000002',server:'Example-MT5',ustecSymbol:'USTEC',timeExit:{enabled:true,time:'15:00'}});
    const request=buildEntryRequest(adapter,{...f.sent[0],maxGap:0},{requestId:'wire-ignore-gap'},Date.parse('2026-10-19T12:00:00Z'));
    assert.equal(request.maxGap,6*zoneMultiplier);assert.equal(request.trigger,25000);
  }
});

test('an old receiver is rejected before any zone arming POST, and a fresh capability enables only explicit arming', async () => {
  const f = fixture(); await f.service.arm(f.payload);
  const adapter = new MT5VPSAdapter({enabled:true,account:'900000002',server:'Example-MT5',ustecSymbol:'USTEC',timeExit:{enabled:true,time:'15:00'}});
  const calls=[]; let zoneReady=false;
  adapter.request=async (route,payload) => {
    calls.push(route);
    return route==='/health' ? {connected:true,tradingEnabled:true,capabilities:['chunked-breakout','market-zone-entry','market-zone-entry-stop','market-offset-entry',...(zoneReady ? ['market-side-zone-entry','market-limit-side-zone-entry','hybrid-market-limit-v1'] : [])]}
      : {entry:{id:payload.id,entryMode:'side-zone',phase:'armed'}};
  };
  const result=await adapter.armChunkEntry(f.sent[0],{requestId:'old-receiver'});
  assert.equal(result.status,'rejected'); assert.match(result.reason,/приёмник VPS/);
  assert.deepEqual(calls,['/health']);
  zoneReady=true;
  assert.equal((await adapter.armChunkEntry(f.sent[0],{requestId:'new-receiver'})).status,'ok');
  assert.deepEqual(calls,['/health','/health','/entry-plan']);
});

test('v33 side-zone capability cannot silently accept a new market/limit plan', async () => {
  const f=fixture(); await f.service.arm(f.payload);
  const adapter=new MT5VPSAdapter({enabled:true,account:'900000002',server:'Example-MT5',ustecSymbol:'USTEC',timeExit:{enabled:true,time:'15:00'}});
  const calls=[];
  adapter.request=async route=>{calls.push(route);return {connected:true,tradingEnabled:true,capabilities:['chunked-breakout','market-zone-entry','market-zone-entry-stop','market-side-zone-entry']};};
  const result=await adapter.armChunkEntry(f.sent[0],{requestId:'no-hybrid-capability'});
  assert.equal(result.status,'rejected'); assert.match(result.reason,/общим стопом/);assert.deepEqual(calls,['/health']);
});

test('v34 market/limit capability cannot silently accept percentage hybrid entry with a common stop', async () => {
  const f=fixture(); await f.service.arm(f.payload);
  const adapter=new MT5VPSAdapter({enabled:true,account:'900000002',server:'Example-MT5',ustecSymbol:'USTEC',timeExit:{enabled:true,time:'15:00'}});
  const calls=[];
  adapter.request=async route=>{calls.push(route);return {connected:true,tradingEnabled:true,
    capabilities:['chunked-breakout','market-zone-entry','market-zone-entry-stop','market-side-zone-entry','market-limit-side-zone-entry']};};
  const result=await adapter.armChunkEntry(f.sent[0],{requestId:'no-common-stop-capability'});
  assert.equal(result.status,'rejected');assert.match(result.reason,/общим стопом/);assert.deepEqual(calls,['/health']);
});

test('BML/SML use configured 25 percent and never trust a forged percent from a renderer payload', async () => {
  const f=fixture({getConfig:()=>({defaults:{chunkPercent:30},symbols:[{ticker:'ustec',chunkPercent:25}]})});
  assert.equal((await f.service.arm({...f.payload,chunkPercent:100})).status,'ok');
  assert.equal(f.sent[0].chunkPercent,25);assert.equal(f.sent[0].chunkVolume,.7);assert.equal(f.sent[0].totalQty,3.1);
});

test('hybrid wire validation requires the bounded percentage and explicit shared-stop inputs', async () => {
  const f=fixture();await f.service.arm(f.payload);
  const adapter=new MT5VPSAdapter({enabled:true,account:'900000002',server:'Example-MT5',ustecSymbol:'USTEC',timeExit:{enabled:true,time:'15:00'}});
  const value=f.sent[0];
  for (const overrides of [{chunkPercent:undefined},{chunkPercent:0},{chunkPercent:-1},{chunkPercent:100.01},
    {stopMode:undefined},{stopOffsetPts:undefined},{executionPolicy:'hybrid-market-limit-v2'}])
    assert.throws(()=>buildEntryRequest(adapter,{...value,...overrides},{requestId:'invalid-hybrid'}));
  const legacy=buildEntryRequest(adapter,{...value,executionPolicy:'market-limit',chunkPercent:100,chunkVolume:value.totalQty},{requestId:'legacy-saved-shape'});
  assert.equal(legacy.executionPolicy,'market-limit');assert.equal(legacy.chunkPercent,100);
});

test('every positive configured percentage, including fractions and 100%, survives admission and wire construction', async () => {
  const adapter=new MT5VPSAdapter({enabled:true,account:'900000002',server:'Example-MT5',ustecSymbol:'USTEC',timeExit:{enabled:true,time:'15:00'}});
  for (const [chunkPercent,volume] of [[1,.1],[10,.1],[20,.1],[50,.3],[100,.7],[.5,.1],[.01,.1],[.001,.1]]) {
    const f=fixture({getConfig:()=>({defaults:{chunkPercent}})});
    assert.equal((await f.service.arm({...f.payload,riskUsd:12.5,chunkPercent:99})).status,'ok');
    assert.equal(f.sent[0].chunkPercent,chunkPercent);assert.equal(f.sent[0].chunkVolume,volume);
    assert.equal(f.sent[0].totalQty,.7);
    const request=buildEntryRequest(adapter,f.sent[0],{requestId:'any-percent-'+chunkPercent});
    assert.equal(request.chunkPercent,chunkPercent);assert.equal(request.chunkVolume,volume);assert.equal(request.volume,.7);
  }
});

test('hybrid wire rejects zero, negative, over-100 and non-finite percentages before sending', async () => {
  const f=fixture();await f.service.arm(f.payload);
  const adapter=new MT5VPSAdapter({enabled:true,account:'900000002',server:'Example-MT5',ustecSymbol:'USTEC',timeExit:{enabled:true,time:'15:00'}});
  const requests=[];adapter.request=async(...args)=>{requests.push(args);throw Error('must not send');};
  for (const chunkPercent of [0,-.01,100.01,NaN,Infinity,-Infinity,'bad']) {
    const result=await adapter.armChunkEntry({...f.sent[0],chunkPercent});
    assert.equal(result.status,'rejected');
  }
  assert.equal(requests.length,0);
});

test('percentage settings allow arbitrary decimal input without an artificial minimum or step', () => {
  const descriptor=require('../payload/src/app/services/levelOrder/config/level-order-settings-descriptor.json');
  for (const option of [descriptor.options.defaults.chunkPercent,descriptor.options.symbols.item.chunkPercent]) {
    assert.equal(option.type,'number');assert.equal(option.step,'any');assert.equal(option.max,100);
    assert.equal(option.min,undefined);assert.match(option.description,/дробные/);
  }
});

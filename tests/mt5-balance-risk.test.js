const test=require('node:test');
const assert=require('node:assert/strict');
const {followers}=require('../payload/src/app/services/mt5-vps/sizing');
const {buildEntryRequest,armChunkEntry}=require('../payload/src/app/services/mt5-vps/chunkEntryClient');
const admission=require('../payload/src/app/services/mt5-vps/entryAdmission');
const config={account:'900000002',followers:[{account:'900000001',multiplier:1.6,enabled:true},
  {account:'900000003',multiplier:1,riskPercent:.1,enabled:true}],timeExit:{enabled:true,time:'15:59'}};
const plan={ticker:'USTEC',orderSide:'buy',referencePrice:27000,stopPrice:26984,takeProfitPts:9000,tickSize:.01,
  totalQty:.8,chunkVolume:.1,maxGap:2,riskUsd:13,level:27000,stopMode:'entry',stopOffsetPts:1600,
  entryMode:'side-zone',zoneRadius:2,zoneMultiplier:1,executionPolicy:'hybrid-market-limit-v1',chunkPercent:20};
test('New account captures an independent balance policy and retains existing multiplier',()=>{
  assert.deepEqual(followers(config),config.followers);
  const adapter={config,capabilities:['independent-account-engines-v5','entry-initial-sizing-only-v1','entry-balance-risk-v1','atomic-entry-admission-v1'],brokerSymbol:x=>x};
  const result=buildEntryRequest(adapter,plan);
  assert.deepEqual(result.mirrorAccounts,[{account:'900000001',multiplier:1.6},{account:'900000003',multiplier:1,riskPercent:.1}]);
  assert.ok(result.requiredCapabilities.includes('entry-balance-risk-v1'));
  assert.throws(()=>admission.validateHealth({capabilities:['chunked-breakout']},'arm',result));
});
test('No legacy fallback silently turns the percent into a multiplier',()=>{
  const adapter={config,capabilities:[],brokerSymbol:x=>x};
  assert.throws(()=>buildEntryRequest(adapter,plan),/обновите VPS/);
  adapter.capabilities=['independent-account-engines-v5','entry-initial-sizing-only-v1','entry-balance-risk-v1'];
  assert.equal(buildEntryRequest(adapter,{...plan,stopMode:'level'}).stopMode,'level');
  for(const riskPercent of [0,-.1,101,NaN,Infinity])assert.throws(()=>followers({...config,followers:[{account:'900000003',multiplier:1,riskPercent}]}));
});

test('An independent start accepts the durable intent while the primary size is still pending',async()=>{
  const primaryRiskRequest={account:config.account,percent:.1};
  const nativePlan={...plan,primaryRiskRequest};
  const adapter={config:{...config,followers:config.followers.filter(row=>row.riskPercent)},brokerSymbol:x=>x};
  const required=admission.requiredCapabilities({...plan,primaryRiskRequest,
    accountExecution:'independent-v5',mirrorAccounts:adapter.config.followers,accountStart:admission.ACCOUNT_START_CAPABILITY});
  adapter.capabilities=[...required,admission.CAPABILITY];
  const request=buildEntryRequest(adapter,nativePlan);
  assert.equal(request.accountStart,admission.ACCOUNT_START_CAPABILITY);
  assert.equal(buildEntryRequest({...adapter,config},nativePlan).accountStart,undefined);
  adapter.request=async(route,payload)=>{
    assert.equal(route,'/entry-plan-admit');
    const {id,symbol,side}=payload;
    const checking=percent=>({multiplier:1,riskPercent:percent,targetVolume:0,riskUsd:0,filledVolume:0,entryStatus:'checking'});
    const entry={...payload,id,symbol,side,phase:'armed',volume:0,chunkVolume:0,riskUsd:0,
      accounts:{900000002:checking(.1),900000003:checking(.1)},lanePhases:{900000002:'armed',900000003:'armed'},children:[]};
    return {account:config.account,version:39,capabilities:adapter.capabilities,admissionContract:admission.CAPABILITY,entry};
  };
  assert.equal((await armChunkEntry(adapter,nativePlan,{requestId:'separate-accounts'})).status,'ok');
});

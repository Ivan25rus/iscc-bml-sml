const test=require('node:test');
const assert=require('node:assert/strict');
const {followers}=require('../payload/src/app/services/mt5-vps/sizing');
const {buildEntryRequest}=require('../payload/src/app/services/mt5-vps/chunkEntryClient');
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

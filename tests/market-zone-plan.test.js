const {test} = require('node:test');
const assert = require('node:assert/strict');
const {calculateMarketZoneTradePlan,resolveMarketChunkPercent} = require('../payload/src/app/services/levelOrder/marketZone');

function plan(overrides = {}) {
  return calculateMarketZoneTradePlan({action:'MB', ticker:'USTEC', instrumentType:'FX',
    level:100, entryPrice:99, maxOffset:2, zoneMultiplier:1, entryMode:'zone', riskUsd:20,
    stopOffsetPts:100, stopMode:'entry', tickSize:.1, minLot:.1, minQty:.1, maxQty:100,
    lot:1, orderCalculator:{qty:({riskUsd, stopPts, tickSize, lot}) => riskUsd / (stopPts * tickSize * lot)}, ...overrides});
}

test('entry stops preserve the ordinary limit quantity on either side of LT', () => {
  for (const [action, entryPrice, lower, upper, stopPrice, worstPrice] of [
    ['MB',99,97,101,89,101], ['MS',101,99,103,111,99]]) {
    const value = plan({action, entryPrice});
    assert.equal(value.ok,true);
    assert.equal(value.orderType,'market');
    assert.equal(value.zoneLower,lower); assert.equal(value.zoneUpper,upper);
    assert.equal(value.stopPrice,stopPrice); assert.equal(value.worstPrice,worstPrice);
    assert.equal(value.totalQty,2); assert.equal(value.chunkVolume,value.totalQty);
    assert.equal(value.chunkPercent,100);
    assert.ok(value.totalQty * value.stopPts * value.tickSize <= value.riskUsd);
  }
});

test('double play expands around the selected entry, while quantity keeps the same risk budget', () => {
  const value = plan({zoneMultiplier:2});
  assert.equal(value.ok,true); assert.equal(value.referencePrice,99);
  assert.equal(value.zoneLower,95); assert.equal(value.zoneUpper,103); assert.equal(value.zoneRadius,4);
  assert.equal(value.totalQty,2); assert.equal(value.riskUsd,20);
  assert.equal(value.stopPts,100);
});

test('fixed level stop and broker/order caps split only the full risk-sized target', () => {
  const value = plan({stopMode:'level',maxQty:.5,maxLot:.4});
  assert.equal(value.ok,true); assert.equal(value.stopPrice,90);
  assert.equal(value.totalQty,1.8); assert.equal(value.chunkVolume,.4);
});

test('a zero-width zone remains an exact-price entry', () => {
  const value = plan({maxOffset:0});
  assert.equal(value.ok,true); assert.equal(value.zoneLower,99); assert.equal(value.zoneUpper,99);
});

test('invalid zones, off-tick protection, unsafe boundaries and unusable lots never produce a plan', () => {
  for (const overrides of [
    {action:'LB'}, {zoneMultiplier:3}, {zoneMultiplier:NaN}, {maxOffset:-1}, {maxOffset:NaN},
    {entryPrice:0}, {entryPrice:null}, {entryPrice:99.01}, {tickSize:0}, {maxQty:undefined},
    {stopMode:'bad'}, {stopMode:'level',stopOffsetPts:1}, {stopOffsetPts:100.01}, {takeProfitPts:1},
    {takeProfitPts:-1}, {takeProfitPts:NaN}, {riskUsd:.01}, {maxLot:.01}, {maxLot:-1},
    {minQty:3}, {maxQty:.01}, {lot:0}, {maxOffset:1000}, {stopMode:'level',level:1000}
  ]) {
    assert.equal(plan(overrides).ok,false,JSON.stringify(overrides));
  }
});

test('NASDAQ stop 16 and play 6 or 12 keep the same standard 0.7 lot at risk 12.5', () => {
  for (const action of ['MB','MS']) for (const zoneMultiplier of [1,2]) {
    const value=plan({action,entryMode:'side-zone',level:29611.2,entryPrice:29611.2,maxOffset:6,zoneMultiplier,
      riskUsd:12.5,stopOffsetPts:1600,tickSize:.01});
    assert.equal(value.ok,true);assert.equal(value.totalQty,.7);assert.equal(value.stopPts,1600);
  }
});

test('BML includes LT through LT plus play and SML includes LT minus play through LT', () => {
  for (const zoneMultiplier of [1,2]) for (const action of ['MB','MS']) {
    const buy=action==='MB';
    const value=plan({action,entryMode:'side-zone',level:29477.42,entryPrice:1,maxOffset:6,zoneMultiplier,
      maxGap:2,riskUsd:2,stopOffsetPts:1600,tickSize:.01});
    const trigger=29477.42;
    assert.equal(value.ok,true);assert.equal(value.referencePrice,trigger);assert.equal(value.entryMode,'side-zone');
    assert.equal(value.totalQty,.1);assert.equal(value.maxGap,6*zoneMultiplier);
    assert.equal(value.zoneLower,Number((trigger-(buy ? 0 : 6*zoneMultiplier)).toFixed(2)));
    assert.equal(value.zoneUpper,Number((trigger+(buy ? 6*zoneMultiplier : 0)).toFixed(2)));
    assert.equal(value.stopPrice,Number((trigger+(buy ? -16 : 16)).toFixed(2)));
  }
});

test('fixed LT protection sizes by the adverse inclusive edge and never extends by the execution gap', () => {
  for (const action of ['MB','MS']) {
    const value=plan({action,entryMode:'side-zone',entryPrice:100,stopMode:'level',zoneMultiplier:2,maxGap:2});
    assert.equal(value.ok,true);assert.equal(value.stopPts,140);assert.equal(value.totalQty,1.4);
    assert.equal(value.worstPrice,action==='MB' ? 104 : 96);
  }
});

test('BML and SML plans depend on their captured play, never the separate gap setting', () => {
  for (const action of ['MB','MS']) for (const zoneMultiplier of [1,2]) {
    const expected=plan({action,entryMode:'side-zone',entryPrice:100,zoneMultiplier});
    for (const maxGap of [0,.01,1000,-1,NaN,Infinity,'invalid'])
      assert.deepEqual(plan({action,entryMode:'side-zone',entryPrice:100,zoneMultiplier,maxGap}),expected);
  }
});

test('new BML/SML split the original target into 30 percent parts or configured 25 percent without raising the total', () => {
  for (const action of ['MB','MS']) {
    const value=plan({action,entryMode:'side-zone',entryPrice:100});
    assert.equal(value.ok,true);assert.equal(value.executionPolicy,'hybrid-market-limit-v1');
    assert.equal(value.totalQty,2);assert.equal(value.chunkPercent,30);assert.equal(value.chunkVolume,.6);
    const quarter=plan({action,entryMode:'side-zone',entryPrice:100,chunkPercent:25});
    assert.equal(quarter.totalQty,2);assert.equal(quarter.chunkPercent,25);assert.equal(quarter.chunkVolume,.5);
    const capped=plan({action,entryMode:'side-zone',entryPrice:100,maxLot:.2});
    assert.equal(capped.totalQty,2);assert.equal(capped.chunkVolume,.2);
  }
});

test('market chunk defaults preserve every valid percentage and only replace absent or invalid values', () => {
  for (const chunkPercent of [undefined,0,-1,100.01,'invalid',NaN,Infinity])
    assert.equal(resolveMarketChunkPercent({defaults:{chunkPercent}},'USTEC'),30);
  for (const chunkPercent of [1,10,20,25,30,50,100,.5,.01])
    assert.equal(resolveMarketChunkPercent({defaults:{chunkPercent}},'USTEC'),chunkPercent);
  assert.equal(resolveMarketChunkPercent({defaults:{chunkPercent:25}},'USTEC'),25);
  assert.equal(resolveMarketChunkPercent({defaults:{chunkPercent:30},symbols:[{ticker:'ustec',chunkPercent:25}]},'USTEC'),25);
  assert.equal(resolveMarketChunkPercent({defaults:{chunkPercent:25},symbols:[{ticker:'SPX500',chunkPercent:30}]},'USTEC'),25);
  assert.equal(resolveMarketChunkPercent({defaults:{chunkPercent:30},symbols:[{ticker:'ustec',chunkPercent:.01}]},'USTEC'),.01);
});

test('minimum lot may require one larger part, but percentage rounding never enlarges the risk-sized target', () => {
  const small=plan({entryMode:'side-zone',entryPrice:100,riskUsd:1});
  assert.equal(small.ok,true);assert.equal(small.totalQty,.1);assert.equal(small.chunkVolume,.1);
  const nasdaq=plan({entryMode:'side-zone',level:29611.2,entryPrice:29611.2,maxOffset:6,
    riskUsd:12.5,stopOffsetPts:1600,tickSize:.01});
  assert.equal(nasdaq.ok,true);assert.equal(nasdaq.totalQty,.7);assert.equal(nasdaq.chunkVolume,.2);
  assert.ok(nasdaq.totalQty*nasdaq.stopPts*nasdaq.tickSize<=nasdaq.riskUsd);
  for (const chunkPercent of [0,-1,100.01,NaN,Infinity,'bad',null,''])
    assert.equal(plan({entryMode:'side-zone',entryPrice:100,chunkPercent}).ok,false);
});

test('arbitrary positive percentages produce correctly sized feasible parts with broker minimums', () => {
  for (const action of ['MB','MS']) for (const [chunkPercent,volume] of [[1,.1],[10,.2],[20,.4],[50,1],[100,2],[.5,.1],[.01,.1],[.001,.1]]) {
    const value=plan({action,entryMode:'side-zone',entryPrice:100,chunkPercent});
    assert.equal(value.ok,true,`${action} ${chunkPercent}: ${value.reason}`);
    assert.equal(value.chunkPercent,chunkPercent);assert.equal(value.totalQty,2);assert.equal(value.chunkVolume,volume);
    assert.ok(value.totalQty*value.stopPts*value.tickSize<=value.riskUsd);
  }
});

test('one percent can produce one hundred market parts while larger resource plans reject without rewriting the percentage', () => {
  const config={defaults:{chunkPercent:1}};
  assert.equal(resolveMarketChunkPercent(config,'USTEC'),1);
  const value=plan({entryMode:'side-zone',entryPrice:100,riskUsd:2000,maxQty:1000,chunkPercent:1});
  assert.equal(value.ok,true);assert.equal(value.chunkPercent,1);assert.equal(value.totalQty,200);assert.equal(value.chunkVolume,2);
  assert.equal(Math.ceil(value.totalQty/value.chunkVolume),100);
  const tooMany=plan({entryMode:'side-zone',entryPrice:100,riskUsd:2000,maxQty:1000,chunkPercent:.5});
  assert.equal(tooMany.ok,false);assert.match(tooMany.reason,/больше 100 частей/);
  const representable=plan({entryMode:'side-zone',entryPrice:100,riskUsd:2000,maxQty:1000,chunkPercent:100});
  assert.equal(representable.ok,true);assert.equal(representable.chunkPercent,100);assert.equal(representable.totalQty,200);
});

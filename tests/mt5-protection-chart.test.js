const test=require('node:test');
const assert=require('node:assert/strict');
const {snapPrice,chartScale,protectionScale,priceAt,zoomScale}=require('../payload/src/app/services/mt5-vps/protectionChart');
test('Chart drafts snap to the broker tick and never turn invalid or sub-tick input into removal',()=>{
  assert.equal(snapPrice(25001.236,.01),25001.24);
  assert.equal(snapPrice(25001.2,.25),25001.25);
  assert.equal(snapPrice(1.23456789,.00001),1.23457);
  for(const [value,step] of [[0,.01],[-1,.01],[NaN,.01],[Infinity,.01],[100,0],[100,NaN],[.001,.01]])assert.equal(snapPrice(value,step),null);
});
test('Both buy and sell protection ranges include entry, price and real levels; removed levels do not distort the scale',()=>{
  for(const prices of [[25000,25002,24984,25048],[25000,24998,25016,24952]]){
    const scale=chartScale(prices,.01);assert.ok(scale.low<Math.min(...prices));assert.ok(scale.high>Math.max(...prices));
  }
  assert.deepEqual(chartScale([100,0,null,undefined,NaN,Infinity],.01),chartScale([100],.01));
  assert.equal(chartScale([null,0,NaN],.01),null);
});
test('Pointer mapping is monotonic, bounded and aligned to tick size instead of allowing a zero SL/TP by dragging outside',()=>{
  const scale={low:99,high:101};
  assert.equal(priceAt(14,scale,.01),101);assert.equal(priceAt(69,scale,.01),100);assert.equal(priceAt(124,scale,.01),99);
  assert.equal(priceAt(-1000,scale,.01),101);assert.equal(priceAt(1000,scale,.01),99);
  assert.equal(priceAt(NaN,scale,.01),null);assert.equal(priceAt(40,null,.01),null);assert.equal(priceAt(40,scale,0),null);
});
test('SL stays precise while TP has directional room beyond the existing take for both buy and sell',()=>{
  for(const [side,sl,tp,current] of [['buy',24986,25042,25003],['sell',25014,24958,24997]]){
    const model={entry:25000,current,sl,tp,side,tickSize:.01},before=JSON.stringify(model);
    const stop=protectionScale(model,'sl'),take=protectionScale(model,'tp');
    assert.ok(Math.abs((stop.high-stop.low)-72.8)<1e-8);
    assert.ok(take.high-take.low>3*(stop.high-stop.low));
    assert.ok(side==='buy'?take.high>tp+100:take.low<tp-100);
    assert.ok(stop.low<sl && stop.high>sl);
    assert.equal(JSON.stringify(model),before,'Changing scale must not edit either broker level');
  }
});
test('Manual zoom expands or tightens only the viewport; a missing TP still leaves room to set one',()=>{
  const model={entry:25000,current:25003,sl:24986,tp:0,side:'buy',tickSize:.01};
  const normal=protectionScale(model,'tp'),wide=protectionScale(model,'tp',2),tight=protectionScale(model,'tp',.5);
  assert.ok(normal.high>25100);assert.equal(model.tp,0);
  assert.ok(Math.abs((wide.high-wide.low)/(normal.high-normal.low)-2)<1e-8);
  assert.ok(Math.abs((tight.high-tight.low)/(normal.high-normal.low)-.5)<1e-8);
  assert.deepEqual(protectionScale(model,'tp',NaN),normal);
  const nearZero=protectionScale({...model,entry:1,current:1,sl:.9,side:'sell'},'tp',10000);
  assert.ok(nearZero.low>=0 && nearZero.high>nearZero.low && Number.isFinite(nearZero.high));
});
test('SL Auto fits actual bars and current price without flattening them for distant entry, stop or take',()=>{
  const bars=Array.from({length:30},(_,i)=>[i*60,25000,25004,24998,25002]);
  for(const side of ['buy','sell']){
    const model={entry:24000,current:25003,sl:side==='buy'?23000:26000,tp:side==='buy'?28000:22000,side,tickSize:.01,bars};
    const before=JSON.stringify(model),scale=protectionScale(model,'sl');
    assert.ok(scale.low<24998 && scale.high>25004);
    assert.ok(6/(scale.high-scale.low)>.75,'Bars occupy most of the vertical price range');
    assert.deepEqual(protectionScale({...model,sl:side==='buy'?10000:40000,tp:50000},'sl'),scale);
    const level=protectionScale(model,'sl',1,'level');
    assert.ok(level.low<model.sl && level.high>model.sl,'Off-scale SL remains reachable in its level view');
    assert.ok(protectionScale(model,'tp').high-protectionScale(model,'tp').low>scale.high-scale.low);
    assert.equal(JSON.stringify(model),before,'Viewport never changes broker protection');
  }
});
test('SL bar framing keeps flat and near-zero histories finite and retains manual zoom and live-price visibility',()=>{
  const model={entry:25000,current:25000,sl:24984,tp:25102,side:'buy',tickSize:.01,bars:[[0,25000,25000,25000,25000]]};
  const scale=protectionScale(model,'sl'),wide=protectionScale(model,'sl',2);
  assert.ok(scale.high>scale.low && scale.high-scale.low<1);
  assert.ok(Math.abs((wide.high-wide.low)/(scale.high-scale.low)-2)<1e-8);
  const moved=protectionScale({...model,current:25010},'sl');assert.ok(moved.high>25010);
  const nearZero=protectionScale({...model,current:.001,tickSize:.01,bars:[[0,.001,.001,.001,.001]]},'sl');
  assert.ok(nearZero.low>=0 && nearZero.high>nearZero.low && Number.isFinite(nearZero.high));
  assert.deepEqual(protectionScale({...model,bars:[[0,0,null,NaN,0]]},'sl'),protectionScale({...model,bars:[]},'sl'));
});
test('Smooth zoom keeps the price under the pointer anchored and respects positive bounded ranges',()=>{
  const original={low:100,high:200},zoomed=zoomScale(original,.8,125,1,1000);
  assert.equal((125-zoomed.low)/(zoomed.high-zoomed.low),.25);
  assert.equal(zoomed.high-zoomed.low,80);
  assert.deepEqual(zoomScale(zoomed,1.25,125,1,1000),original);
  const zero=zoomScale({low:.01,high:.1},20,.02,.01,1);assert.ok(zero.low>=0 && zero.high-zero.low<=1);
  assert.equal(zoomScale(original,0,125),original);
});

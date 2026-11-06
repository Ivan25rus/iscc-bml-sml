const test=require('node:test'),assert=require('node:assert/strict');
const {createMinuteHistory}=require('../payload/src/app/services/mt5-vps/minuteHistory');
const settled=()=>new Promise(resolve=>setImmediate(resolve));
const data={capturedAt:1,bars:[[60,100,102,99,101]]};
test('Chart history joins one in-flight read and caches success for the current minute',async()=>{
  let clock=10000,calls=0,finish;
  const cache=createMinuteHistory({now:()=>clock,load:()=>{calls++;return new Promise(resolve=>finish=resolve);}});
  assert.ok(cache.get('USTEC').loading);cache.get('USTEC');await settled();assert.equal(calls,1);
  finish({status:'ok',data});await settled();assert.equal(cache.get('USTEC').data,data);assert.equal(calls,1);
  clock=60000;cache.get('USTEC');await settled();assert.equal(calls,2);
  cache.close();finish({status:'ok',data});await settled();assert.equal(cache.get('USTEC').data,null);
});
test('Unavailable M1 gets a visible reason and bounded retries without tight loops or a new polling timer',async()=>{
  let clock=10000,calls=0;
  const cache=createMinuteHistory({now:()=>clock,load:async()=>{calls++;return {status:'rejected',reason:'history timeout'};}});
  for(let i=0;i<3;i++){
    cache.get('USTEC');await settled();assert.equal(cache.get('USTEC').error,'history timeout');
    for(let j=0;j<100;j++)cache.get('USTEC');await settled();assert.equal(calls,i+1);clock+=2000;
  }
  cache.get('USTEC');await settled();assert.equal(calls,3);
  cache.retry('USTEC');await settled();assert.equal(calls,4,'An explicit retry is allowed');
  cache.retry('USTEC');await settled();assert.equal(calls,4,'Repeated clicks remain throttled');
  clock=60000;cache.get('USTEC');await settled();assert.equal(calls,5);
});
test('Missing protocol and empty history never become a successful price-line substitute',async()=>{
  let calls=0;
  const cache=createMinuteHistory({load:async()=>{calls++;return {status:'ok',data:{bars:[]}};}});
  assert.match(cache.get('USTEC',{enabled:false}).error,/недоступны/);assert.equal(calls,0);
  cache.get('USTEC');await settled();assert.equal(cache.get('USTEC').data,null);assert.match(cache.get('USTEC').error,/не вернул/);
});

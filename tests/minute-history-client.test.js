const test=require('node:test'),assert=require('node:assert/strict');
const {EventEmitter}=require('node:events');
const {MT5VPSAdapter}=require('../payload/src/app/services/mt5-vps/client');
const config={account:'900000002',server:'broker',spxSymbol:'SPX500',ustecSymbol:'USTEC'};
function history(){return {account:900000002,server:'broker',symbol:'USTEC',timeframe:'M1',basis:'bid',capturedAt:Date.now(),
  bars:[[Math.floor(Date.now()/60000)*60,100,102,99,101]]};}
test('Native minute history keeps identity/OHLC/time checks and grants a longer deadline only to its read',async t=>{
  const a=new MT5VPSAdapter(config);t.after(()=>a.close());a.capabilities=['minute-chart-bars-v1'];const calls=[];
  a.request=async(...args)=>{calls.push(args);return history();};
  assert.equal((await a.minuteBars('USTEC')).bars.length,1);await a.minuteBars('USTEC');assert.equal(calls.length,1);
  assert.equal(calls[0][0],'/minute-bars?symbol=USTEC&limit=30');assert.equal(calls[0][1],undefined);assert.deepEqual(calls[0][2],{readTimeoutMs:7500});
  for(const mutate of [x=>x.account=900000001,x=>x.bars[0][2]=98,x=>x.bars[0][0]+=3600,x=>x.capturedAt-=61000]){
    a.minuteBarsCache.clear();a.request=async()=>{const x=history();mutate(x);return x;};
    await assert.rejects(a.minuteBars('USTEC'));assert.equal(a.minuteBarsCache.size,0);
  }
});
test('A failed optional chart read uses a separate bounded channel and cannot declare a healthy trade connection lost',async t=>{
  const https=require('https'),a=new MT5VPSAdapter({...config,enabled:true,endpoint:'https://127.0.0.1:19443',token:'fixture',caFile:__filename});
  t.after(()=>a.close());let options;
  t.mock.method(https,'request',(_url,selected)=>{
    options=selected;const request=new EventEmitter();request.end=()=>process.nextTick(()=>request.emit('error',Error('history unavailable')));
    request.destroy=()=>request.destroyed=true;return request;
  });
  const status={connected:true,message:'confirmed health'};a.lastStatus=status;
  await assert.rejects(a.request('/minute-bars?symbol=USTEC&limit=30',undefined,{readTimeoutMs:7500}),/history unavailable/);
  assert.equal(a.lastStatus,status);assert.equal(options.agent,a.agents.chartHistory);assert.equal(options.agent.maxSockets,1);
  assert.notEqual(options.agent,a.agents.commands);assert.notEqual(options.agent,a.agents.execution);
  await assert.rejects(a.request('/place',{symbol:'USTEC'},{readTimeoutMs:7500}),/read-only/);
});

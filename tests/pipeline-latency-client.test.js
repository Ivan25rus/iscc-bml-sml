const test=require('node:test'),assert=require('node:assert/strict');
const {EventEmitter}=require('node:events');
const {MT5VPSAdapter}=require('../payload/src/app/services/mt5-vps/client');
test('Entry event waits own a bounded socket pool and leave state and execution channels free',async t=>{
  const https=require('https'),calls=[];
  const adapter=new MT5VPSAdapter({enabled:true,endpoint:'https://127.0.0.1:19443',token:'fixture',caFile:__filename,account:900000002,server:'broker'});
  t.after(()=>adapter.close());
  t.mock.method(https,'request',(url,options)=>{
    calls.push({route:url.pathname,options});const request=new EventEmitter();
    request.end=()=>process.nextTick(()=>request.emit('error',Error('fixture read stopped')));
    request.destroy=()=>request.destroyed=true;return request;
  });
  for(const route of ['/entry-events?since=0','/management?view=active','/snapshot?symbol=USTEC'])await assert.rejects(adapter.request(route));
  assert.equal(calls[0].options.agent,adapter.agents.entryEvents);
  assert.equal(adapter.agents.entryEvents.maxSockets,1);
  assert.notEqual(adapter.agents.entryEvents,adapter.agents.commands);
  assert.notEqual(adapter.agents.entryEvents,adapter.agents.execution);
  assert.equal(calls[1].options.agent,adapter.agents.commands);assert.equal(calls[2].options.agent,adapter.agents.commands);
  assert.ok(calls.every(row=>row.options.method==='GET'));
});

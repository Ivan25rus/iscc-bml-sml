const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('fs'),path=require('path'),vm=require('vm'),os=require('os');
const {EventEmitter}=require('events');

test('Hybrid protection and complete close use the primary coordinator even when the follower is selected',async()=>{
  const handlers={},calls=[],app=new EventEmitter(),module={exports:{}};
  let finalEntry;
  app.getPath=()=>os.tmpdir();
  const config={enabled:true,account:900000002,server:'Example-MT5',followers:[{account:'900000001',multiplier:1,enabled:true}]};
  class Connection extends EventEmitter {
    constructor(config){super();this.config=config;this.inFlight=new Set();}
    modifyEntryProtection(target,changes,options){calls.push({kind:'parent-protection',route:this.config.routePrefix,target,changes,options});return {status:'ok',entry:finalEntry};}
    closeEntryPosition(entry,options){calls.push({kind:'parent-close',route:this.config.routePrefix,entry,options});return {status:'ok',entry:finalEntry};}
    modifyOrder(target,changes){calls.push({kind:'ordinary-modify',route:this.config.routePrefix,target,changes});return {status:'ok'};}
    close(){}
  }
  const stubs={
    '../settings':{register(){},onApply(){}},'../../config/load':()=>config,'./client':{MT5VPSAdapter:Connection},
    './commandJournal':{CommandJournal:class{constructor(){this.records=[];}}},'./sizing':require('../payload/src/app/services/mt5-vps/sizing'),
    './runtimeActivity':{createRuntimeActivity:()=>({start(){},stop(){}})},
    './accountEnrollment':{createAccountEnrollment:()=>({busy:false})},
    './tunnelRecovery':{createTunnelRecovery:()=>({})},'../events':new EventEmitter(),
    electron:{app,ipcMain:{handle:(name,handler)=>handlers[name]=handler}},
    '../brokerage/brokerageAdapters':{},timers:{setInterval:()=>({unref(){}}),clearInterval(){}}
  };
  const file=path.resolve(__dirname,'../reference/mt5-service-manifest.js.txt');
  vm.runInNewContext(fs.readFileSync(file,'utf8'),{module,__dirname:path.dirname(file),require:name=>name in stubs?stubs[name]:require(name)},{filename:file});
  const api={};module.exports.initService(api);
  const target={ticket:'71',symbol:'USTEC',__isPosition:true,singleAccount:true,entryPlan:{id:'parent-a'},clickAt:123};
  const manage=handlers['mt5-vps:manage'];
  await manage(null,{action:'entry-protection',account:'900000001',target,operation:'breakeven',changes:{tp:25050}});
  assert.equal(calls[0].kind,'parent-protection');assert.equal(calls[0].route,undefined);
  assert.equal(calls[0].options.account,'900000001');assert.equal(calls[0].options.operation,'breakeven');
  assert.equal(calls[0].target.singleAccount,true);assert.equal(calls[0].changes.tp,25050);
  await manage(null,{action:'modify',account:'900000001',target,changes:{sl:25002}});
  assert.equal(calls[1].kind,'parent-protection');assert.equal(calls[1].route,undefined);
  await manage(null,{action:'entry-close',account:'900000001',target});
  assert.equal(calls[2].kind,'parent-close');assert.equal(calls[2].route,undefined);
  assert.equal(calls[2].entry.id,'parent-a');assert.equal(calls[2].entry.symbol,'USTEC');assert.equal(calls[2].options.clickAt,123);
  await manage(null,{action:'modify',account:'900000001',target:{...target,entryPlan:undefined},changes:{sl:24990}});
  assert.equal(calls[3].kind,'ordinary-modify');assert.equal(calls[3].route,'/accounts/900000001');
  await assert.rejects(manage(null,{action:'entry-protection',account:'999999',target}),/не настроен/);
  assert.equal(calls.length,4);
  finalEntry={id:'parent-a',phase:'completed'};
  const delivered=[];
  await manage({sender:{send:(channel,result)=>delivered.push({channel,result})}},{action:'entry-protection',account:'900000001',target,changes:{sl:25001}});
  assert.equal(delivered[0].channel,'mt5-vps:entry-control');assert.equal(delivered[0].result.entry,finalEntry);
  const result=await manage({sender:{send(){throw Error('Synthetic closed renderer');}}},{action:'entry-close',account:'900000001',target});
  assert.equal(result.status,'ok');assert.equal(result.entry,finalEntry);
  app.emit('before-quit');
});

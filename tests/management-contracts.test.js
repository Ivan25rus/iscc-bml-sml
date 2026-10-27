const test=require('node:test');
const assert=require('node:assert/strict');
const {closeVolumes}=require('../payload/src/app/services/mt5-vps/sizing');
const {createAccountEnrollment}=require('../payload/src/app/services/mt5-vps/accountEnrollment');

test('a linked percent uses each actual position and rejects an invalid remainder',()=>{
  const row={__isPosition:true,lots:2,quantityStep:.1,minQty:.1,mirrorAccount:'900000002',
    linkedPositions:[{account:'900000002',lots:1.4,quantityStep:.1,minQty:.1}]};
  assert.deepEqual(closeVolumes(row,'900000001',true,'percent',50).map(x=>x.volume),[1,.7]);
  row.linkedPositions[0].lots=.1;
  assert.ok(closeVolumes(row,'900000001',true,'percent',50)[1].error);
});

test('registration never enables copying and a lost answer never repeats registration',async()=>{
  const calls=[],adapter={config:{enabled:true,followers:[]},inFlight:new Set(),
    async request(route){calls.push(route);if(route==='/account-register')throw Error('lost reply');return {};}};
  const enrollment=createAccountEnrollment({getAdapter:()=>adapter,isTradingBusy:()=>false});
  assert.equal((await enrollment.run('register',{candidateId:'a'.repeat(64)})).status,'rejected');
  await enrollment.run('catalog');
  assert.deepEqual(calls,['/account-register','/account-catalog']);
  assert.deepEqual(adapter.config.followers,[]);
});

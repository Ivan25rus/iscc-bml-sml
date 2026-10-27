const test=require('node:test');
const assert=require('node:assert/strict');
const {createBmlSml}=require('../module');
test('loading and a disabled arm never access the recipient adapter',async()=>{
  const addon=createBmlSml({getAdapter:()=>assert.fail('Unexpected adapter access')});
  assert.equal((await addon.arm({action:'MB',entryMode:'side-zone'})).status,'rejected');
});
test('new entry wrapper rejects legacy modes and multipliers before admission',async()=>{
  const addon=createBmlSml({enabled:()=>true,getAdapter:()=>assert.fail('Unexpected adapter access')});
  for(const payload of [{action:'BS',entryMode:'breakout'},{action:'MB',entryMode:'side-zone',zoneMultiplier:2}])
    assert.equal((await addon.arm(payload)).status,'rejected');
});

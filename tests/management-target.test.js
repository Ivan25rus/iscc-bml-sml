const test=require('node:test'),assert=require('node:assert/strict');
const {entryTarget,normalizeTarget,matchesTarget}=require('../payload/src/app/services/mt5-vps/managementTarget');
test('T|S captures the exact filled parent and honors its primary account',()=>{
  const entry={id:'parent',symbol:'USTEC',side:'buy',children:[{result:{primaryAccount:'900000002'}}],
    accounts:{'900000001':{filledVolume:.5},'900000002':{filledVolume:1}}};
  assert.deepEqual(entryTarget(entry),{entryId:'parent',symbol:'USTEC',side:'buy',account:'900000002'});
  entry.accounts['900000002'].filledVolume=0;assert.equal(entryTarget(entry).account,'900000001');
  entry.positionLifecycle='closed';assert.equal(entryTarget(entry),null);
});
test('T|S cannot silently select another parent, side, symbol, account or a pending order',()=>{
  const wanted=normalizeTarget({entryId:'parent',symbol:'SPX500',side:'sell',account:900000002});
  const row={__isPosition:true,entryPlan:{id:'parent'},symbol:'SPX500',type:'sell'};
  assert.ok(matchesTarget(row,wanted,'900000002'));
  for(const change of [{entryPlan:{id:'other'}},{symbol:'USTEC'},{type:'buy'},{__isPosition:false}])
    assert.equal(matchesTarget({...row,...change},wanted,'900000002'),false);
  assert.equal(matchesTarget(row,wanted,'900000001'),false);
  assert.equal(entryTarget({id:'empty',symbol:'USTEC',side:'buy',accounts:{'900000002':{filledVolume:0}}}),null);
  assert.equal(normalizeTarget({entryId:'parent',symbol:'USTEC',side:'buy',account:'foreign'}),null);
});

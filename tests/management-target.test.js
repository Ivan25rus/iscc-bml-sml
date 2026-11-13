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

test('T|S prefers the exact live account even if a peer snapshot is incomplete',()=>{
  const entry={id:'parent',symbol:'USTEC',side:'buy',primaryAccount:'900000003',
    accounts:{900000001:{filledVolume:.5},900000003:{filledVolume:1}},positionsConfirmed:true,hasOpenPositions:true,
    livePositions:[{account:'900000001',ticket:'81',volume:.5}]};
  assert.deepEqual(entryTarget(entry),{entryId:'parent',symbol:'USTEC',side:'buy',account:'900000001'});
  assert.equal(entryTarget({...entry,positionsConfirmed:false,hasOpenPositions:null}).account,'900000001');
  assert.equal(entryTarget({...entry,livePositions:[{account:'other',ticket:'81',volume:.5}]}).account,'900000003');
  assert.equal(entryTarget({...entry,accounts:{...entry.accounts,900000003:{filledVolume:1,positionsClosed:true}},livePositions:[]}).account,'900000001');
});

test('T|S can navigate to a broker position while the exact entry volume is still being reconciled',()=>{
  const entry={id:'parent',symbol:'SPX500',side:'sell',phase:'running',positionsConfirmed:false,
    primaryAccount:'900000003',accounts:{900000001:{filledVolume:0},900000003:{filledVolume:0}},
    livePositions:[{account:'900000003',ticket:'81',volume:.25}]};
  assert.deepEqual(entryTarget(entry),{entryId:'parent',symbol:'SPX500',side:'sell',account:'900000003'});
  assert.equal(entryTarget({...entry,livePositions:[{account:'900000003',ticket:'0',volume:.25}]}),null);
  assert.equal(entryTarget({...entry,livePositions:[],livePendingOrders:[{account:'900000003',ticket:'81',volume:.25}]}),null);
});

test('T|S honors the captured primary risk account on independent entries without an explicit primary field',()=>{
  const entry={id:'parent',symbol:'SPX500',side:'buy',phase:'running',primaryRiskSizing:{account:'900000003'},
    accounts:{900000001:{filledVolume:5.8},900000003:{filledVolume:73.1},900000004:{filledVolume:0}},children:[]};
  assert.equal(entryTarget(entry).account,'900000003');
  assert.equal(entryTarget({...entry,accounts:{...entry.accounts,900000003:{filledVolume:0}}}).account,'900000001');
});

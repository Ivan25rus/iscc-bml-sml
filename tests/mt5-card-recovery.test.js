const test=require('node:test');
const assert=require('node:assert/strict');
const {recoverCards,recoveryCandidates}=require('../payload/src/app/services/mt5-vps/cardRecovery');
const child={requestId:'group_1',qty:.7,result:{status:'ok',providerOrderId:'71',primaryAccount:'900000002',accountResults:{
  900000002:{status:'ok',providerOrderId:'71',requestedVolume:.7},900000001:{status:'ok',providerOrderId:'81',requestedVolume:.7}}}};
const record={t:100,kind:'level-order',valid:true,reqId:'group',provider:'mt5-vps',strategyId:'group_ls',
  plan:{ticker:'USTEC',level:30000,riskUsd:12.5,stopOffsetPts:1600,takeProfitPts:11600,tickSize:.01},result:{raw:{results:[child]}}};
const log=(...rows)=>rows.map(JSON.stringify).join('\n');
const health=(account,ticket)=>({connected:true,account,server:'Example-MT5',orders:[{ticket,symbol:'USTEC',lots:.7,__isPosition:true}]});
function fixture() {
  const state={server:'Example-MT5',accountStates:{900000002:health(900000002,'71'),900000001:health(900000001,'81')}};
  const reads=[];
  const adapter={config:{account:900000002},brokerSymbol:s=>s,managementState:async options=>{reads.push(options);return state;},
    placeOrder:()=>assert.fail('Recovery must never trade'),closePosition:()=>assert.fail('Recovery must never close')};
  return {adapter,state,reads};
}
test('Restart restores captured tickets on both accounts with read-only watch, never replays an opening',async()=>{
  const {adapter,reads}=fixture();const cards=await recoverCards(adapter,log(record));
  assert.equal(cards.length,1);assert.equal(cards[0].scan.ready,true);assert.deepEqual(cards[0].scan.foundTickets,['71']);
  assert.equal(cards[0].row.followNearestLT,false);assert.equal(cards[0].row.level,30000);
  assert.deepEqual([...reads[0].watch].sort((a,b)=>a.account.localeCompare(b.account)),[{account:'900000001',ticket:'81',symbol:'USTEC'},{account:'900000002',ticket:'71',symbol:'USTEC'}]);
});
test('Corrupt lines, rejected openings, completed groups and duplicate records do not resurrect trades',()=>{
  assert.equal(recoveryCandidates(log(record,record)+'\nbroken').length,1);
  assert.deepEqual(recoveryCandidates(log(record,{kind:'level-order-group-completed',accountsConfirmed:true,parentRequestId:'group'})),[]);
  assert.deepEqual(recoveryCandidates(log({...record,valid:false})),[]);
});
test('Restart restores pending Stop tickets and preserves their order type without another submission',async()=>{
  const {adapter,state,reads}=fixture();
  for (const health of Object.values(state.accountStates)) health.orders[0].__isPosition=false;
  const stopped={...record,plan:{...record.plan,orderType:'stop',action:'BS',referencePrice:30002}};
  const cards=await recoverCards(adapter,log(stopped));
  assert.equal(cards.length,1);assert.equal(cards[0].row.orderType,'stop');assert.equal(cards[0].scan.phase,'waiting');
  assert.equal(reads.length,1);assert.deepEqual(cards[0].children,[child]);
});
test('Same symbol and size on unrelated hedge tickets never restores a card; broker-closed group stays absent',async()=>{
  const {adapter,state}=fixture();state.accountStates[900000002].orders[0].ticket='99';state.accountStates[900000001].orders[0].ticket='98';
  assert.deepEqual(await recoverCards(adapter,log(record)),[]);
  for(const [a,t] of [['900000002','71'],['900000001','81']]) state.accountStates[a].openingStates={[t]:{phase:'closed'}};
  assert.deepEqual(await recoverCards(adapter,log(record)),[]);
});
test('Broker ticket conversion is restored, while disconnected follower and partial volume keep the card locked',async()=>{
  const {adapter,state}=fixture();state.accountStates[900000002].orders[0].ticket='91';
  state.accountStates[900000002].openingStates={'71':{phase:'opened',ticket:'91'}};
  assert.deepEqual((await recoverCards(adapter,log(record)))[0].scan.foundTickets,['91']);
  state.accountStates[900000001].connected=false;
  let cards=await recoverCards(adapter,log(record));assert.equal(cards.length,1);assert.equal(cards[0].scan.ready,false);assert.equal(cards[0].scan.phase,'unknown');
  state.accountStates[900000001].connected=true;state.accountStates[900000002].orders[0].lots=.3;
  cards=await recoverCards(adapter,log(record));assert.equal(cards.length,1);assert.equal(cards[0].scan.ready,false);
});

test('Restart restores an accepted primary after a definitive follower rejection and retires it only after broker exit evidence',async()=>{
  const {adapter,state,reads}=fixture(),incident=JSON.parse(JSON.stringify(record));
  incident.result.raw.results[0].result.status='unknown';
  incident.result.raw.results[0].result.accountResults[900000001]={status:'rejected',reason:'Command expired before submission'};
  state.accountStates[900000001].orders=[];
  let cards=await recoverCards(adapter,log(incident));
  assert.equal(cards.length,1);assert.equal(cards[0].scan.phase,'partial-opened');assert.equal(cards[0].scan.ready,false);
  assert.deepEqual(reads[0].watch,[{account:'900000002',ticket:'71',symbol:'USTEC'}]);
  state.accountStates[900000002].orders=[];state.accountStates[900000002].openingStates={'71':{phase:'closed'}};
  const completed=[];
  assert.deepEqual(await recoverCards(adapter,log(incident),{onCompleted:(record,scan)=>completed.push({id:record.reqId,accounts:scan.accounts})}),[]);
  assert.deepEqual(completed,[{id:'group',accounts:{900000002:['closed'],900000001:['rejected']}}]);
});

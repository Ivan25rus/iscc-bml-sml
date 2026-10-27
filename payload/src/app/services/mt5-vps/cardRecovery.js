const {indexSymbol}=require('./approachPreparation');
const {openingWatch,scanLinkedPlacement}=require('./levelControl');

function recoveryCandidates(text) {
  const groups=new Map(),completed=new Set();
  for (const line of String(text || '').split('\n')) {
    let record;try {record=JSON.parse(line);} catch {continue;}
    if (record.kind==='level-order-group-completed' && record.accountsConfirmed===true) completed.add(record.parentRequestId);
    if (record.kind!=='level-order' || record.provider!=='mt5-vps' || record.valid!==true || !record.reqId) continue;
    const children=record.result?.raw?.results;
    if (!Array.isArray(children) || !children.length || !['USTEC','SPX500'].includes(indexSymbol(record.plan?.ticker))) continue;
    if (children.some(child=>openingWatch([child],record.plan.ticker,child.result?.primaryAccount).length)) groups.set(record.reqId,record);
  }
  return [...groups.values()].filter(record=>!completed.has(record.reqId));
}

async function recoverCards(adapter,text,{onCompleted=()=>{}}={}) {
  const recovered=[];
  for (const record of recoveryCandidates(text)) {
    const children=record.result.raw.results,symbol=record.plan.ticker;
    const watch=openingWatch(children,adapter.brokerSymbol(symbol),adapter.config.account);
    if (!watch.length || watch.length>100) continue;
    const state=await adapter.managementState({maxAgeMs:0,watch});
    const scan=scanLinkedPlacement(state,children,symbol,adapter.config.account);
    if (scan.completed) {onCompleted(record,scan);continue;}
    // Match original opening tickets (or broker-resolved position tickets),
    // never infer ownership merely from a symbol or a similar position size.
    const active=watch.some(item=>{
      const health=state.accountStates?.[item.account];
      if (!health?.connected || String(health.account)!==item.account || health.server!==state.server) return false;
      const resolved=health.openingStates?.[item.ticket]?.ticket;
      return health.orders?.some(row=>indexSymbol(row.symbol)===indexSymbol(symbol)
        && [row.ticket,row.identifier].some(id=>id!=null && [item.ticket,resolved].includes(String(id))));
    });
    if (!active) continue;
    recovered.push({parentRequestId:record.reqId,strategyId:record.strategyId,children,scan,
      row:{ticker:indexSymbol(symbol),provider:'mt5-vps',instrumentType:'FX',cardType:'levelOrder',event:'levelOrder',
        cardSource:'recovered-position',time:record.t || record.reqId,level:record.plan.level,
        riskUsd:record.plan.riskUsd,stopOffsetPts:record.plan.stopOffsetPts,stopMode:record.plan.stopMode,
        orderType:record.plan.orderType || 'limit',
        takeProfitPts:record.plan.takeProfitPts,pointSize:record.plan.tickSize,minLot:record.plan.minLot,
        followNearestLT:false}});
  }
  return recovered;
}

module.exports={recoveryCandidates,recoverCards};

const {TRADING_SYMBOLS}=require('./tradingSymbols');
const {managementFreshness}=require('./managementFreshness');
const freshPositionAt=(checkedAt,now=Date.now())=>Number.isFinite(checkedAt) && now-checkedAt>=-250 && now-checkedAt<=2500;
const isOpenPosition=(order,symbol)=>order?.__isPosition===true && order.symbol===symbol && Number(order.lots)>0;

// Display consolidation only. Active/uncertain ownership is supplied by the caller.
function coalesceIdleCards(rows,{key,symbol,eligible,protectedRow,preferredKey,priority=()=>0}={}) {
  const selected=new Map(),removed=[];
  for(const row of rows) {
    if(!eligible(row) || protectedRow(row))continue;
    const name=symbol(row),previous=selected.get(name);
    if(!previous){selected.set(name,row);continue;}
    if(key(previous)!==preferredKey && (key(row)===preferredKey || priority(row)>priority(previous))) {
      removed.push(previous);selected.set(name,row);
    }
    else removed.push(row);
  }
  const discard=new Set(removed);
  return {rows:rows.filter(row=>!discard.has(row)),removed};
}
function uniqueCardNodes(nodes,preferred) {
  const result=new Map();
  for(const node of nodes) {
    const key=node.dataset.rowkey,previous=result.get(key);
    if(previous) {
      if(node===preferred){previous.remove();result.set(key,node);}
      else node.remove();
    } else result.set(key,node);
  }
  return result;
}
function canAttachRecoveredEntry(entry) {
  if(entry._armUnconfirmed || entry._stopUnconfirmed)return true;
  if(!['unknown','completed','stopped'].includes(entry.phase) && !entry.closurePending && !entry.protectionPending)return true;
  if(entry.lifecycleDeferred===true)return false;
  // A durable control or pending-volume record can outlive its broker order.
  // Positive evidence from a connected account remains useful even when a
  // retired peer cannot contribute to the aggregate positionsConfirmed flag.
  return !!(entry.livePositions?.length || entry.livePendingOrders?.length
    || entry.positionsConfirmed!==false && entry.hasOpenPositions===true);
}
function keepsLtLevel(entry) {
  if(!entry || entry.positionLifecycle==='closed' || entry.positionsClosed===true || entry.cancelledBeforeAdmission)return false;
  if(!['completed','stopped'].includes(entry.phase) || entry.livePendingOrders?.length)return true;
  return !(entry.positionsConfirmed===true && entry.hasOpenPositions===false)
    && (entry.hasOpenPositions===true || entry.livePositions?.length>0
      || Object.values(entry.accounts || {}).some(row=>row.filledVolume>0 && !row.positionsClosed));
}
function openIndexSymbols(state, brokerSymbol, receivedAt=Date.now(), registeredAccounts=[]) {
  const snapshot=state && managementFreshness(state,receivedAt);
  const accounts=snapshot?.accountStates || {};
  const expected=new Set([...registeredAccounts, snapshot?.account,...(snapshot?.linkedAccounts || []),...Object.keys(accounts)]
    .filter(account=>account!=null).map(String));
  const healthy=Object.entries(accounts).filter(([account,row])=>row.connected===true && row.evidenceReady!==false
    && String(row.account)===account && row.server===snapshot.server && Number.isFinite(row.checkedAt) && Array.isArray(row.orders))
    .map(([,row])=>row);
  const complete=healthy.length>0 && healthy.length===expected.size;
  const includes=(row,symbol)=>Array.isArray(row?.orders) && row.orders.some(order=>isOpenPosition(order,symbol));
  const positionTimes=Object.fromEntries(TRADING_SYMBOLS.flatMap(symbol=>{
    const rows=healthy.filter(row=>includes(row,brokerSymbol(symbol)));
    return rows.length ? [[symbol,Math.max(...rows.map(row=>row.checkedAt))]] : [];
  }));
  const symbols=Object.keys(positionTimes);
  const symbolsIn=views=>TRADING_SYMBOLS.filter(symbol=>views.some(row=>includes(row,brokerSymbol(symbol))));
  const uncertainSymbols=complete ? [] : symbolsIn(Object.values(state?.accountStates || {})).filter(symbol=>!symbols.includes(symbol));
  return {complete,symbols,positionTimes,uncertainSymbols,accounts:[...expected],
    checkedAt:complete ? Math.min(...healthy.map(row=>row.checkedAt)) : 0};
}
function hasEntryPosition(entry, evidence) {
  if(!keepsLtLevel(entry))return false;
  if(evidence?.complete && evidence.checkedAt>=(Number(entry.updatedAt || entry.createdAt) || 0)
    && ['completed','stopped'].includes(entry.phase)
    && !entry._armUnconfirmed && !entry._stopUnconfirmed && !entry.closurePending && !entry.protectionPending)
    return false;
  return entry.hasOpenPositions===true || entry.livePositions?.length>0
    || Object.values(entry.accounts || {}).some(row=>row.filledVolume>0 && !row.positionsClosed);
}
module.exports={coalesceIdleCards,uniqueCardNodes,canAttachRecoveredEntry,keepsLtLevel,openIndexSymbols,hasEntryPosition,
  freshPositionAt,isOpenPosition};

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
module.exports={coalesceIdleCards,uniqueCardNodes,canAttachRecoveredEntry};

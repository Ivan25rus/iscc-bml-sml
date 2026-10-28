// Final control receipts can retire only their own older pending projection.
// This is display state, never permission to replay a command or infer a fill.
const EXACT_CLOSE_CAPABILITY='entry-exact-parent-close-v1';
const PHASES=new Set(['armed','running','paused','sending','completed','stopped','unknown']);
function accountIds(entry) {
  if(!entry?.accounts || typeof entry.accounts!=='object' || Array.isArray(entry.accounts))return null;
  const accounts=Object.keys(entry.accounts).sort();
  return accounts.length>0 && accounts.length<=32 && accounts.every(id=>/^[1-9][0-9]{0,19}$/.test(id)) ? accounts : null;
}
function sameParent(left,right) {
  const a=accountIds(left),b=accountIds(right);
  return !!a && !!b && left.id===right.id && left.symbol===right.symbol && left.side===right.side
    && left.executionPolicy==='hybrid-market-limit-v1' && right.executionPolicy===left.executionPolicy
    && a.join(',')===b.join(',');
}
function controlEntry(entry,result,expected,operation) {
  const accounts=accountIds(entry),wanted=[...(expected.accounts || [])].sort();
  if(!entry || !['ok','rejected'].includes(result?.status) || result.entryId!==expected.id
    || entry.id!==expected.id || entry.symbol!==expected.symbol || entry.side!==expected.side
    || entry.executionPolicy!=='hybrid-market-limit-v1' || !accounts || accounts.join(',')!==wanted.join(',')
    || !PHASES.has(entry.phase) || !Array.isArray(entry.children)
    || entry.protectionPending || entry.closurePending)return null;
  if(operation==='entry-close' && result.status==='ok'
    && (entry.hasOpenPositions!==false || entry.positionsConfirmed!==true || entry.positionLifecycle!=='closed'))return null;
  const {_controlReceipt,...record}=entry;
  return {...record,_controlReceipt:{id:result.id,entryId:entry.id,operation,status:result.status}};
}
function retainControlEntry(current,next) {
  const proof=current?._controlReceipt;
  return !!proof && ['ok','rejected'].includes(proof.status) && proof.entryId===current.id && sameParent(current,next)
    && !current.protectionPending && !current.closurePending
    && (proof.operation==='entry-protection' && next.protectionPending===proof.id
      || proof.operation==='entry-close' && next.closurePending===proof.id)
    && Number.isFinite(current.updatedAt) && Number.isFinite(next.updatedAt) && next.updatedAt<=current.updatedAt;
}
module.exports={EXACT_CLOSE_CAPABILITY,accountIds,controlEntry,retainControlEntry};

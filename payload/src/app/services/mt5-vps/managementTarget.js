// Navigation only. Native execution always revalidates the captured position.
function normalizeTarget(value) {
  if(!value || typeof value!=='object' || typeof value.entryId!=='string'
    || !/^[A-Za-z0-9_.:-]{1,100}$/.test(value.entryId) || typeof value.symbol!=='string'
    || !/^[A-Za-z0-9_.:-]{1,80}$/.test(value.symbol) || !['buy','sell'].includes(value.side)
    || !['string','number'].includes(typeof value.account) || !/^[1-9][0-9]{0,18}$/.test(String(value.account)))return null;
  return {entryId:value.entryId,symbol:value.symbol,side:value.side,account:String(value.account)};
}
function entryTarget(entry) {
  if(!entry || entry.positionLifecycle==='closed' || entry.positionsClosed===true
    || entry.hasOpenPositions===false && entry.positionsConfirmed===true)return null;
  const accounts=Object.entries(entry.accounts || {}).filter(([,row])=>row && row.positionsClosed!==true);
  const primary=String(entry.primaryAccount || entry.primaryRiskSizing?.account
    || entry.children?.find(child=>child.result?.primaryAccount)?.result.primaryAccount || '');
  // The receiver includes live positions only for the accounts it could read.
  // An incomplete peer snapshot does not invalidate an exact live ticket.
  const livePositions=Array.isArray(entry.livePositions) ? entry.livePositions : [];
  const liveAccounts=new Set(livePositions.filter(row=>row &&
    /^[1-9][0-9]{0,19}$/.test(String(row.ticket)) && Number.isFinite(row.volume) && row.volume>0)
    .map(row=>String(row.account)));
  const live=accounts.filter(([id])=>liveAccounts.has(id));
  const candidates=live.length ? live : accounts.filter(([,row])=>Number.isFinite(row.filledVolume) && row.filledVolume>0);
  const account=candidates.find(([id])=>id===primary)?.[0] || candidates[0]?.[0];
  return normalizeTarget({entryId:entry.id,symbol:entry.symbol,side:entry.side,account});
}
function matchesTarget(row,wanted,account) {
  return !!row && row.__isPosition===true && row.entryPlan?.id===wanted.entryId
    && row.symbol===wanted.symbol && row.type===wanted.side && String(account)===wanted.account;
}
module.exports={normalizeTarget,entryTarget,matchesTarget};

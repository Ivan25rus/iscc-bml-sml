const {createHash}=require('node:crypto');
const {readFileSync}=require('node:fs');

function closedEntryCardIdentity(entry) {
  const accounts=Object.entries(entry?.accounts || {}).sort();
  const children=entry?.children;
  if(entry?.phase!=='unknown' || entry.executionPolicy!=='hybrid-market-limit-v1'
      || !accounts.length || !Array.isArray(children) || !children.length
      || entry.livePositions?.length || entry.positionsConfirmed===true && entry.hasOpenPositions===true
      || accounts.some(([account,row])=>!['completed','stopped','unknown'].includes(entry.lanePhases?.[account])
        || Number(row.pendingVolume)>0 || row.admissionWaiting)
      || children.some(child=>!['confirmed','cancelled','rejected'].includes(child.state)))return null;
  const identity=[entry.id,entry.symbol,entry.side,entry.createdAt,entry.accountExecution,
    entry.closurePending ?? null,entry.protectionPending ?? null,
    accounts.map(([account,row])=>[account,row.targetVolume,row.filledVolume]),
    children.map(child=>[child.id,child.index,child.type,child.state,Object.entries(child.volumes || {}).sort(),
      Object.entries(child.positions || {}).sort().map(([account,row])=>[
        account,String(row.ticket),String(row.openingTicket || row.ticket),row.volume,row.fillPrice])])];
  return createHash('sha256').update(JSON.stringify(identity)).digest('hex');
}

function loadClosedEntryCards(file) {
  let proofs;
  try {
    const data=JSON.parse(readFileSync(file,'utf8'));
    if(data.schema===1 && Array.isArray(data.entries))
      proofs=new Map(data.entries.filter(row=>typeof row?.id==='string' && /^[a-f0-9]{64}$/.test(row.identity))
        .map(row=>[row.id,row.identity]));
  } catch(error) {
    if(error.code!=='ENOENT')console.error('[closed-entry-cards]',error.message);
  }
  return entries=>!proofs?.size || !Array.isArray(entries) ? entries : entries.filter(entry=>
    !proofs.has(entry?.id) || proofs.get(entry.id)!==closedEntryCardIdentity(entry));
}

module.exports={closedEntryCardIdentity,loadClosedEntryCards};

// Display-only projection of exact native proofs. Never authorizes a trade.
const {createHash}=require('crypto');
const commandDigest=(id,account)=>createHash('sha256').update(`${id}:${account}`).digest('hex');

function protectionReceiptView(rows,result,{target,account,server,checkedAt}={}) {
  const plan=target?.entryPlan,selected=String(account);
  if(!['ok','rejected','unknown'].includes(result?.status) || typeof result.id!=='string' || !result.id
    || !plan?.id || result.entryId!==plan.id || target.__isPosition!==true || plan.side!==target.type
    || !Array.isArray(plan.accounts) || !plan.accounts.includes(selected))return rows;
  const expected=target.singleAccount ? [selected] : plan.accounts;
  if(Object.keys(result.accountResults || {}).some(id=>!expected.includes(id)))return rows;
  const outcome=result.accountResults?.[selected],proof=outcome?.protectionProof;
  const independent=result.independentAccountControls===true;
  const localId=independent ? 'account-protection:'+commandDigest(result.id,selected) : result.id;
  if(result.status!=='ok' && !independent || independent && outcome?.controlId!==localId
    || outcome?.status!=='ok' || outcome.noPosition===true || proof?.schema!=='entry-protection-proof-v1'
    || proof.commandId!=='parent-protection:'+commandDigest(localId,selected) || proof.account!==selected
    || typeof server!=='string' || proof.server!==server || proof.symbol!==target.symbol
    || !Number.isFinite(proof.checkedAt) || Number.isFinite(checkedAt) && proof.checkedAt<checkedAt
    || !Array.isArray(proof.positions)
    || proof.positions.length>200 || JSON.stringify(proof.positions)!==JSON.stringify(outcome.positions))return rows;
  const own=rows.filter(row=>row.__isPosition===true && row.entryPlan?.id===plan.id
    && row.symbol===target.symbol && row.type===target.type);
  if(!own.some(row=>String(row.ticket)===String(target.ticket) && row.lots===target.lots && row.open_price===target.open_price))return rows;
  // The aggregate receipt may omit the field that was not edited. Both
  // current prices must come from the complete native position proof.
  const sl=proof.positions[0]?.sl,tp=proof.positions[0]?.tp,averagePrice=outcome.averagePrice;
  if(!Number.isFinite(sl) || sl<=0 || !Number.isFinite(tp) || tp<0
    || !Number.isFinite(averagePrice) || averagePrice<=0
    || proof.positions.some(position=>position?.sl!==sl || position.tp!==tp || !/^[1-9][0-9]{0,19}$/.test(String(position.ticket))
      || !Number.isFinite(position.volume) || position.volume<=0 || !Number.isFinite(position.fillPrice) || position.fillPrice<=0))return rows;
  const byTicket=new Map(proof.positions.map(row=>[String(row.ticket),row]));
  if(byTicket.size!==proof.positions.length
    || own.some(row=>{const p=byTicket.get(String(row.ticket));return !p || row.lots!==p.volume || row.open_price!==p.fillPrice;}))return rows;
  const ownRows=new Set(own);
  return rows.map(row=>{
    if(!ownRows.has(row))return row;
    const p=byTicket.get(String(row.ticket));
    return {...row,sl:p.sl,tp:p.tp,entryPlan:{...row.entryPlan,sl,tp,averagePrice}};
  });
}
module.exports={protectionReceiptView};

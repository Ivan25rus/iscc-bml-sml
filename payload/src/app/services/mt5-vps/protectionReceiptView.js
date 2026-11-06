// Display-only projection of exact native proofs. Never authorizes a trade.
function protectionReceiptView(rows,result,account,server) {
  if(result?.status!=='ok' || !result.id || !result.entryId)return rows;
  const outcome=result.accountResults?.[account],proof=outcome?.protectionProof;
  if(outcome?.status!=='ok' || proof?.schema!=='entry-protection-proof-v1' || proof.account!==String(account)
    || proof.server!==server || !Number.isFinite(proof.checkedAt) || !Array.isArray(proof.positions)
    || proof.positions.length>200 || JSON.stringify(proof.positions)!==JSON.stringify(outcome.positions))return rows;
  const own=rows.filter(row=>row.entryPlan?.id===result.entryId);
  if(!own.length || own.some(row=>row.entryPlan.accounts.some(id=>result.accountResults?.[id]?.status!=='ok')))return rows;
  // The aggregate receipt may omit the field that was not edited. Both
  // current prices must come from the complete native position proof.
  const sl=proof.positions[0]?.sl,tp=proof.positions[0]?.tp,averagePrice=outcome.averagePrice;
  if(!Number.isFinite(sl) || sl<=0 || !Number.isFinite(tp) || tp<0
    || !Number.isFinite(averagePrice) || averagePrice<=0
    || proof.positions.some(position=>position.sl!==sl || position.tp!==tp))return rows;
  const byTicket=new Map(proof.positions.map(row=>[String(row.ticket),row]));
  if(own.some(row=>{const p=byTicket.get(String(row.ticket));return !p || row.lots!==p.volume || row.open_price!==p.fillPrice
    || !Number.isFinite(p.sl) || p.sl<=0 || !Number.isFinite(p.tp) || p.tp<0;}))return rows;
  return rows.map(row=>{
    if(row.entryPlan?.id!==result.entryId)return row;
    const p=byTicket.get(String(row.ticket));
    return {...row,sl:p.sl,tp:p.tp,entryPlan:{...row.entryPlan,sl,tp,averagePrice}};
  });
}
module.exports={protectionReceiptView};

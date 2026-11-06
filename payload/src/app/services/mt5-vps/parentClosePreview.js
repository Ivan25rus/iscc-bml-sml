const {percentVolume}=require('./sizing');
function parentClosePreview(row,account,states,mode,value) {
  const plan=row?.entryPlan;
  if(!Array.isArray(plan?.accounts) || !plan.accounts.includes(String(account)))throw Error('Нет состава счетов всего набора.');
  const groups=plan.accounts.map(account=>{
    const state=states[account];
    if(!state?.connected || state.evidenceReady===false || !Array.isArray(state.orders))
      return {account,error:'Нет подтверждённых данных всего набора на счёте'};
    const positions=state.orders.filter(item=>item.__isPosition && item.entryPlan?.id===plan.id && item.symbol===row.symbol && item.type===row.type);
    if(new Set(positions.map(item=>String(item.ticket))).size!==positions.length)return {account,error:'Повторяющиеся позиции набора'};
    return {account,positions,lots:Number(positions.reduce((sum,item)=>sum+Number(item.lots),0).toFixed(8))};
  });
  const selected=groups.find(item=>item.account===String(account));
  const percent=mode==='percent'?Number(value):Number(value)/selected?.lots*100;
  if(!Number.isFinite(percent) || percent<=0 || percent>=100)throw Error('Процент закрытия набора должен быть больше 0 и меньше 100.');
  return {percent,preview:groups.map(group=>{
    try {
      if(group.error)throw Error(group.error);
      if(!group.positions.length)return {account:group.account,volume:0,remaining:0,step:'—'};
      const spec=group.positions[0],step=Number(spec.quantityStep),minimum=Number(spec.minQty);
      if(group.positions.some(item=>Number(item.quantityStep)!==step || Number(item.minQty)!==minimum
        || !Number.isFinite(item.lots) || item.lots<=0))throw Error('Различаются параметры объёма позиций набора');
      const volume=percentVolume({...spec,lots:group.lots},percent);
      return {account:group.account,volume,remaining:Number((group.lots-volume).toFixed(8)),step};
    } catch(error){return {account:group.account,error:error.message};}
  })};
}
module.exports={parentClosePreview};

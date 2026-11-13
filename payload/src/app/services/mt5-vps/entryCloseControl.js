const {accountIds}=require('./entryControlState');

function pendingEntryCloseConfirmation(entry,result) {
  const accounts=accountIds(entry),outcomes=result?.accountResults;
  if(result?.status!=='unknown' || result.entryId!==entry?.id || !result.id
    || result.independentAccountControls!==true || Object.hasOwn(result,'closePercent') || !accounts || !outcomes
    || Object.keys(outcomes).length!==accounts.length)return null;
  const pending=[],lines=[];
  let confirmed=0,total=0;
  for(const account of accounts) {
    const outcome=outcomes[account];
    if(outcome?.status==='ok') {
      if(outcome.noPosition===true)lines.push(`${account} · позиции не было`);
      else {confirmed++;total++;lines.push(`${account} · закрыто`);}
      continue;
    }
    if(outcome?.status!=='unknown' || !/^(Broker has not confirmed every owned position fully closed$|Absent owned position has no definite broker full-exit proof(?::|$))/.test(outcome.reason)
      || !Array.isArray(outcome.positions) || !outcome.positions.length)return null;
    const captures=(entry.children || []).flatMap(child=>{
      const position=child.positions?.[account];
      return position ? [[String(position.ticket),position.volume]] : [];
    });
    const expected=new Map(captures);
    if(expected.size!==captures.length || outcome.positions.length!==expected.size)return null;
    const orders=new Set();
    for(const position of outcome.positions) {
      const ticket=String(position?.ticket),volume=expected.get(ticket),order=String(position?.providerOrderId);
      if(!/^[1-9][0-9]{0,19}$/.test(ticket) || !Number.isFinite(volume) || volume<=0)return null;
      expected.delete(ticket);
      if(position.alreadyClosed===true && position.status==='closed')continue;
      if(position.status!=='ok' || position.operation!=='close' || position.retcode!==10009
        || !/^[1-9][0-9]{0,19}$/.test(order) || orders.has(order) || !Number.isFinite(position.filledVolume)
        || Math.abs(position.filledVolume-volume)>1e-8)return null;
      orders.add(order);
    }
    if(expected.size || !orders.size)return null;
    pending.push(account);total++;lines.push(`${account} · сверяется`);
  }
  if(!pending.length)return null;
  const caption=`Закрыто ${confirmed} из ${total}`;
  return {caption,detail:`${caption} · ${pending.join(', ')} ${pending.length===1?'сверяется':'сверяются'}`,accounts:lines.join('\n')};
}

// Presentation evidence only. Never updates broker state or authorizes a trade.
class EntryCloseControl {
  constructor(){this.knownOpen=new Map();}
  forget(id){this.knownOpen.delete(id);}
  clear(){this.knownOpen.clear();}
  state(entry,{busy=false,stopping=false,closeUnknown=false}={}) {
    const hidden={visible:false,disabled:true,checking:false};
    if(!entry?.id || !entry.symbol || entry.executionPolicy!=='hybrid-market-limit-v1')return hidden;
    if(entry.positionLifecycle==='closed' || entry.positionsClosed===true) {
      this.forget(entry.id);return hidden;
    }
    const symbol=String(entry.symbol).toUpperCase();
    if(entry.hasOpenPositions===true)this.knownOpen.set(entry.id,symbol);
    if(this.knownOpen.get(entry.id)!==symbol)return hidden;
    return {visible:true,disabled:!!(busy || stopping || closeUnknown && entry.independentAccountControls!==true || entry.protectionPending || entry.closurePending),
      checking:entry.hasOpenPositions!==true || entry.positionsConfirmed!==true
        || entry.lifecycleDeferred===true || !!entry._readError || entry.phase==='unknown'};
  }
}
module.exports={EntryCloseControl,pendingEntryCloseConfirmation};

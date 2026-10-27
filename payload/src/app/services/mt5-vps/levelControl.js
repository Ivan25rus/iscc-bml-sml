const {indexSymbol}=require('./approachPreparation');
const {performance}=require('perf_hooks');
const busy=new WeakMap();

async function controlLevelTickets(adapter,payload,{cancel=false,onResult=()=>{}}={}) {
  const symbol=adapter.brokerSymbol(payload.symbol);
  const ids=new Set([...(payload.tickets || []),...(payload.expectedIds || [])].map(String));
  if (!ids.size || ids.size>100 || [...ids].some(id=>!id || id.length>160)) return {status:'rejected',reason:'Нужны номера выбранной сделки',closed:0,results:[]};
  let locks=busy.get(adapter);if (!locks) busy.set(adapter,locks=new Set());
  if (locks.has(symbol)) return {status:'unknown',reason:'Команда для этой карточки уже выполняется',closed:0,results:[]};
  locks.add(symbol);
  const started=Date.now(),supplied=Number(payload.clickAt);
  const clickAt=Number.isFinite(supplied) && supplied<=started && started-supplied<=60000 ? supplied : started;
  const results=[];
  try {
    const readStarted=performance.now();
    const rows=await adapter.managementOrders();
    const managementReadMs=Number((performance.now()-readStarted).toFixed(2));
    const selected=rows.filter(row=>indexSymbol(row.symbol)===indexSymbol(symbol) && row.__isPosition===!cancel
      && [row.ticket,row.identifier,...String(row.comment || '').split(/[:\s]+/)].some(id=>id!=null && ids.has(String(id))));
    const explicit=(payload.tickets || []).map(String);
    if (!selected.length || explicit.some(id=>!selected.some(row=>String(row.ticket)===id || String(row.identifier)===id))) {
      return {status:'rejected',reason:cancel ? 'Заявка уже изменилась или исполнилась. Обнови позиции.' : 'Выбранная позиция не подтверждена. Обнови список.',closed:0,results:[]};
    }
    for (const row of selected) {
      const result=cancel ? await adapter.cancelPendingOrder(row.ticket,row.symbol,false,clickAt)
        : await adapter.closePosition({...row,clickAt},'level card close',undefined,undefined,{managementReadMs});
      result.timings={...result.timings,managementReadMs};
      results.push({ticket:String(row.ticket),result});onResult(row,result);
      if (result.status==='unknown') break;
    }
    const confirmed=results.filter(row=>row.result.status==='ok').length;
    const errors=results.filter(row=>row.result.status!=='ok').map(row=>({ticket:row.ticket,reason:row.result.reason || row.result.status}));
    return {status:results.some(row=>row.result.status==='unknown') ? 'unknown' : errors.length ? (confirmed ? 'partial' : 'rejected') : 'ok',
      provider:'mt5-vps',symbol,closed:cancel ? 0 : confirmed,cancelled:cancel ? confirmed : 0,results,errors,
      timings:{macClickToResultMs:Date.now()-clickAt,macPreliminaryMs:results[0]?.result?.timings?.macBeforeSendMs}};
  } catch(error) {return {status:results.length ? 'unknown' : 'rejected',reason:error.message,closed:0,results};}
  finally {locks.delete(symbol);}
}

function scanLinkedPlacement(state,children,symbol,primaryAccount) {
  const accounts={},reasons={},closeDetails={};let expectedQty=0,foundQty=0;const foundTickets=[];
  for (const child of children || []) {
    const replies=child.result?.accountResults || {[primaryAccount]:child.result};
    for (const [account,reply] of Object.entries(replies)) {
      const health=state.accountStates?.[account];
      const verified=health?.connected && String(health.account)===String(account) && health.server===state.server;
      const good=verified && reply?.status==='ok';
      const ticket=String(reply?.providerOrderId || '');
      const history=health?.openingStates?.[ticket];
      const row=good && health.orders?.find(row=>indexSymbol(row.symbol)===indexSymbol(symbol)
        && [row.ticket,row.identifier].some(value=>value!=null && (String(value)===ticket || String(value)===history?.ticket)));
      const volume=Number(reply?.requestedVolume || reply?.filledVolume || (String(account)===String(primaryAccount) ? child.qty : 0));
      const phase=verified && reply?.status==='rejected' ? 'rejected' : !good ? 'unknown' : !row ? ['closed','cancelled'].includes(history?.phase) ? history.phase : 'absent' : row.__isPosition && volume>0 && Math.abs(row.lots-volume)<1e-8 ? 'opened' : row.__isPosition ? 'changed' : 'waiting';
      accounts[account]=accounts[account] || [];accounts[account].push(phase);
      if(['closed','cancelled'].includes(phase))(closeDetails[account] ||= []).push({ticket,phase,exitReasons:history.exitReasons || []});
      if (reply?.reason) (reasons[account] ||= []).push(reply.reason);
      if (String(account)===String(primaryAccount)) {
        expectedQty+=volume;
        if (phase==='opened') {foundQty+=row.lots;foundTickets.push(String(row.ticket));}
      }
    }
  }
  const phases=Object.values(accounts).flat();
  const accepted=phases.filter(value=>value!=='rejected'),partial=phases.includes('rejected') && accepted.length>0;
  return {ready:phases.length>0 && phases.every(value=>value==='opened'),
    completed:accepted.length>0 && phases.every(value=>['closed','cancelled','rejected'].includes(value)),
    phase:!phases.length || phases.some(value=>['unknown','changed','absent'].includes(value)) ? 'unknown'
      : partial && accepted.every(value=>value==='opened') ? 'partial-opened'
      : partial && accepted.every(value=>value==='waiting') ? 'partial-waiting'
      : phases.every(value=>value==='waiting') ? 'waiting' : 'mixed',
    accounts,reasons,closeDetails,expectedQty,foundQty,foundTickets,expectedCids:[],foundCids:[]};
}

function openingWatch(children,symbol,primaryAccount) {
  return (children || []).flatMap(child=>Object.entries(child.result?.accountResults || {[primaryAccount]:child.result})
    .filter(([,reply])=>reply?.status==='ok' && /^[1-9][0-9]{0,19}$/.test(String(reply.providerOrderId)))
    .map(([account,reply])=>({account:String(account),ticket:String(reply.providerOrderId),symbol})));
}

module.exports={controlLevelTickets,scanLinkedPlacement,openingWatch};

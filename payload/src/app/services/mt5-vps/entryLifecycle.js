// Read-only position lifecycle for captured entry tickets. A missing plan or
// order is never proof of a close; only the broker's balanced exit history is.
const TERMINAL=new Set(['completed','stopped']);
const LIFECYCLE_VERSIONS=new Set([34,35,36,37,38,39]);
const HYBRID_LIFECYCLE_VERSIONS=new Set([35,36,37,38,39]);
const HYBRID_POLICY='hybrid-market-limit-v1';
const ADMISSION_CANCEL_CAPABILITY='entry-admission-cancel-proof-v1';
const CANCEL_FIELDS=new Set(['id','phase','reason','accounts','children','createdAt','updatedAt','cancelledBeforeAdmission',
  'symbol','side','executionPolicy','_admissionCancelConfirmed','viewRevision','viewEpoch']);
const MAX_WATCH=400;
const WATCH_BATCH=100;
const {readErrorKind}=require('./entryStatus');
const {retainControlEntry}=require('./entryControlState');
const supportsLifecycle=(state,entry)=>LIFECYCLE_VERSIONS.has(state?.version)
  && (entry.executionPolicy!==HYBRID_POLICY || HYBRID_LIFECYCLE_VERSIONS.has(state.version));
const unresolved=entry=>(entry.children || []).some(child=>!['confirmed','cancelled','rejected'].includes(child.state));
const hasFills=entry=>Object.values(entry.accounts || {}).some(account=>Number(account.filledVolume)>0);
function settledIndependentOutcome(entry) {
  const accounts=Object.keys(entry.accounts || {});
  return ['independent-v4','independent-v5'].includes(entry.accountExecution) && entry.phase==='unknown' && accounts.length>0
    && accounts.every(account=>['completed','stopped','unknown'].includes(entry.lanePhases?.[account]))
    && !entry.protectionPending && !entry.closurePending && entry.reconciliation?.status!=='checking'
    && !unresolved(entry);
}
function settledClosingOutcome(entry) {
  const accounts=Object.keys(entry.accounts || {});
  return entry.accountExecution==='independent-v5' && entry.phase==='unknown' && typeof entry.closurePending==='string'
    && accounts.length>0 && !entry.protectionPending && entry.reconciliation?.status!=='checking' && !unresolved(entry)
    && accounts.every(account=>['completed','stopped','unknown'].includes(entry.lanePhases?.[account])
      && !entry.accounts[account].commonStopUncertain && Number(entry.accounts[account].targetVolume)>0
      && Math.abs(entry.accounts[account].filledVolume-entry.accounts[account].targetVolume)<1e-8);
}
const settledLifecycle=entry=>settledIndependentOutcome(entry) || settledClosingOutcome(entry);
function closedControlIdentity(entry) {
  const sorted=object=>Object.entries(object || {}).sort(([a],[b])=>a.localeCompare(b));
  return JSON.stringify([entry.symbol,entry.side,entry.closurePending,
    sorted(entry.accounts).map(([account,row])=>[account,row.targetVolume,row.filledVolume]),
    (entry.children || []).map(child=>[child.id,child.state,child.type,sorted(child.volumes),
      sorted(child.positions).map(([account,row])=>[account,row.ticket,row.openingTicket || row.ticket,row.volume,row.fillPrice])])]);
}
function closedDisplayIdentity(entry) {
  const sorted=object=>Object.entries(object || {}).sort(([a],[b])=>a.localeCompare(b));
  return JSON.stringify([entry.symbol,entry.side,entry.executionPolicy,entry.accountExecution,
    sorted(entry.accounts).map(([account,row])=>[account,row.targetVolume]),
    (entry.children || []).map(child=>[child.id,child.index,child.state,child.type,sorted(child.volumes),
      sorted(child.positions).map(([account,row])=>[account,row.ticket,row.openingTicket || row.ticket,row.volume,row.fillPrice])])]);
}
function settledDisplay(entry) {
  return (TERMINAL.has(entry.phase) || settledIndependentOutcome(entry)) && !unresolved(entry)
    && !entry.protectionPending && !entry.closurePending && entry.reconciliation?.status!=='checking'
    && (!['independent-v4','independent-v5'].includes(entry.accountExecution)
      || Object.keys(entry.accounts || {}).every(account=>['completed','stopped','unknown'].includes(entry.lanePhases?.[account])))
    && Object.entries(entry.accounts || {}).every(([account,row])=>Number.isFinite(row.filledVolume) && row.filledVolume>=0
      && Number.isFinite(row.targetVolume) && row.targetVolume>0 && row.filledVolume<=row.targetVolume+1e-7
      && !row.commonStopUncertain && !(Number(row.pendingVolume)>0));
}
function retainClosedDisplay(adapter,entry) {
  if(!settledDisplay(entry))return;
  entry._closedDisplayIdentity=closedDisplayIdentity(entry);
  adapter.closedLifecycleProofs ||= new Map();
  adapter.closedLifecycleProofs.set(entry.id,{identity:closedDisplayIdentity(entry),entry:JSON.parse(JSON.stringify(entry))});
}
function closedSnapshotCanReplace(next,current) {
  if(next?.id!==current?.id || next?.positionLifecycle!=='closed' || next.hasOpenPositions!==false
    || next.positionsConfirmed!==true)return false;
  return !!next._closedDisplayIdentity && settledDisplay(current) && next._closedDisplayIdentity===closedDisplayIdentity(current)
    || !!next._closedControlIdentity && settledClosingOutcome(current) && next._closedControlIdentity===closedControlIdentity(current);
}
function exactClosedOpening(entry,item,history) {
  const position=(entry.children || []).map(child=>child.positions?.[item.account]).find(row=>
    String(row?.openingTicket || row?.ticket)===item.ticket);
  const proof=history?.openingProof;
  return history?.phase==='closed' && proof?.kind==='exact-balanced-opening-v1' && position
    && proof.openingTicket===item.ticket && proof.positionId===String(position.ticket)
    && proof.symbol===entry.symbol && proof.side===entry.side
    && Number.isFinite(proof.volume) && Math.abs(proof.volume-position.volume)<1e-8
    && Number.isFinite(position.fillPrice) && Number.isFinite(proof.fillPrice)
    && Math.abs(proof.fillPrice-position.fillPrice)<1e-8;
}
function cancelledAdmissionShape(entry,expectedId) {
  return !!entry && typeof entry==='object' && !Array.isArray(entry) && entry.id===expectedId
    && entry.phase==='stopped' && entry.cancelledBeforeAdmission===true
    && entry.accounts && typeof entry.accounts==='object' && !Array.isArray(entry.accounts) && !Object.keys(entry.accounts).length
    && Array.isArray(entry.children) && !entry.children.length
    && Number.isFinite(entry.createdAt) && entry.createdAt>=0 && Number.isFinite(entry.updatedAt) && entry.updatedAt>=entry.createdAt
    && Object.keys(entry).every(key=>CANCEL_FIELDS.has(key));
}
function confirmAdmissionCancellation(entry,expectedId,response,request={}) {
  if(![38,39].includes(response?.version) || !response.capabilities?.includes(ADMISSION_CANCEL_CAPABILITY)
    || !cancelledAdmissionShape(entry,expectedId))throw Error('VPS не подтвердил отмену до запуска набора');
  // This local marker is created only after the authenticated, versioned proof.
  // Neither an empty GET result nor an empty account list can create it.
  return {...entry,...(request.symbol ? {symbol:request.symbol} : {}),...(request.side ? {side:request.side} : {}),
    ...(request.executionPolicy ? {executionPolicy:request.executionPolicy} : {}),_admissionCancelConfirmed:true};
}
function unfilledEntryCanReset(entry) {
  if(entry?.cancelledBeforeAdmission!==undefined)
    return entry._admissionCancelConfirmed===true && cancelledAdmissionShape(entry,entry.id);
  const accounts=Object.values(entry?.accounts || {});
  return !!entry && TERMINAL.has(entry.phase) && accounts.length>0
    && accounts.every(account=>Number(account.filledVolume)===0) && !unresolved(entry)
    && !(entry.children || []).some(child=>Object.values(child.positions || {}).some(position=>Number(position.volume)>0))
    && entry.hasOpenPositions!==true && !entry._readError;
}
function entryWatch(entry) {
  const rows=new Map();
  for(const [account,details] of Object.entries(entry.accounts || {})) {
    let found=false;
    for(const child of entry.children || []) {
      const position=child.positions?.[account];
      const result=child.result?.accountResults?.[account]
        || (String(child.result?.primaryAccount)===account ? child.result : null);
      // Hybrid's complete parent captures every actual fill in positions.
      // Definitively cancelled, unfilled support orders need no position watch;
      // their final cancellation is already proven by the durable child state.
      const ticket=position?.openingTicket || position?.ticket
        || (entry.executionPolicy!==HYBRID_POLICY && result?.status==='ok' ? result.providerOrderId : null);
      if(!ticket)continue;
      if(!/^[1-9][0-9]{0,19}$/.test(String(ticket)))throw Error('Не подтверждён номер входа');
      rows.set(account+':'+ticket,{account,ticket:String(ticket),symbol:entry.symbol});found=true;
    }
    if(Number(details.filledVolume)>0 && !found)throw Error('Нет точных номеров исполненной позиции');
  }
  if(!rows.size || rows.size>MAX_WATCH)throw Error('Не подтверждены номера входа на счетах');
  return [...rows.values()];
}
function rememberEntry(adapter,entry) {
  if(!entry?.id)return;
  adapter.chunkLifecycleEntries ||= new Map();
  if(retainControlEntry(adapter.chunkLifecycleEntries.get(entry.id),entry))return;
  const closed=adapter.closedControlProofs?.get(entry.id);
  if(closed && settledClosingOutcome(entry) && closed.identity===closedControlIdentity(entry)) {
    adapter.chunkLifecycleEntries.set(entry.id,JSON.parse(JSON.stringify(closed.entry)));return;
  }
  adapter.closedControlProofs?.delete(entry.id);
  const display=adapter.closedLifecycleProofs?.get(entry.id);
  if(display && settledDisplay(entry) && display.identity===closedDisplayIdentity(entry)) {
    adapter.chunkLifecycleEntries.set(entry.id,JSON.parse(JSON.stringify(display.entry)));return;
  }
  adapter.closedLifecycleProofs?.delete(entry.id);
  // Record positive evidence separately from optional fast-view fields. This
  // is scheduling metadata only, never a replacement for broker exit proof.
  adapter.chunkLifecycleOpenOrder ||= new Map();
  if(entry.hasOpenPositions===true && !adapter.chunkLifecycleOpenOrder.has(entry.id)) {
    adapter.chunkLifecycleObservation=(adapter.chunkLifecycleObservation || 0)+1;
    adapter.chunkLifecycleOpenOrder.set(entry.id,adapter.chunkLifecycleObservation);
  }
  adapter.chunkLifecycleEntries.set(entry.id,JSON.parse(JSON.stringify(entry)));
  // Retain unresolved/live records. Old final display receipts are expendable.
  if(adapter.chunkLifecycleEntries.size>100)for(const [id,value] of adapter.chunkLifecycleEntries) {
    if(value.positionLifecycle==='closed' || unfilledEntryCanReset(value)) {
      adapter.chunkLifecycleEntries.delete(id);adapter.closedControlProofs?.delete(id);
      adapter.closedLifecycleProofs?.delete(id);adapter.chunkLifecycleOpenOrder?.delete(id);adapter.chunkLifecycleChecks?.delete(id);
    }
    if(adapter.chunkLifecycleEntries.size<=100)break;
  }
}
async function goneEntryWatch(adapter,entry) {
  // A very fast SL/TP can happen before the UI ever sees a filled child.
  // Recover only this parent's durable, broker-confirmed opening identities.
  if(entry.executionPolicy===HYBRID_POLICY)throw Error('Нужен точный полный журнал гибридного набора');
  const state=await adapter.managementState({maxAgeMs:1000}),watch=[];
  if(!supportsLifecycle(state,entry) || !Array.isArray(state.entryPlans) || state.entryPlans.some(row=>row.id===entry.id))
    throw Error('Окончание входа ещё не подтверждено');
  const accounts=Object.keys(entry.accounts || {});
  if(!accounts.length)throw Error('Нет подтверждённого состава счетов входа');
  for(const account of accounts) {
    const health=state.accountStates?.[account];
    if(!health?.connected || health.evidenceReady===false || String(health.account)!==account || health.server!==adapter.config.server)
      throw Error('Нет свежего подтверждения всех счетов');
    const prefix=entry.id+':c';
    const rules=(health.timeRules || []).filter(rule=>{
      if(rule.symbol!==entry.symbol || !String(rule.id).startsWith(prefix) || rule.openingId!==rule.id)return false;
      const match=rule.id.slice(prefix.length).match(/^([1-9][0-9]?)(?::([0-9]+))?$/);
      return match && Number(match[1])<=50 && (account===String(adapter.config.account) ? !match[2] : match[2]===account);
    });
    if(!rules.length)throw Error('Нет точных подтверждений быстро завершившегося входа');
    for(const rule of rules) {
      if(rule.state==='rejected' && rule.result?.status==='rejected' && !rule.ticket)continue;
      if(rule.state!=='completed' || rule.result?.status!=='ok' || !['closed','cancelled'].includes(rule.result.phase)
        || !/^[1-9][0-9]{0,19}$/.test(String(rule.ticket)))throw Error('Исход входа ещё уточняется у брокера');
      watch.push({account,ticket:String(rule.ticket),symbol:entry.symbol});
    }
  }
  if(!watch.length || watch.length>100)throw Error('Нет подтверждённых номеров завершившегося входа');
  return watch;
}
function exactHybridEntry(entry,current) {
  const accounts=Object.keys(current.accounts || {}).sort();
  const lanes=['independent-v4','independent-v5'].includes(current.accountExecution);
  if(!entry || entry.id!==current.id || entry.symbol!==current.symbol || entry.side!==current.side
    || entry.accountExecution!==current.accountExecution
    || entry.executionPolicy!==HYBRID_POLICY || !accounts.length
    || Object.keys(entry.accounts || {}).sort().join(',')!==accounts.join(',')
    || !['armed','running','paused','sending','completed','stopped','unknown'].includes(entry.phase)
    || !Array.isArray(entry.children) || entry.children.length>(lanes ? 512*accounts.length : 512))
    throw Error('Точный журнал набора не подтвердил исходный вход и состав счетов');
  for(const account of accounts) {
    const details=entry.accounts[account];
    if(!Number.isFinite(details?.filledVolume) || details.filledVolume<0
      || !Number.isFinite(details.targetVolume) || details.targetVolume<=0 || details.filledVolume>details.targetVolume+1e-7)
      throw Error('Точный журнал набора не подтвердил объёмы счетов');
  }
  const captured=Object.fromEntries(accounts.map(account=>[account,0]));
  const counts=Object.fromEntries(accounts.map(account=>[account,0]));
  const ownerSlots=new Map(),slotOwners=new Map();
  const identifiers=new Set();let previous=0;
  for(let index=0;index<entry.children.length;index++) {
    const child=entry.children[index];
    const owners=Object.keys(child?.volumes || {}),owner=owners[0];
    // JSON object keys containing account numbers are reordered by JS. Infer
    // and verify the captured slot from immutable child IDs, never key order.
    const slot=Number.isSafeInteger(child?.index) ? (child.index-1)%32 : -1;
    if(lanes && (!accounts.includes(owner) || slot<0 || slot>=accounts.length
      || ownerSlots.has(owner) && ownerSlots.get(owner)!==slot
      || slotOwners.has(slot) && slotOwners.get(slot)!==owner))
      throw Error('Точный журнал набора не подтвердил принадлежность части счёту');
    const expected=current.accountExecution==='independent-v5' ? child.index : lanes && accounts.includes(owner) ? counts[owner]*32+slot+1 : index+1;
    if(lanes && (owners.length!==1 || !accounts.includes(owner))
      || child?.id!==entry.id+':h'+expected || child.index!==expected
      || identifiers.has(child.id) || child.index<=previous
      || Object.keys(child.positions || {}).some(account=>!accounts.includes(account) || lanes && account!==owner))
      throw Error('Точный журнал набора не подтвердил полный список частей');
    identifiers.add(child.id);previous=child.index;
    if(lanes){counts[owner]++;ownerSlots.set(owner,slot);slotOwners.set(slot,owner);}
    for(const [account,position] of Object.entries(child.positions || {})) {
      if(!Number.isFinite(position?.volume) || position.volume<=0)
        throw Error('Точный журнал набора не подтвердил объёмы частей');
      captured[account]+=position.volume;
    }
  }
  if(entry.phase==='completed' && accounts.some(account=>Math.abs(captured[account]-entry.accounts[account].targetVolume)>1e-7))
    throw Error('Точный журнал завершённого набора не содержит всего целевого объёма');
  if(accounts.some(account=>Math.abs(captured[account]-entry.accounts[account].filledVolume)>1e-7)) {
    const error=Error('Точный журнал набора содержит неполный список исполненных частей');
    // Protection refresh can replace acquired volume with remaining volume.
    // This is eligible for a fresh full-exit proof, never a relaxed validator.
    if(accounts.every(account=>captured[account]<=entry.accounts[account].targetVolume+1e-7
      && entry.accounts[account].filledVolume<=captured[account]+1e-7))error.code='HYBRID_REDUCED_VOLUME';
    throw error;
  }
  return entry;
}
async function confirmedHybridEntry(adapter,entry,current) {
  try {return exactHybridEntry(entry,current);} catch(error) {
    if(error.code!=='HYBRID_REDUCED_VOLUME' || !(TERMINAL.has(entry.phase) || settledIndependentOutcome(entry)) || unresolved(entry)
      || entry.protectionPending || entry.closurePending || entry.reconciliation?.status==='checking'
      || ['independent-v4','independent-v5'].includes(entry.accountExecution) && Object.keys(entry.accounts).some(account=>
        !TERMINAL.has(entry.lanePhases?.[account]) && !settledIndependentOutcome(entry))
      || Object.entries(entry.accounts).some(([account,details])=>Math.abs(details.targetVolume
        -entry.children.reduce((sum,child)=>sum+(child.positions?.[account]?.volume || 0),0))>1e-7))throw error;
    const watch=entryWatch(entry),closeDetails={};
    if(watch.length!==entry.children.reduce((sum,child)=>sum+Object.keys(child.positions || {}).length,0))throw error;
    for(let offset=0;offset<watch.length;offset+=WATCH_BATCH) {
      const batch=watch.slice(offset,offset+WATCH_BATCH);
      const state=await adapter.managementState({maxAgeMs:0,watch:batch});
      if(!supportsLifecycle(state,entry))throw error;
      for(const item of batch) {
        const health=state.accountStates?.[item.account],history=health?.openingStates?.[item.ticket];
        if(!health?.connected || health.evidenceReady===false || String(health.account)!==item.account || health.server!==adapter.config.server
          || !Array.isArray(health.orders) || health.orders.some(row=>row.symbol===item.symbol
            && [row.ticket,row.identifier].some(id=>id!=null
              && [item.ticket,history?.ticket,history?.positionId].includes(String(id))))
          || !exactClosedOpening(entry,item,history))throw error;
        (closeDetails[item.account] ||= []).push({ticket:item.ticket,phase:'closed',exitReasons:history.exitReasons || []});
      }
    }
    // Local display receipt only. Broker/parent quantities and protection are
    // never written. Every historical fill has just been proved fully closed.
    const restored=JSON.parse(JSON.stringify(entry));
    for(const [account,details] of Object.entries(restored.accounts)) {
      details.filledVolume=restored.children.reduce((sum,child)=>sum+(child.positions?.[account]?.volume || 0),0);
      details.pendingVolume=0;details.freeVolume=Math.max(0,details.targetVolume-details.filledVolume);
    }
    exactHybridEntry(restored,current);
    delete restored._readError;
    delete restored._readErrorKind;
    return {...restored,closeDetails,positionLifecycle:'closed',hasOpenPositions:false,positionsConfirmed:true};
  }
}
async function reconcileEntries(adapter,entries) {
  const present=new Set(entries.map(entry=>entry.id));
  for(const entry of entries)rememberEntry(adapter,entry);
  const known=adapter.chunkLifecycleEntries || new Map();
  const candidates=[];
  for(const entry of known.values()) {
    if(entry._armUnconfirmed || entry.positionLifecycle==='closed' || unfilledEntryCanReset(entry))continue;
    if(present.has(entry.id) && entry.lifecycleDeferred===true && !TERMINAL.has(entry.phase) && !settledLifecycle(entry))continue;
    if(!present.has(entry.id)) {
      entry._readError='Вход исчез из краткого списка; проверяем точные сделки у брокера.';
      entry._readErrorKind='checking';
    }
    const filledAccounts=Object.entries(entry.accounts || {}).filter(([account,value])=>Number(value.filledVolume)>0
      || (entry.children || []).some(child=>Number(child.positions?.[account]?.volume)>0)).map(([account])=>account);
    const missingFilled=filledAccounts.some(account=>!entry.livePositions?.some(row=>String(row.account)===account));
    if(filledAccounts.length && missingFilled || !present.has(entry.id) && Object.keys(entry.accounts || {}).length>0)candidates.push(entry);
  }
  adapter.chunkLifecycleChecks ||= new Map();
  const turn=(adapter.chunkLifecycleCursor || 0)+1;
  // Prefer the latest observed position on two of three existing polls. The
  // third poll services the oldest unchecked entry, so old receipts cannot
  // starve and cannot put the current SL exit at the end of a long queue.
  const recent=[...candidates].sort((a,b)=>Number(adapter.chunkLifecycleOpenOrder?.has(b.id))
    -Number(adapter.chunkLifecycleOpenOrder?.has(a.id)) || (Number(b.createdAt) || 0)-(Number(a.createdAt) || 0)
    || (adapter.chunkLifecycleOpenOrder?.get(b.id) || 0)-(adapter.chunkLifecycleOpenOrder?.get(a.id) || 0));
  const fair=[...candidates].sort((a,b)=>(adapter.chunkLifecycleChecks.get(a.id) || 0)-(adapter.chunkLifecycleChecks.get(b.id) || 0));
  let candidate=turn%3 && adapter.chunkLifecycleOpenOrder?.has(recent[0]?.id) ? recent[0] : fair[0];
  if(candidate){adapter.chunkLifecycleCursor=turn;adapter.chunkLifecycleChecks.set(candidate.id,turn);}
  // Inspect one candidate per existing UI poll, with at most four bounded
  // history batches; never create another price subscription.
  if(candidate)try {
    const unknownOutcome=candidate.phase==='unknown';
    if(candidate.executionPolicy===HYBRID_POLICY && (!present.has(candidate.id) || unknownOutcome)) {
      if(adapter.config.routePrefix)throw Error('Точный журнал набора доступен только через основной счёт');
      const response=await adapter.request('/entry-plan?id='+encodeURIComponent(candidate.id));
      const restored=await confirmedHybridEntry(adapter,response?.entry,candidate);
      rememberEntry(adapter,restored);
      candidate=known.get(restored.id) || restored;
      if(candidate.positionLifecycle==='closed'){retainClosedDisplay(adapter,candidate);return [...known.values()];}
      if(!TERMINAL.has(candidate.phase) && !settledLifecycle(candidate))throw Error('Точный журнал ещё не подтвердил окончание набора');
      if(unfilledEntryCanReset(candidate))return [...known.values()];
    }
    if(candidate.executionPolicy===HYBRID_POLICY) {
      const confirmed=await confirmedHybridEntry(adapter,candidate,candidate);
      if(confirmed.positionLifecycle==='closed') {
        retainClosedDisplay(adapter,confirmed);rememberEntry(adapter,confirmed);return [...known.values()];
      }
    }
    if(candidate.executionPolicy===HYBRID_POLICY && !hasFills(candidate))
      throw Error('Точный журнал не подтвердил исполненные части; восстановление по краткому списку запрещено');
    const watch=hasFills(candidate) ? entryWatch(candidate) : await goneEntryWatch(adapter,candidate);
    let state;
    const closeDetails={};let closed=0,live=0;
    // One lifecycle check obtains every batch afresh. Never combine evidence
    // from separate UI polls; the server's watch budget remains 100 per GET.
    for(let offset=0;offset<watch.length;offset+=WATCH_BATCH) {
      const batch=watch.slice(offset,offset+WATCH_BATCH);
      state=await adapter.managementState({maxAgeMs:0,watch:batch});
      if(!supportsLifecycle(state,candidate))throw Error('Неизвестная версия подтверждения закрытия VPS');
      for(const item of batch) {
        const health=state.accountStates?.[item.account];
        if(!health?.connected || health.evidenceReady===false || String(health.account)!==item.account || health.server!==adapter.config.server
          || !Array.isArray(health.orders))throw Error('Нет свежего подтверждения всех счетов');
        const history=health.openingStates?.[item.ticket];
        const current=health.orders.some(row=>row.symbol===item.symbol
          && [row.ticket,row.identifier].some(id=>id!=null && [item.ticket,history?.ticket,history?.positionId].includes(String(id))));
        if(current){live++;continue;}
        if(!['closed','cancelled'].includes(history?.phase))throw Error('Закрытие ещё не подтверждено историей брокера');
        if(candidate.phase==='unknown' && !exactClosedOpening(candidate,item,history))
          throw Error('Нужна точная сбалансированная история каждой исполненной части');
        (closeDetails[item.account] ||= []).push({ticket:item.ticket,phase:history.phase,exitReasons:history.exitReasons || []});closed++;
      }
    }
    delete candidate._readError;
    delete candidate._readErrorKind;
    candidate.closeDetails=closeDetails;
    const closing=settledClosingOutcome(candidate);
    const terminal=TERMINAL.has(candidate.phase) || settledLifecycle(candidate) || candidate.executionPolicy!==HYBRID_POLICY
      && supportsLifecycle(state,candidate) && Array.isArray(state.entryPlans)
      && !state.entryPlans.some(entry=>entry.id===candidate.id);
    if(closed===watch.length && !live && !unresolved(candidate) && terminal) {
      if(!TERMINAL.has(candidate.phase))candidate.phase='stopped';
      candidate.positionLifecycle='closed';candidate.hasOpenPositions=false;candidate.positionsConfirmed=true;
      candidate.lifecycleDeferred=false;
      retainClosedDisplay(adapter,candidate);
      if(closing) {
        // This retires the position display only. The original close command's
        // unknown receipt stays untouched and is never retried or called ok.
        candidate.controlOutcomeUnresolved=true;
        candidate._closedControlIdentity=closedControlIdentity(candidate);
        adapter.closedControlProofs ||= new Map();
        adapter.closedControlProofs.set(candidate.id,{identity:closedControlIdentity(candidate),entry:JSON.parse(JSON.stringify(candidate))});
      }
    } else if(live) {
      candidate.hasOpenPositions=true;candidate.positionsConfirmed=true;candidate.lifecycleDeferred=false;
      if(closed)candidate.positionLifecycle='partial-closed';
    }
  } catch(error) {candidate._readError=error.message;candidate._readErrorKind=readErrorKind(error);}
  return [...known.values()];
}
module.exports={reconcileEntries,rememberEntry,entryWatch,unfilledEntryCanReset,confirmAdmissionCancellation,
  exactHybridEntry,closedSnapshotCanReplace,ADMISSION_CANCEL_CAPABILITY};

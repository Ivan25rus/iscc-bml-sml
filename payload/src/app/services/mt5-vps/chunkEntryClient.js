const {randomUUID}=require('crypto');
const {performance}=require('perf_hooks');
const {followers}=require('./sizing');
const {deadlineForNewTrade}=require('./timeExit');
const {CommandJournal}=require('./commandJournal');
const admission=require('./entryAdmission');

const CAPABILITY='chunked-breakout';
const STOP_PRIORITY_CAPABILITY='entry-stop-priority-v1';
const PHASES=new Set(['armed','running','paused','sending','completed','stopped','unknown']);
const DATE_FIELDS=new Set(['expiresAt','closeAtUtc','closeLocalDate']);
const PREPARATION_FIELDS=['ipcDispatchMs','trackedValidationMs','snapshotReadMs','planValidationMs','admissionMs','requestBuildMs'];
const elapsed=since=>Number((performance.now()-since).toFixed(2));
function preparationTimings(value={}) {
  if(!value || typeof value!=='object')return {};
  return Object.fromEntries(PREPARATION_FIELDS.filter(key=>Number.isFinite(value[key]) && value[key]>=0).map(key=>[key,value[key]]));
}
function positive(value,label) {
  const number=Number(value);
  if (!Number.isFinite(number) || number<=0) throw Error('Неверный параметр набора: '+label);
  return number;
}
function commandId(value,maximum=100) {
  if (typeof value!=='string' || value.length>maximum || !/^[A-Za-z0-9_.:-]+$/.test(value)) throw Error('Неверный номер команды набора');
  return value;
}
function buildEntryRequest(adapter,plan,{requestId=randomUUID(),cardPayload}={},now=Date.now()) {
  const deadline=deadlineForNewTrade(adapter.config,now);
  if (!deadline) throw Error('Для набора частями включите закрытие по времени: у набора должен быть ограниченный срок');
  const side=plan?.orderSide;
  if (!['buy','sell'].includes(side)) throw Error('Неверное направление набора');
  const trigger=positive(plan.referencePrice,'цена срабатывания'),sl=positive(plan.stopPrice,'стоп');
  if (side==='buy' ? sl>=trigger : sl<=trigger) throw Error('Стоп должен находиться за ценой срабатывания');
  const tickSize=positive(plan.tickSize,'шаг цены');
  const takeProfitPts=Number(plan.takeProfitPts ?? 0);
  if (!Number.isFinite(takeProfitPts) || takeProfitPts<0) throw Error('Неверный тейк набора');
  const maxGap=Number(plan.entryMode==='side-zone' ? plan.zoneRadius : plan.maxGap);
  if (!Number.isFinite(maxGap) || maxGap<0) throw Error('Неверный параметр набора: допуск цены');
  const request={id:commandId(requestId),symbol:adapter.brokerSymbol(plan.ticker),side,
    trigger,sl,tp:takeProfitPts ? trigger+(side==='buy' ? 1 : -1)*takeProfitPts*tickSize : 0,
    volume:positive(plan.totalQty,'общий объём'),chunkVolume:positive(plan.chunkVolume,'часть объёма'),
    maxGap,riskUsd:positive(plan.riskUsd,'общий риск'),
    level:positive(plan.level,'уровень'),tickSize,expiresAt:now+3000,...deadline};
  if (plan.entryMode!==undefined && !['breakout','zone','boundary','side-zone'].includes(plan.entryMode)) throw Error('Неверный режим входа');
  if (['zone','boundary','side-zone'].includes(plan.entryMode)) {
    const radius=Number(plan.zoneRadius),multiplier=Number(plan.zoneMultiplier);
    if (!Number.isFinite(radius) || radius<0 || ![1,2].includes(multiplier) || trigger-radius<=0)
      throw Error('Неверная зона входа по рынку');
    const lower=plan.entryMode==='side-zone' ? trigger-(side==='sell' ? radius : 0)
      : plan.entryMode==='boundary' ? trigger-(side==='sell' ? maxGap : 0) : trigger-radius;
    const upper=plan.entryMode==='side-zone' ? trigger+(side==='buy' ? radius : 0)
      : plan.entryMode==='boundary' ? trigger+(side==='buy' ? maxGap : 0) : trigger+radius;
    if (plan.entryMode==='side-zone' && Math.abs(trigger-request.level)>1e-8)
      throw Error('Диапазон BML/SML должен начинаться от уровня LT');
    if (plan.entryMode==='boundary' && Math.abs(trigger-(request.level+(side==='buy' ? radius : -radius)))>1e-8)
      throw Error('Цена входа должна равняться уровню плюс/минус люфт');
    if (side==='buy' ? (plan.stopMode!=='entry' && sl>=lower) || request.tp && request.tp<=upper
      : (plan.stopMode!=='entry' && sl<=upper) || request.tp && request.tp>=lower)
      throw Error('Зона рынка пересекает SL или TP');
    request.entryMode=plan.entryMode;request.zoneRadius=radius;request.zoneMultiplier=multiplier;
  }
  if (plan.executionPolicy!==undefined) {
    if (!['market-limit','hybrid-market-limit-v1'].includes(plan.executionPolicy) || plan.entryMode!=='side-zone') throw Error('Неверный режим лимитки и рынка');
    request.executionPolicy=plan.executionPolicy;
    if(plan.executionPolicy==='hybrid-market-limit-v1') {
      if(adapter.capabilities?.includes('independent-account-engines-v5') && adapter.capabilities?.includes('entry-initial-sizing-only-v1'))request.accountExecution='independent-v5';
      else if(adapter.capabilities?.includes('independent-account-lanes-v4'))request.accountExecution='independent-v4';
      else if(adapter.capabilities?.includes('parallel-account-entry-v3'))request.accountExecution='independent-v3';
      else if(adapter.capabilities?.includes('independent-account-entry-v2'))request.accountExecution='independent-v2';
    }
  }
  if (request.tp<0 || !Number.isFinite(request.tp)) throw Error('Неверная цена тейка набора');
  if(plan.fillPriority!==undefined) {
    if(!['risk','volume'].includes(plan.fillPriority) || !['independent-v4','independent-v5'].includes(request.accountExecution))
      throw Error('Обновите VPS для настройки приоритета добора');
    request.fillPriority=plan.fillPriority;
  }
  if (plan.stopMode!==undefined) {
    if (!['entry','level'].includes(plan.stopMode)) throw Error('Неверный режим стопа набора');
    request.stopMode=plan.stopMode;
  }
  if (plan.stopOffsetPts!==undefined) request.stopOffsetPts=positive(plan.stopOffsetPts,'размер стопа');
  if (plan.chunkPercent!==undefined) {
    const percent=Number(plan.chunkPercent);
    if (!Number.isFinite(percent) || percent>100 || (request.executionPolicy==='hybrid-market-limit-v1' ? percent<=0 : percent<2))
      throw Error(request.executionPolicy==='hybrid-market-limit-v1' ? 'Размер части: больше 0 и не больше 100%' : 'Размер части: от 2 до 100%');
    request.chunkPercent=percent;
  }
  if (request.executionPolicy==='hybrid-market-limit-v1' && (!Number.isFinite(request.chunkPercent)
    || request.chunkPercent<=0 || request.chunkPercent>100 || !['entry','level'].includes(request.stopMode)
    || !Number.isFinite(request.stopOffsetPts))) throw Error('Для БМЛ/СМЛ нужны процент части и параметры общего стопа');
  if (cardPayload?.cid!==undefined) {
    if (typeof cardPayload.cid!=='string' || cardPayload.cid.length>128) throw Error('Неверный номер карточки');
    request.cid=cardPayload.cid;
  }
  const copies=followers(adapter.config).filter(row=>row.enabled);
  if (copies.length) request.mirrorAccounts=copies.map(({account,multiplier})=>({account,multiplier}));
  const riskLimit=require('./entryRisk').checkEntryRisk(adapter.config,request.riskUsd);
  if(riskLimit>0)request.entryRiskLimitUsd=riskLimit;
  if(adapter.capabilities?.includes(admission.CAPABILITY)) {
    request.admissionContract=admission.CAPABILITY;
    request.requiredCapabilities=admission.requiredCapabilities(request);
  }
  return request;
}
function identity(operation,request) {
  return JSON.stringify([operation,Object.fromEntries(Object.entries(request).filter(([key])=>!DATE_FIELDS.has(key)))]);
}
function delivery(adapter,operation,previous) {
  if(previous)return Object.fromEntries(['intentDelivery','intentTtlMs'].filter(key=>previous[key]!==undefined).map(key=>[key,previous[key]]));
  return adapter.capabilities?.includes('entry-intent-delivery-v1')
    ? {intentDelivery:'entry-intent-delivery-v1',intentTtlMs:operation==='stop' ? 60000 : 15000} : {};
}
async function settleRevokedContinuations(adapter,request,response) {
  if(!response.capabilities?.includes('entry-control-recovery-v1') || !request.cancelContinueIds)return;
  const proof=response.entryControlReceipt;
  if(!proof || proof.schema!=='entry-control-recovery-v1' || proof.id!==request.id || proof.entryId!==request.entryId
    || !Array.isArray(proof.cancelContinueIds) || JSON.stringify(proof.cancelContinueIds)!==JSON.stringify(request.cancelContinueIds))
    throw Error('VPS не подтвердил точный отзыв команд добора');
  const journal=journalFor(adapter),ids=new Set(proof.cancelContinueIds),changed=[];
  for(const row of journal?.records || [])if(ids.has(row.id) && row.operation==='entry-plan-continue'
    && row.payload?.entryId===request.entryId && row.result?.status==='unknown') {
    row.revokedBy={id:proof.id,entryId:proof.entryId};
    row.result={status:'rejected',reason:'Добор отозван подтверждённой остановкой',revokedBy:proof.id};
    row.updatedAt=Date.now();changed.push(row);
  }
  if(changed.length)await journal.save(changed);
}
function journalFor(adapter) {
  // Entry plans have their own durable receipts on the VPS. In particular they
  // must never be reconciled through the ordinary trade-command /receipt API.
  if (!adapter.chunkEntryJournal && adapter.commandJournal?.file)
    adapter.chunkEntryJournal=new CommandJournal(adapter.commandJournal.file+'.entry-plans.json', {asyncWrites:adapter.commandJournal.asyncWrites});
  return adapter.chunkEntryJournal;
}
function unavailable(adapter) {
  if (adapter.config.routePrefix) return 'Набор частями запускается только через основной счёт';
  if (adapter.closed || adapter.connectionRestarting || adapter.accountEnrollmentBusy) return 'Подключение обновляется. Дождись готовности счетов';
}
function capturedAccounts(adapter,request) {
  const accounts=new Map([[String(adapter.config.account),1]]);
  const copies=request.mirrorAccounts || (request.mirrorAccount ? [{account:request.mirrorAccount,multiplier:1}] : []);
  if(!Array.isArray(copies) || copies.length>16)throw Error('Не подтверждён сохранённый состав счетов набора');
  for(const copy of copies) {
    const account=String(copy?.account),multiplier=copy?.multiplier;
    if(!/^[1-9][0-9]{0,15}$/.test(account) || accounts.has(account) || !Number.isFinite(multiplier) || multiplier<=0 || multiplier>100)
      throw Error('Не подтверждён сохранённый состав счетов набора');
    accounts.set(account,multiplier);
  }
  return accounts;
}
function validateRecoveredEntry(adapter,response,request) {
  const lifecycle=require('./entryLifecycle'),entry=response?.entry;
  if(request.fillPriority!==undefined && !response?.capabilities?.includes('entry-fill-priority-v1'))
    throw Error('VPS не подтвердил настройку приоритета добора');
  if(request.accountExecution && (entry?.accountExecution!==request.accountExecution
    || !response.capabilities?.includes(request.accountExecution==='independent-v5' ? 'independent-account-engines-v5' : request.accountExecution==='independent-v4' ? 'independent-account-lanes-v4'
      : request.accountExecution==='independent-v3' ? 'parallel-account-entry-v3' : 'independent-account-entry-v2')))
    throw Error('VPS не подтвердил независимый набор по счетам');
  if(entry?.cancelledBeforeAdmission!==undefined)return lifecycle.confirmAdmissionCancellation(entry,request.id,response,request);
  const hybrid=request.executionPolicy==='hybrid-market-limit-v1';
  if(!(hybrid ? [35,36,37,38,39] : [34,35,36,37,38,39]).includes(response?.version)
    || !response.capabilities?.includes(CAPABILITY)
    || hybrid && !response.capabilities.includes('hybrid-market-limit-v1')
    || !entry || entry.id!==request.id || entry.symbol!==request.symbol || entry.side!==request.side || !PHASES.has(entry.phase)
    || entry.executionPolicy!==request.executionPolicy || (entry.entryMode || 'breakout')!==(request.entryMode || 'breakout'))
    throw Error('Точный журнал VPS не подтвердил исходный набор');
  for(const key of ['trigger','volume','maxGap','riskUsd','level','tickSize','stopOffsetPts','chunkPercent','zoneRadius','zoneMultiplier','closeAtUtc']) {
    if(request[key]!==undefined && (!Number.isFinite(entry[key]) || Math.abs(entry[key]-request[key])>1e-7))
      throw Error('Точный журнал VPS не подтвердил параметры набора');
  }
  // The broker can reduce the initial part to its per-order maximum. SL/TP can
  // subsequently be changed by the user's whole-parent protection controls.
  if(entry.fillPriority!==request.fillPriority || entry.stopMode!==request.stopMode || !Number.isFinite(entry.chunkVolume) || entry.chunkVolume<=0
    || entry.chunkVolume>request.chunkVolume+1e-7 || !entry.accounts || Array.isArray(entry.accounts)
    || !Array.isArray(entry.children) || entry.children.length>(hybrid ? 512*(['independent-v4','independent-v5'].includes(request.accountExecution) ? Object.keys(entry.accounts).length : 1) : 50))
    throw Error('Точный журнал VPS не подтвердил структуру набора');
  const accounts=capturedAccounts(adapter,request);
  if(Object.keys(entry.accounts).sort().join(',')!==[...accounts.keys()].sort().join(','))
    throw Error('Точный журнал VPS не подтвердил захваченные счета набора');
  for(const [account,multiplier] of accounts) {
    const details=entry.accounts[account];
    if(!details || !Number.isFinite(details.multiplier) || Math.abs(details.multiplier-multiplier)>1e-7
      || !Number.isFinite(details.targetVolume) || details.targetVolume<=0 || details.targetVolume>request.volume*multiplier+1e-7
      || account===String(adapter.config.account) && Math.abs(details.targetVolume-request.volume)>1e-7
      || !Number.isFinite(details.filledVolume) || details.filledVolume<0 || details.filledVolume>details.targetVolume+1e-7
      || !Number.isFinite(details.riskUsd) || Math.abs(details.riskUsd-request.riskUsd*multiplier)>1e-7)
      throw Error('Точный журнал VPS не подтвердил объём и риск счетов набора');
  }
  if(hybrid)lifecycle.exactHybridEntry(entry,{...request,accounts:entry.accounts});
  else {
    const captured=Object.fromEntries([...accounts.keys()].map(account=>[account,0]));
    for(const [index,child] of entry.children.entries()) {
      if(child?.id!==entry.id+':c'+(index+1) || child.index!==index+1
        || Object.keys(child.positions || {}).some(account=>!accounts.has(account)))
        throw Error('Точный журнал VPS не подтвердил список частей набора');
      for(const [account,position] of Object.entries(child.positions || {})) {
        if(!Number.isFinite(position?.volume) || position.volume<=0)throw Error('Не подтверждён объём части набора');
        captured[account]+=position.volume;
      }
    }
    if([...accounts.keys()].some(account=>Math.abs(captured[account]-entry.accounts[account].filledVolume)>1e-7))
      throw Error('Точный журнал VPS содержит неполные исполнения набора');
  }
  return entry;
}
function unknownArmCandidates(adapter,journal) {
  const candidates=new Map();
  for(const record of journal?.records || [])if(record.operation==='entry-plan-arm' && record.result?.status==='unknown') {
    const {ticket,...request}=record.payload || {};
    commandId(request.id);
    if(record.id!==request.id)throw Error('Повреждён журнал запуска набора');
    candidates.set(record.id,{record,request});
  }
  for(const [key,command] of adapter.chunkEntryCommands || [])if(command.operation==='arm' && command.result?.status==='unknown'
    && !candidates.has(command.request.id))candidates.set(command.request.id,{request:command.request,command});
  return [...candidates.values()].filter(({request})=>!adapter.chunkEntryCommands?.get('arm:'+request.id)?.pending);
}
async function saveRecoveredArm(adapter,journal,candidate,entry) {
  const result={status:'ok',entry};
  if(candidate.record) {
    const previous=candidate.record.result,updatedAt=candidate.record.updatedAt;
    candidate.record.result=result;candidate.record.updatedAt=Date.now();
    try {await journal.save([candidate.record]);}
    catch(error) {candidate.record.result=previous;candidate.record.updatedAt=updatedAt;throw error;}
  }
  const command=adapter.chunkEntryCommands?.get('arm:'+candidate.request.id);
  if(command && !command.pending){command.result=result;command.promise=Promise.resolve(result);}
  require('./entryLifecycle').rememberEntry(adapter,entry);
}
async function reconcileUnknownArms(adapter,entries) {
  const journal=journalFor(adapter),unconfirmed=unknownArmCandidates(adapter,journal),candidates=[];
  const output=new Map(entries.map(entry=>[entry.id,entry])),proofs=new Map();
  // Immutable cancellation evidence remains useful if the UI response was lost
  // after its journal save. Restore only our locally validated proof, never a
  // marker copied from an unvalidated short-list response.
  for(const record of journal?.records || []) {
    const entry=record.result?.entry,expected=record.operation==='entry-plan-arm' ? record.payload?.id
      : record.operation==='entry-plan-stop' ? record.payload?.entryId : undefined;
    if(record.result?.status==='ok' && entry?.id===expected && entry?._admissionCancelConfirmed===true
      && require('./entryLifecycle').unfilledEntryCanReset(entry)){output.set(entry.id,entry);proofs.set(entry.id,entry);}
  }
  const uncertain=(request,reason)=>{
    const current=output.get(request.id);
    const entry={...(current || {}),id:request.id,symbol:request.symbol,side:request.side,phase:'unknown',
      entryMode:request.entryMode,executionPolicy:request.executionPolicy,accountExecution:request.accountExecution,level:request.level,trigger:request.trigger,
      riskUsd:request.riskUsd,stopMode:request.stopMode,stopOffsetPts:request.stopOffsetPts,
      accounts:current?.accounts || {},children:current?.children || [],_armUnconfirmed:true,
      reason:reason || 'Проверяем результат запуска по точному журналу VPS. Повторный вход пока заблокирован.'};
    delete entry.positionLifecycle;delete entry._admissionCancelConfirmed;
    output.set(entry.id,entry);
    require('./entryLifecycle').rememberEntry(adapter,entry);
  };
  for(const candidate of unconfirmed) {
    const proof=proofs.get(candidate.request.id);
    if(proof)try {
      // An explicit stop may already have durably settled this launch even if
      // its original arm receipt was lost. Preserve that proof across restart.
      // A stop can precede even the local arm journal, so its validated proof
      // may have no display metadata. Recover that metadata from this exact
      // saved request before publishing or durably settling the lost arm.
      const restored={...proof,symbol:candidate.request.symbol,side:candidate.request.side,
        ...(candidate.request.executionPolicy ? {executionPolicy:candidate.request.executionPolicy} : {})};
      await saveRecoveredArm(adapter,journal,candidate,restored);
      output.set(candidate.request.id,restored);
    } catch(error) {uncertain(candidate.request,'Не удалось сохранить подтверждение отмены: '+error.message);}
    else {uncertain(candidate.request);candidates.push(candidate);}
  }
  if(!candidates.length)return [...output.values()];
  const candidate=candidates[(adapter.chunkArmRecoveryCursor || 0)%candidates.length];
  adapter.chunkArmRecoveryCursor=(adapter.chunkArmRecoveryCursor || 0)+1;
  try {
    // One exact parent per existing UI refresh. This GET cannot arm, stop,
    // cancel or close anything, and a missing result is never an empty fill.
    const response=await adapter.request('/entry-plan?id='+encodeURIComponent(candidate.request.id));
    const entry=validateRecoveredEntry(adapter,response,candidate.request);
    await saveRecoveredArm(adapter,journal,candidate,entry);
    output.set(entry.id,entry);
  } catch(error) {uncertain(candidate.request,'Не удалось подтвердить запуск: '+error.message+'. Повторная команда не отправляется.');}
  return [...output.values()];
}
async function retryExplicitStop(journal,record,send) {
  // This path is reached only by another explicit stop invocation. The durable
  // unknown intent and immutable command ID stay in place before transmission.
  record.result={status:'unknown',reason:'Повторная явная остановка сохранена перед отправкой; требуется подтверждение VPS'};
  record.updatedAt=Date.now();
  await journal.save([record]);
  try {record.result=await send();}
  catch(error) {record.result={status:'unknown',reason:error.message};}
  record.updatedAt=Date.now();
  try {await journal.save([record]);}
  catch {record.result={status:'unknown',reason:'Не удалось сохранить ответ остановки. Проверь статус набора на VPS.'};}
  return record.result;
}
async function runMutation(adapter,operation,request,{clickAt,preparation={}}={}) {
  preparation=preparationTimings(preparation);
  const reason=unavailable(adapter);
  if (reason) return {status:'rejected',provider:adapter.provider,reason,timings:preparation};
  let submitted=false;
  try {
    const key=operation+':'+request.id,signature=identity(operation,request);
    adapter.chunkEntryCommands ||= new Map();
    const existing=adapter.chunkEntryCommands.get(key);
    if (existing) {
      if (existing.signature!==signature) return {status:'rejected',provider:adapter.provider,reason:'Номер команды уже использован с другими параметрами'};
      if (operation!=='stop' || existing.pending || existing.result?.status==='ok') return await existing.promise;
    }
    const journalStarted=performance.now();
    const journal=journalFor(adapter),previous=journal?.records.find(row=>row.id===request.id);
    preparation.journalLoadMs=elapsed(journalStarted);
    if (previous) {
      const {ticket,...payload}=previous.payload;
      if (identity(operation,payload)!==signature) return {status:'rejected',provider:adapter.provider,reason:'Номер команды уже использован с другими параметрами'};
      if (operation!=='stop' || previous.result.status==='ok') return {...previous.result,provider:adapter.provider};
    }
    const run=(async()=>{
      const atomic=operation==='arm' && request.admissionContract===admission.CAPABILITY;
      const healthStarted=performance.now();let health;
      try {
        if(!atomic && operation!=='stop') {
          // This selects an already verified route only. The selected VPS
          // account still validates identity/protection before continuing.
          // A busy primary health RPC must not hold a peer's explicit resume.
          const independentContinue=operation==='continue' && adapter.capabilities?.includes('independent-account-engines-v5')
            && adapter.capabilities?.includes('entry-account-continue-v1');
          health=independentContinue ? {capabilities:adapter.capabilities} : await adapter.health();
        }
      }
      finally {preparation.healthReadMs=elapsed(healthStarted);}
      const capabilityStarted=performance.now();
      try {
        if(!atomic && operation==='arm' && request.fillPriority!==undefined
          && !health?.capabilities?.includes('entry-fill-priority-v1'))
          throw Error('Обновите VPS для настройки приоритета добора');
        if (!atomic && operation!=='stop' && (!health || health.connected===false || (operation==='arm' && health.tradingEnabled===false)
          || !health.capabilities?.includes(CAPABILITY))) throw Error('Обновите приёмник VPS для набора частями и проверьте подключение');
        if(operation==='continue' && (health.tradingEnabled===false || !health.capabilities?.includes('entry-account-continue-v1')))
          throw Error('Обновите VPS для продолжения добора по счёту');
        if (!atomic && operation==='arm' && ['zone','boundary','side-zone'].includes(request.entryMode) && !health.capabilities?.includes('market-zone-entry'))
          throw Error('Обновите приёмник VPS для входа по рынку в люфте');
        if (!atomic && operation==='arm' && ['zone','boundary','side-zone'].includes(request.entryMode) && request.stopMode==='entry'
          && !health.capabilities?.includes('market-zone-entry-stop'))
          throw Error('Обновите приёмник VPS для стопа от входа BML/SML');
        if (!atomic && operation==='arm' && request.entryMode==='boundary' && !health.capabilities?.includes('market-offset-entry'))
          throw Error('Обновите приёмник VPS для входа BML/SML по уровню плюс/минус люфт');
        if (!atomic && operation==='arm' && request.entryMode==='side-zone' && !health.capabilities?.includes('market-side-zone-entry'))
          throw Error('Обновите приёмник VPS для включительных диапазонов BML/SML');
        if (!atomic && operation==='arm' && request.executionPolicy==='market-limit' && !health.capabilities?.includes('market-limit-side-zone-entry'))
          throw Error('Обновите приёмник VPS для лимитки и рынка в BML/SML');
        if (!atomic && operation==='arm' && request.executionPolicy==='hybrid-market-limit-v1' && !health.capabilities?.includes('hybrid-market-limit-v1'))
          throw Error('Обновите приёмник VPS для процентного набора лимиткой и рынком с общим стопом БМЛ/СМЛ');
        const unavailableReason=unavailable(adapter);
        if (unavailableReason) throw Error(unavailableReason);
      } finally {preparation.capabilityValidationMs=elapsed(capabilityStarted);}
      let journalBeforeSend;
      const send=async journalTiming=>{
        if(journalTiming)Object.assign(preparation,journalTiming);
        preparation.journalBeforeSendMs=elapsed(journalBeforeSend);
        submitted=true;
        const beforeSend=Date.now();
        // Expiry is transport metadata excluded from the durable identity.
        // Local fsync must not spend the operator intent's delivery window.
        request.expiresAt=request.intentTtlMs
          ? require('./entryIntentClock').intentExpiry(adapter,request.intentTtlMs,beforeSend)
          : beforeSend+3000;
        const response=await adapter.request(atomic ? '/entry-plan-admit' : operation==='arm' ? '/entry-plan'
          : operation==='continue' ? '/entry-plan-continue' : '/entry-plan-stop',request,{execution:true});
        const validationStarted=performance.now();
        // Stopping an exact parent needs no preliminary health round trip.
        // The authenticated stop reply supplies its own protocol evidence,
        // including proof when cancellation beat the original admission.
        if(operation==='stop')health=response;
        if(atomic) {
          if(response?.admissionContract!==admission.CAPABILITY)throw Error('VPS не подтвердил протокол приёма набора');
          health=response;
          admission.validateHealth(health,operation,request);
        }
        if (Object.hasOwn(response || {},'entryResult')) {
          const result=response.entryResult;
          if (!result || typeof result!=='object' || Array.isArray(result) || result.status!=='rejected'
            || typeof result.reason!=='string' || !result.reason.trim() || response.entry!==undefined)
            throw Error('MT5 VPS: некорректное подтверждение отказа набора');
          preparation.replyValidationMs=elapsed(validationStarted);
          return {status:'rejected',reason:result.reason,timings:{...preparation,server:response.serverTiming,transport:response.transportTiming,
            macBeforeSendMs:Number.isFinite(clickAt) ? Math.max(0,beforeSend-clickAt) : undefined,
            macClickToResultMs:Number.isFinite(clickAt) ? Math.max(0,Date.now()-clickAt) : undefined}};
        }
        let entry=response?.entry;
        const expected=operation==='arm' ? request.id : request.entryId;
        if (!entry || typeof entry!=='object' || Array.isArray(entry) || entry.id!==expected
          || !PHASES.has(entry.phase)) throw Error('MT5 VPS: некорректное подтверждение набора');
        if(entry.cancelledBeforeAdmission!==undefined) {
          if(![38,39].includes(health.version) || !health.capabilities?.includes(require('./entryLifecycle').ADMISSION_CANCEL_CAPABILITY))
            throw Error('Обновите VPS для подтверждения отмены до запуска набора');
          const original=operation==='arm' ? request : journal?.records.find(row=>row.operation==='entry-plan-arm' && row.id===expected)?.payload;
          entry=require('./entryLifecycle').confirmAdmissionCancellation(entry,expected,response,original);
        }
        adapter.managementSnapshot=null;adapter.managementStateView=null;
        if(operation==='stop')await settleRevokedContinuations(adapter,request,response);
        preparation.replyValidationMs=elapsed(validationStarted);
        return {status:'ok',entry,timings:{...preparation,server:response.serverTiming,transport:response.transportTiming,
          macBeforeSendMs:Number.isFinite(clickAt) ? Math.max(0,beforeSend-clickAt) : undefined,
          macClickToResultMs:Number.isFinite(clickAt) ? Math.max(0,Date.now()-clickAt) : undefined}};
      };
      // Distinct scopes allow an uncertain arm to be stopped explicitly. Arming
      // is never replayed. Only a new explicit stop click may resend the SAME
      // idempotent stop command after a lost reply; polling never calls this.
      journalBeforeSend=performance.now();
      const result=journal ? previous && operation==='stop' ? await retryExplicitStop(journal,previous,send)
        : await journal.run('entry-plan-'+operation,{...request,ticket:operation+':'+(request.entryId || request.id)
          +(operation==='continue' ? ':'+request.account : '')},send) : await send();
      if(result.status==='ok' && result.entry)require('./entryLifecycle').rememberEntry(adapter,result.entry);
      return {...result,provider:adapter.provider,timings:{...preparation,...result.timings}};
    })().catch(error=>({status:submitted ? 'unknown' : 'rejected',provider:adapter.provider,reason:error.message,timings:{...preparation}}));
    const command={signature,promise:run,pending:true,operation,request:JSON.parse(JSON.stringify(request))};
    adapter.chunkEntryCommands.set(key,command);
    const result=await run;command.pending=false;command.result=result;
    return result;
  } catch(error) {return {status:submitted ? 'unknown' : 'rejected',provider:adapter.provider,reason:error.message,timings:{...preparation}};}
}
async function armChunkEntry(adapter,plan,options={}) {
  const started=performance.now(),preparation=preparationTimings(options.preparation);
  try {
    const request={...buildEntryRequest(adapter,plan,options),...delivery(adapter,'arm')};
    preparation.requestBuildMs=elapsed(started);
    return await runMutation(adapter,'arm',request,{...options,preparation});
  } catch(error) {return {status:'rejected',provider:adapter.provider,reason:error.message,timings:{...preparation,requestBuildMs:elapsed(started)}};}
}
function issuedContinuations(adapter,entryId) {
  return [...new Set([
    ...(journalFor(adapter)?.records || []).filter(row=>row.operation==='entry-plan-continue' && row.payload?.entryId===entryId).map(row=>row.id),
    ...[...(adapter.chunkEntryCommands?.values() || [])].filter(row=>row.operation==='continue' && row.request?.entryId===entryId).map(row=>row.request.id)
  ])];
}
function explicitStopId(adapter,entryId,continuations=[]) {
  const commands=[...(adapter.chunkEntryCommands?.values() || [])]
    .filter(command=>command.operation==='stop' && command.request?.entryId===entryId);
  const pending=commands.find(command=>command.pending);
  if(pending)return commandId(pending.request.id,130);
  const records=(journalFor(adapter)?.records || [])
    .filter(record=>record.operation==='entry-plan-stop' && record.payload?.entryId===entryId);
  // A lost response still belongs to the same immutable stop attempt, including
  // after restart. Only an explicit click may retry its existing request ID.
  const unresolved=records.filter(record=>record.result?.status==='unknown').pop();
  if(unresolved)return commandId(unresolved.id,130);
  const previous=records[records.length-1] || commands[commands.length-1];
  const captured=previous?.payload?.cancelContinueIds || previous?.request?.cancelContinueIds || [];
  if(previous?.result?.status==='ok' && previous.result.entry?.id===entryId
    && (previous.result.entry.phase==='unknown' || adapter.capabilities?.includes(STOP_PRIORITY_CAPABILITY)
      && continuations.some(id=>!captured.includes(id)))) {
    // The VPS answered, but its saved stop receipt did not settle the parent.
    // A new explicit recovery stop must not keep returning that old receipt.
    // runMutation persists this distinct intent before its stop-only POST.
    return commandId('stop-recovery-'+randomUUID(),130);
  }
  return commandId(previous?.id || previous?.request?.id || 'stop-'+entryId,130);
}
async function stopChunkEntry(adapter,entryId,{cancelContinueIds=[]}={}) {
  try {
    commandId(entryId);
    if(!Array.isArray(cancelContinueIds) || cancelContinueIds.length>2048)throw Error('Неверный список команд добора');
    cancelContinueIds.forEach(id=>commandId(id,130));
    const priority=adapter.capabilities?.includes(STOP_PRIORITY_CAPABILITY);
    if(cancelContinueIds.length && !priority)throw Error('Обновите VPS для приоритетной остановки добора');
    const continuations=[...new Set([...issuedContinuations(adapter,entryId),...cancelContinueIds])].sort();
    if(continuations.length>2048)throw Error('Достигнут предел команд добора набора');
    const id=explicitStopId(adapter,entryId,continuations);
    const previous=journalFor(adapter)?.records.find(row=>row.id===id)?.payload
      || adapter.chunkEntryCommands?.get('stop:'+id)?.request;
    // A transport retry retains the exact revocations of its original stop.
    const request={id,entryId,expiresAt:Date.now()+3000,...delivery(adapter,'stop',previous)};
    if(previous) {
      if(previous.cancelContinueIds)request.cancelContinueIds=previous.cancelContinueIds;
      if(cancelContinueIds.some(id=>!request.cancelContinueIds?.includes(id)))throw Error('Сначала подтверди предыдущую остановку набора');
    } else if(priority)request.cancelContinueIds=continuations;
    return await runMutation(adapter,'stop',request);
  } catch(error) {return {status:'rejected',provider:adapter.provider,reason:error.message};}
}
async function listChunkEntries(adapter) {
  if (adapter.config.routePrefix) throw Error('Список наборов доступен через основной счёт');
  // New receivers publish entry plans in the ordinary shared management view.
  // Exact unknown-arm recovery below still reads its own immutable plan id.
  const fast=adapter.capabilities?.includes('entry-progress-fast-read-v1');
  if(!fast && adapter.capabilities?.includes(admission.CAPABILITY))await adapter.managementState({maxAgeMs:1000});
  const age=Date.now()-Number(adapter.managementStateAt || 0);
  const cached=adapter.managementStateView?.entryPlans;
  const entries=Array.isArray(cached) && age>=0 && age<=1000 ? cached : (await adapter.request('/entry-plans'))?.entries;
  if (!Array.isArray(entries) || entries.some(entry=>!entry || typeof entry!=='object'
      || typeof entry.id!=='string' || !PHASES.has(entry.phase))) throw Error('MT5 VPS: некорректный список наборов');
  const clean=entries.map(entry=>{
    const {_admissionCancelConfirmed,_armUnconfirmed,...record}=entry;
    return record;
  });
  return require('./entryLifecycle').reconcileEntries(adapter,await reconcileUnknownArms(adapter,clean));
}
async function continueChunkEntry(adapter,entryId,account,{requestId='continue-'+randomUUID()}={}) {
  try {
    commandId(entryId);
    commandId(requestId,130);
    if(typeof account!=='string' || !/^[0-9]{1,20}$/.test(account))throw Error('Неверный счёт добора');
    if(adapter.capabilities?.includes(STOP_PRIORITY_CAPABILITY)) {
      const pending=[...(adapter.chunkEntryCommands?.values() || [])].some(row=>row.operation==='stop'
        && row.request?.entryId===entryId && (row.pending || row.result?.status==='unknown'));
      const unknown=(journalFor(adapter)?.records || []).some(row=>row.operation==='entry-plan-stop'
        && row.payload?.entryId===entryId && row.result?.status==='unknown');
      if(pending || unknown)throw Error('Сначала подтверди остановку набора');
    }
    return await runMutation(adapter,'continue',{id:requestId,entryId,account,expiresAt:Date.now()+3000,...delivery(adapter,'continue')});
  } catch(error) {return {status:'rejected',provider:adapter.provider,reason:error.message};}
}
module.exports={buildEntryRequest,armChunkEntry,stopChunkEntry,continueChunkEntry,listChunkEntries};

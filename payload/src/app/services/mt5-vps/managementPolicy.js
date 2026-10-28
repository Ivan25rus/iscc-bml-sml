// v18 reports this condition as a warning string. Everything else remains a
// blocking divergence; the VPS independently rechecks every account and ticket.
const PROTECTION_WARNING='SL или TP на счетах различаются';
const ENTRY_PROTECTION_CAPABILITY='hybrid-position-protection-v1';
const ENTRY_POLICY='hybrid-market-limit-v1';
const {EXACT_CLOSE_CAPABILITY}=require('./entryControlState');
function isEntryPlanRow(row) {
  return Boolean(row && (Object.hasOwn(row,'entryPlan') || row.executionPolicy===ENTRY_POLICY));
}
function entryPlanMetadata(row) {
  const plan=row?.entryPlan;
  if(!plan || typeof plan.id!=='string' || !/^[A-Za-z0-9_.:-]{1,100}$/.test(plan.id)
    || plan.executionPolicy!==ENTRY_POLICY || !['buy','sell'].includes(plan.side) || plan.side!==row.type
    || !Array.isArray(plan.accounts) || !plan.accounts.length || plan.accounts.length>32
    || plan.accounts.some(account=>typeof account!=='string' || !/^[1-9][0-9]{0,19}$/.test(account))
    || new Set(plan.accounts).size!==plan.accounts.length || typeof plan.phase!=='string'
    || !Number.isFinite(plan.sl) || plan.sl<=0 || !Number.isFinite(plan.tp) || plan.tp<0)
    throw Error('Нет подтверждённых данных всего набора БМЛ/СМЛ. Обнови список и приёмник VPS.');
  return plan;
}
function entryProtectionBlock({row,account,linked,capabilities=[],commands=[],accountStates={}}) {
  if(!isEntryPlanRow(row))return '';
  let plan;try{plan=entryPlanMetadata(row);}catch(error){return error.message;}
  const selected=String(account);
  if(!plan.accounts.includes(selected))return 'Выбранный счёт не входит в этот набор БМЛ/СМЛ.';
  if(!capabilities.includes(ENTRY_PROTECTION_CAPABILITY))return 'Обнови приёмник VPS для общего стопа и тейка БМЛ/СМЛ.';
  if(plan.protectionReady!==true || plan.phase==='unknown' || !Number.isFinite(plan.averagePrice) || plan.averagePrice<=0)
    return 'Защита набора БМЛ/СМЛ ещё не подтверждена. Дождись сверки с MT5.';
  if(commands.some(command=>(command.ticket===`entry:${plan.id}` || command.payload?.entryId===plan.id)
    && command.result?.status==='unknown'))return 'Есть неподтверждённая команда для всего набора БМЛ/СМЛ. Повтор заблокирован.';
  for(const value of linked ? plan.accounts : [selected]) {
    const state=accountStates[value];
    if(state?.evidenceReady===false)return 'Проверяем состояние сделки';
    if(!state?.connected)return 'Нет подтверждённой связи с одним из счетов набора БМЛ/СМЛ.';
    if(!state.capabilities?.includes(ENTRY_PROTECTION_CAPABILITY))return 'Обнови приёмник VPS на каждом выбранном счёте БМЛ/СМЛ.';
  }
  return '';
}
function exactEntryCloseBlock({row,account,capabilities=[],commands=[],rules=[],timeRules=[]}) {
  if(!capabilities.includes(EXACT_CLOSE_CAPABILITY))return 'Точный протокол закрытия набора недоступен.';
  let plan;try{plan=entryPlanMetadata(row);}catch(error){return error.message;}
  if(!row.__isPosition || !plan.accounts.includes(String(account)))return 'Выбери позицию из этого набора.';
  if(commands.some(command=>(command.ticket===`entry:${plan.id}` || command.payload?.entryId===plan.id
      || String(command.ticket)===String(row.ticket)) && command.result?.status==='unknown')
    || rules.some(rule=>String(rule.ticket)===String(row.ticket) && ['unknown','firing'].includes(rule.state))
    || timeRules.some(rule=>String(rule.ticket)===String(row.ticket) && ['opening','unknown','firing'].includes(rule.state)))
    return 'Есть неподтверждённая команда для набора. Дождись сверки с MT5; повтор заблокирован.';
  return '';
}
function protectionView(row) {
  if(!isEntryPlanRow(row))return row;
  const plan=entryPlanMetadata(row);
  return {...row,sl:plan.sl,tp:plan.tp,open_price:plan.averagePrice};
}
function managementBlock({row,account,linked,commands=[],rules=[],timeRules=[],alerts=[],accountStates={}}) {
  if(!row)return 'Выбери позицию или заявку';
  const ticket=String(row.ticket),selected=String(account);
  if(accountStates[selected]?.connected===false)return 'Нет связи с выбранным счётом';
  const parentUnknown=row.entryPlan?.id && commands.some(command=>(command.ticket===`entry:${row.entryPlan.id}` || command.payload?.entryId===row.entryPlan.id) && command.result?.status==='unknown');
  const unknown=parentUnknown || commands.some(command=>String(command.ticket)===ticket && command.result?.status==='unknown'
    && (linked || !command.result.accountResults || !command.result.accountResults[selected]
      || command.result.accountResults[selected].status==='unknown'));
  if(unknown || rules.some(rule=>String(rule.ticket)===ticket && ['unknown','firing'].includes(rule.state))
    || timeRules.some(rule=>String(rule.ticket)===ticket && ['opening','unknown','firing'].includes(rule.state)))
    return 'Есть неподтверждённая команда. Дождись сверки с MT5; повтор заблокирован.';
  if(!linked || !row.mirrorAccount)return '';
  if(row.mirrorStatus!=='ok')return 'Открытие подтверждено не на всех счетах. Выбери отдельный счёт.';
  const accounts=new Set([selected,...(row.mirrorAccounts || [row.mirrorAccount]).map(String)]);
  if([...accounts].some(value=>accountStates[value]?.connected===false))return 'Нет связи с одним из связанных счетов';
  for(const alert of alerts) {
    const relevant=alert.account ? accounts.has(String(alert.account)) : String(alert.tickets?.[selected])===ticket;
    if(relevant && alert.reason!==PROTECTION_WARNING)return alert.reason || 'Связанные позиции различаются. Обнови список или выбери отдельный счёт.';
  }
  return '';
}
function decimal(value) {
  const text=String(value ?? '').trim();
  if(!/^(?:\d+(?:[.,]\d*)?|[.,]\d+)$/.test(text))throw Error('Введи число, например 0,2');
  const number=Number(text.replace(',','.'));
  if(!Number.isFinite(number))throw Error('Некорректное число');
  return number;
}
function protectionChanges(row,values,dirty) {
  const hybrid=isEntryPlanRow(row);
  row=protectionView(row);
  const changes={};
  for(const key of ['sl','tp','price']) {
    if(!dirty.has(key) || key==='price' && (row.__isPosition || hybrid))continue;
    if(String(values[key]).trim()==='')throw Error(`Для снятия ${key.toUpperCase()} введи 0`);
    const value=decimal(values[key]);
    if(key==='price' && value<=0)throw Error('Цена заявки должна быть больше нуля');
    if(value!==Number(key==='price'?row.open_price:row[key]))changes[key]=String(value);
  }
  return changes;
}
module.exports={managementBlock,decimal,protectionChanges,PROTECTION_WARNING,
  ENTRY_PROTECTION_CAPABILITY,isEntryPlanRow,entryPlanMetadata,entryProtectionBlock,exactEntryCloseBlock,protectionView};

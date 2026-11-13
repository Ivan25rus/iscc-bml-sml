const {closeVolumes}=require('./sizing');
const {timingLines}=require('./timings');
const {EXACT_PROTECTION_CAPABILITY,PARENT_PARTIAL_CAPABILITY}=require('./entryControlState');
const {parentClosePreview}=require('./parentClosePreview');
const {protectionReceiptView}=require('./protectionReceiptView');
const {normalizeTarget,matchesTarget}=require('./managementTarget');
const {managementBlock,decimal,protectionChanges,isEntryPlanRow,entryPlanMetadata,entryProtectionBlock,exactEntryProtectionBlock,exactEntryCloseBlock,protectionView}=require('./managementPolicy');
function peerProtectionTarget(data,wanted) {
  const selected=data.accountStates?.[wanted.account],server=data.server || selected?.server;
  if(!data.accountStates || String(data.account)!==wanted.account || selected?.account!=null && String(selected.account)!==wanted.account
    || typeof server!=='string' || !server)return null;
  const accounts=Array.isArray(data.accounts) ? data.accounts.map(String) : Object.keys(data.accountStates);
  for(const account of accounts) {
    if(account===wanted.account)continue;
    const peer=data.accountStates[account],target=normalizeTarget({...wanted,account});
    if(!target || peer?.connected!==true || peer.evidenceReady===false || peer.server!==server
      || String(peer.account)!==account || !Array.isArray(peer.orders))continue;
    if(peer.orders.some(row=>matchesTarget(row,target,account) && Array.isArray(row.entryPlan.accounts) && row.entryPlan.accounts.includes(account)
      && (row.account==null || String(row.account)===account) && /^[1-9][0-9]{0,19}$/.test(String(row.ticket))
      && Number.isFinite(row.lots) && row.lots>0))return target;
  }
  return null;
}
function accountControlDetails(result) {
  const labels={ok:'подтверждено',rejected:'отклонено',unknown:'ожидает подтверждения'};
  return Object.entries(result.accountResults || {}).map(([account,outcome])=>{
    const label=outcome.noPosition===true ? 'нет позиции' : labels[outcome.status] || outcome.status;
    return `${account}: ${label}${outcome.status!=='ok' && outcome.reason ? ` (${outcome.reason})` : ''}`;
  }).join(' · ');
}
function mountManagement(ipc, toolbar, {getInstrumentInfo=()=>null,partialClose=true,priceClose=true}={}) {
  const launch = document.createElement('button');
  launch.id = 'mt5-manage-button';
  launch.textContent = 'Позиции · MT5';
  toolbar.append(launch);
  let dialog,pendingTarget=null,retarget;
  launch.openTarget=value=>{
    const requested=normalizeTarget(value);if(!requested)return false;
    pendingTarget=requested;
    toolbar.closest('details')?.removeAttribute('open');
    openPanel();return true;
  };
  const openPanel = () => {
    if (dialog) {
      dialog.showModal();
      if(pendingTarget){const wanted=pendingTarget;pendingTarget=null;retarget(wanted);}
      else dialog.querySelector('#mt5-refresh').click();
      return;
    }
    let navigationTarget=pendingTarget,allowPeerTarget=true;pendingTarget=null;
    dialog = document.createElement('dialog');
    dialog.id = 'mt5-management';
    dialog.style.cssText = 'box-sizing:border-box;width:min(520px,94vw);max-width:94vw;min-width:0;max-height:88vh;margin:max(12px,6vh) auto 0;overflow:auto;scrollbar-gutter:stable;overflow-wrap:anywhere;color:var(--text);background:var(--panel,#20242b);border:1px solid var(--border);border-radius:12px;padding:14px;font-size:13px';
    const make = (tag, text) => { const element = document.createElement(tag); if (text) element.textContent = text; dialog.append(element); return element; };
    const title=make('h2','Позиции · MT5');
    const status = make('p','Проверяю подключение…');
    const alertsView = make('p'); alertsView.id='mt5-account-alerts'; alertsView.setAttribute('role','alert');
    const accounts = make('select'); accounts.id = 'mt5-account'; accounts.style.cssText = 'box-sizing:border-box;display:block;width:100%;min-width:0;padding:8px;margin-bottom:12px';
    const refresh = make('button','↻');refresh.title='Обновить позиции';refresh.setAttribute('aria-label','Обновить позиции');
    refresh.id = 'mt5-refresh';
    const dismiss = make('button','×');dismiss.setAttribute('aria-label','Закрыть окно позиций');
    dismiss.id='mt5-management-dismiss';dismiss.title='Закрыть меню позиций';
    dismiss.style.marginLeft = '10px'; dismiss.onclick = () => {navigationTarget=null;chart.cancel();dialog.close();};
    const label = make('p','Выбери конкретную позицию или отложенную заявку:');
    const select = make('select'); select.id = 'mt5-target'; select.style.cssText = 'box-sizing:border-box;display:block;width:100%;min-width:0;padding:8px';
    const detail = make('p');
    const selectionSummary=make('p');selectionSummary.id='mt5-selection-summary';
    const blockNote=make('p');blockNote.id='mt5-management-block';blockNote.setAttribute('role','status');
    const mirrorLabel = make('label','Управлять на всех связанных счетах ');
    const mirror = document.createElement('input'); mirror.type='checkbox'; mirror.id='mt5-mirror-management'; mirror.checked=true; mirrorLabel.append(mirror);
    const fields = make('div'); fields.style.cssText = 'display:flex;gap:12px;flex-wrap:wrap;margin:14px 0';
    const input = (text, id) => {
      const label = document.createElement('label'); label.textContent = text;
      const field = document.createElement('input'); field.type = 'text';field.inputMode='decimal'; field.step = 'any'; field.min = '0'; field.id = id;
      field.style.cssText = 'display:block;width:140px;padding:6px'; label.append(field); fields.append(label); return field;
    };
    const sl = input('Стоп · цена','mt5-edit-sl'), tp = input('Тейк · цена','mt5-edit-tp');
    const takePrice=input('Цена','mt5-partial-take-price');
    const takePercent=input('Закрыть, %','mt5-partial-take-percent');takePercent.value='50';
    const takeSave=make('button','Сохранить частичный тейк');takeSave.id='mt5-partial-take-save';
    const takeCancel=make('button','Отменить частичный тейк');takeCancel.id='mt5-partial-take-cancel';
    const takeNote=make('p');takeNote.id='mt5-partial-take-status';takeNote.className='mt5-note';
    const takePreview=make('p');takePreview.id='mt5-partial-take-preview';takePreview.className='mt5-note';
    const takeField=document.createElement('div'),takeLabel=tp.closest('label');takeLabel.replaceWith(takeField);takeField.append(takeLabel);
    const noTakeLabel=document.createElement('label');noTakeLabel.className='mt5-no-take';
    const noTake=document.createElement('input');noTake.type='checkbox';noTake.id='mt5-no-tp';
    noTakeLabel.append(noTake,document.createTextNode('Без тейка'));takeField.append(noTakeLabel);
    noTakeLabel.title='Убрать тейк на выбранных счетах. Команда отправится после «Сохранить». Сними галочку, чтобы вернуть цену или задать новую.';
    const price = input('Цена заявки','mt5-edit-price');
    const modify = make('button','Применить SL/TP'); modify.id = 'mt5-modify';
    const toEntry=make('button','Стоп во вход');toEntry.id='mt5-stop-to-entry';toEntry.title='Подставить цену входа в поле стопа; затем нажми «Сохранить»';
    const protectionNote=make('p','SL / TP — цены; 0 снимает уровень.');
    const volume = input('Закрыть · лоты','mt5-close-volume');
    const sizeMode=make('select'); sizeMode.id='mt5-close-mode';
    for (const [value,text] of [['lots','Лоты'],['percent','% позиции']]) {const option=document.createElement('option'); option.value=value; option.textContent=text; sizeMode.append(option);}
    sizeMode.setAttribute('aria-label','Способ расчёта закрываемой части');
    sizeMode.value='percent';
    const percent=input('Закрыть · %','mt5-close-percent'); percent.value='50'; percent.max='100'; percent.step='1'; percent.min='0.01';
    const closePreview=make('p'); closePreview.id='mt5-close-preview';
    const closeSummary=make('p');closeSummary.id='mt5-close-summary';
    const partial = make('button','Закрыть часть'); partial.id = 'mt5-partial-close';
    const full = make('button','Закрыть всю позицию'); full.id = 'mt5-full-close'; full.style.marginLeft = '10px';
    const cancel = make('button','Отменить заявку'); cancel.id = 'mt5-cancel'; cancel.style.marginLeft = '10px';
    const trigger = input('Условие · цена','mt5-trigger-price');
    const comparison = make('select'); comparison.id = 'mt5-trigger-comparison';
    for (const [value,text] of [['gte','Цена не ниже'],['lte','Цена не выше']]) {
      const option = document.createElement('option'); option.value=value; option.textContent=text; comparison.append(option);
    }
    const schedule = make('button','Закрыть часть при цене'); schedule.id='mt5-schedule-close'; schedule.style.marginLeft='10px';
    const triggerNote=make('p','На VPS, даже при закрытом Mac. BUY — Bid, SELL — Ask; при достижении цены закрывается выбранная доля по рынку.');
    const conditions = make('div'); conditions.id='mt5-close-rules';
    const message = make('p'); message.id = 'mt5-command-result'; message.setAttribute('role','status');
    const resultDetail=make('p');resultDetail.id='mt5-command-result-detail';
    const showMessage=(text,caption=text)=>{message.textContent=caption;resultDetail.textContent=text;};
    const speed = make('div'); speed.id='mt5-command-timing'; speed.style.cssText='font-size:12px;color:var(--muted);white-space:pre-line';
    speed.title='Ответ MT5 — подтверждение команды; для лимитки это постановка заявки. VPS — время от приёма команды до результата счёта. От нажатия до ответа включает связь с Mac.';
    status.after(speed);
    const history = make('div'); history.id = 'mt5-command-history';
    const style=make('style');style.textContent=`
      #mt5-management h2{font-size:18px;margin:0}
      #mt5-management p{margin:6px 0;line-height:1.35}
      #mt5-management .mt5-row{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin:6px 0}
      #mt5-management .mt5-section{border-top:1px solid var(--border);padding-top:6px;margin-top:8px}
      #mt5-management .mt5-section>strong{display:block;margin-bottom:6px}
      #mt5-management .mt5-fields{display:flex;align-items:end;gap:8px;flex-wrap:wrap;margin:6px 0}
      #mt5-management input[inputmode=decimal]{box-sizing:border-box;width:120px!important;max-width:100%;padding:8px}
      #mt5-management button,#mt5-management select{font-size:13px;padding:6px 8px;margin:0!important}
      #mt5-management input,#mt5-management select,#mt5-management button{color:var(--text);background:var(--bg,#111);border:1px solid var(--border);border-radius:7px}
      #mt5-management button{cursor:pointer}#mt5-management button:disabled{opacity:.45;cursor:default}
      #mt5-management #mt5-management-dismiss{color:#ef4444;border:0;background:transparent;font-size:25px;line-height:24px;width:30px;height:30px;padding:0!important}
      #mt5-management #mt5-management-dismiss:hover{background:rgba(239,68,68,.1)}
      #mt5-management #mt5-full-close{border-color:#ac5c48}
      #mt5-management #mt5-account-alerts{font-size:12px;color:#e0b96b}
      #mt5-management #mt5-management-block{font-size:12px;color:#e0b96b}
      #mt5-management .mt5-notices{height:32px;overflow:auto;margin:6px 0}
      #mt5-management .mt5-notices p{margin:0}
      #mt5-management .mt5-protection-fields{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));align-items:start}
      #mt5-management .mt5-protection-fields input{width:100%!important}
      #mt5-management .mt5-no-take{display:flex;align-items:center;gap:5px;margin-top:5px;font-size:11px;color:var(--muted)}
      #mt5-management .mt5-no-take input{width:auto!important;margin:0}
      #mt5-management .mt5-action{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-top:8px}
      #mt5-management #mt5-partial-close{flex:1;min-width:120px}
      #mt5-management .mt5-percent-presets{display:flex;gap:6px}
      #mt5-management .mt5-percent-presets button[aria-pressed=true]{border-color:#76a996}
      #mt5-management .mt5-note{font-size:11px;color:var(--muted)}
      #mt5-management details{margin-top:8px}
      #mt5-management summary{cursor:pointer;color:var(--muted)}
      #mt5-management #mt5-close-preview{font-size:12px;font-variant-numeric:tabular-nums}
      #mt5-management #mt5-selection-summary,#mt5-management #mt5-close-summary{font-size:12px;color:var(--muted);font-variant-numeric:tabular-nums}
      #mt5-management #mt5-target{font-size:13px}
      #mt5-management #mt5-selection-summary{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
      #mt5-management #mt5-full-close{width:100%}
      #mt5-management #mt5-command-result:empty{display:none}
      #mt5-management [hidden]{display:none!important}
    `;
    const header=make('div');header.className='mt5-row';header.prepend(title);header.append(refresh,dismiss);title.style.marginRight='auto';dialog.prepend(header);
    const accountRow=make('div');accountRow.className='mt5-row';accountRow.append(accounts,mirrorLabel);
    const notices=make('div');notices.className='mt5-notices';notices.append(alertsView,blockNote);
    header.after(accountRow,select,selectionSummary,notices);
    status.className=detail.className='mt5-note';status.style.fontVariantNumeric='tabular-nums';label.textContent='Позиция или заявка';label.className='mt5-note';
    label.hidden=true;select.setAttribute('aria-label','Позиция или заявка');accounts.setAttribute('aria-label','Счёт');
    accounts.style.cssText='max-width:100%;min-width:0';select.style.padding='6px';
    const section=(text)=>{const block=make('div');block.className='mt5-section';const heading=document.createElement('strong');heading.textContent=text;block.append(heading);return block;};
    const protection=section('Стоп и тейк');fields.className='mt5-fields mt5-protection-fields';fields.removeAttribute('style');
    const chart=require('./protectionChart').createProtectionChart({document,onHistoryRetry:()=>{
      const row=target();if(row?.__isPosition){minuteHistory.retry(row.symbol);buttons();}
    },onEdit:(key,value)=>{
      if(modify.disabled || !target()?.__isPosition)return;
      const field=key==='partialTp'?takePrice:key==='sl'?sl:tp;field.value=value;field.dispatchEvent(new Event('input',{bubbles:true}));
    }});
    protection.append(chart.element);
    const protectionActions=document.createElement('div');protectionActions.className='mt5-action';protectionActions.append(modify,toEntry);protection.append(fields,protectionActions);protectionNote.className='mt5-note';
    const takeSection=make('details');takeSection.id='mt5-partial-take-section';
    const takeTitle=document.createElement('summary');takeTitle.textContent='Частичный тейк · все счета';
    const takeFields=document.createElement('div');takeFields.className='mt5-fields mt5-protection-fields';takeFields.append(takePrice.closest('label'),takePercent.closest('label'));
    const takeActions=document.createElement('div');takeActions.className='mt5-action';takeActions.append(takeSave,takeCancel);
    takeSection.append(takeTitle,takeFields,takePreview,takeActions,takeNote);protection.append(takeSection);
    const closure=section('Закрыть часть');const closeRow=document.createElement('div');closeRow.className='mt5-row';closeRow.append(sizeMode,volume.closest('label'),percent.closest('label'),partial);
    const presets=document.createElement('div');presets.className='mt5-percent-presets';
    for(const value of [25,50,75]){const button=document.createElement('button');button.textContent=value+'%';button.dataset.percent=String(value);button.onclick=()=>{if(busy)return;sizeMode.value='percent';percent.value=String(value);buttons();};presets.append(button);}
    const partialRow=document.createElement('div');partialRow.className='mt5-action';partialRow.append(presets,partial);
    const fullRow=document.createElement('div');fullRow.className='mt5-action';fullRow.append(full,cancel);closure.append(partialRow,closeSummary,fullRow);
    const details=make('details');details.id='mt5-management-details';const detailsTitle=document.createElement('summary');detailsTitle.textContent='Подробности';details.append(detailsTitle);
    const customSize=document.createElement('strong');customSize.textContent='Другой объём';details.append(customSize,closeRow,closePreview,status,detail,protectionNote,resultDetail);
    const conditional=make('details');conditional.id='mt5-price-close-section';conditional.className='mt5-section';const conditionalTitle=document.createElement('summary');conditionalTitle.textContent='Закрыть часть при достижении цены';conditional.append(conditionalTitle);
    const triggerRow=document.createElement('div');triggerRow.className='mt5-row';triggerRow.append(comparison,trigger.closest('label'),schedule);conditional.append(triggerRow,triggerNote);triggerNote.className='mt5-note';
    const conditionsPanel=make('details');conditionsPanel.id='mt5-conditions-panel';const conditionsTitle=document.createElement('summary');conditionsTitle.textContent='Условия сделки';conditionsPanel.append(conditionsTitle,conditions);
    dialog.append(message);
    const timingsPanel=make('details');const timingsTitle=document.createElement('summary');timingsTitle.textContent='Замеры последней команды';timingsPanel.append(timingsTitle,speed);
    const historyPanel=make('details');historyPanel.id='mt5-history-panel';const historyTitle=document.createElement('summary');historyTitle.textContent='Последние команды';historyPanel.append(historyTitle,history);
    details.append(conditional,conditionsPanel,timingsPanel,historyPanel);dialog.append(details);
    let rows = [], capabilities = [], commands = [], rules = [], parentTakes=[], timeRules=[], alerts = [], accountStates={}, connected=false, busy = false, loading = false, generation = 0;
    let takeDirty=false,takeDraftParent='';
    const parentTake=()=>parentTakes.find(rule=>rule.entryId===target()?.entryPlan?.id);
    const dirty=new Set();
    let readPromise=null,breakeven=false,observedAt=0,commandGeneration=0,previousTake='',commandBusy=false,cachedDisplay=false;
    let nativeObservedAt,lastProtectionReceipt=null;
    const minuteHistory=require('./minuteHistory').createMinuteHistory({load:symbol=>ipc.invoke('mt5-vps:minute-bars',symbol),
      changed:()=>{if(dialog.open)buttons();}});
    function chartHistory(row) {
      if(!dialog.open || document.hidden || !row?.__isPosition)return {data:null,loading:false,error:''};
      return minuteHistory.get(row.symbol,{enabled:capabilities.includes('minute-chart-bars-v1')});
    }
    const quoteHandler=(_event,snapshot)=>{
      const row=target();
      if(!dialog.open || document.hidden || snapshot?.provider!=='mt5-vps' || snapshot.symbol!==row?.symbol)return;
      const at=Number(snapshot.quote?.utcTime)*1000;
      if(Number.isFinite(at) && Math.abs(Date.now()-at)<=1500)chart.quote({bid:snapshot.quote?.bid,at});
    };
    ipc.on?.('instrument:updated',quoteHandler);
    let closePreviewKey=null,historyKey=null,conditionsKey=null;
    const target = () => {
      const row=rows.find(row=>String(row.ticket)===select.value);
      return navigationTarget && !matchesTarget(row,navigationTarget,accounts.value) ? undefined : row;
    };
    const chartSpecifications=new Map(),chartReads=new Set(),chartAttempts=new Set();
    function chartTickSize(symbol){
      const live=Number(getInstrumentInfo(symbol)?.tickSize);
      const cached=Number.isFinite(live) && live>0?live:chartSpecifications.get(symbol);
      if(Number.isFinite(Number(cached)) && Number(cached)>0)return Number(cached);
      // A manually opened broker position may have no order card/cache yet.
      // Fetch its specification once on demand, never on a new polling loop;
      // ignore the quote and leave all account prices in their existing view.
      if(symbol && dialog.open && !busy && !chartAttempts.has(symbol)){
        chartAttempts.add(symbol);chartReads.add(symbol);
        Promise.resolve(ipc.invoke('instrument:get',{symbol,provider:'mt5-vps'})).then(info=>{
          const tick=Number(info?.metadata?.tickSize ?? info?.tickSize);
          if(Number.isFinite(tick) && tick>0)chartSpecifications.set(symbol,tick);
        }).catch(()=>{}).finally(()=>{chartReads.delete(symbol);if(dialog.open)buttons();});
      }
      return null;
    }
    function buttons() {
      const row = target(),hybrid=isEntryPlanRow(row);
      volume.closest('label').hidden=sizeMode.value==='percent';percent.closest('label').hidden=sizeMode.value!=='percent';
      price.closest('label').hidden=hybrid || Boolean(row?.__isPosition);partial.hidden=full.hidden=!row?.__isPosition;cancel.hidden=Boolean(row?.__isPosition);
      protection.hidden=!row && !navigationTarget;closure.hidden=!row;conditional.hidden=!row?.__isPosition;closeRow.hidden=!row?.__isPosition;closePreview.hidden=!row?.__isPosition;
      closure.querySelector('strong').textContent=row?.__isPosition ? (hybrid?'Закрыть часть набора · все счета':'Закрыть часть'):'Отложенная заявка';
      const blocked = connected ? managementBlock({row,account:dataAccount(),linked:!hybrid && mirror.checked,commands,rules,timeRules,alerts,accountStates}) : 'Нет подтверждённой связи с MT5. Обнови список.';
      const protectionBlocked=hybrid && capabilities.includes(EXACT_PROTECTION_CAPABILITY)
        ? exactEntryProtectionBlock({row,account:dataAccount(),linked:mirror.checked,capabilities,commands,rules,timeRules,accountStates})
        : blocked || entryProtectionBlock({row,account:dataAccount(),linked:mirror.checked,capabilities,commands,accountStates});
      const displayNote=protectionBlocked || (cachedDisplay ? 'Сохранённые данные · сверяем с MT5…' : '');
      blockNote.textContent=displayNote;blockNote.hidden=!displayNote || !row;
      mirror.disabled=busy || (hybrid ? !(row.entryPlan?.accounts?.length>1) : !row?.mirrorAccount);
      modify.disabled = busy || protectionBlocked || !row || !hybrid && !capabilities.includes('modify');
      noTake.disabled=Boolean(modify.disabled);
      try{const take=decimal(tp.value);noTake.checked=take===0;if(take>0)previousTake=tp.value;}catch{noTake.checked=false;}
      toEntry.disabled=busy || !row?.__isPosition || hybrid && Boolean(protectionBlocked);toEntry.hidden=!row?.__isPosition;
      cancel.disabled = busy || blocked || !row || row.__isPosition || !capabilities.includes('cancel');
      const exactClose=hybrid && capabilities.includes('entry-exact-parent-close-v1');
      const fullBlock=exactClose ? exactEntryCloseBlock({row,account:dataAccount(),capabilities,commands,rules,timeRules})
        : blocked || hybrid && entryProtectionBlock({row,account:dataAccount(),linked:true,capabilities,commands,accountStates});
      full.disabled = busy || fullBlock || !row?.__isPosition || !capabilities.includes('close');
      full.textContent=hybrid ? 'Закрыть всё · все счета' : 'Закрыть всю позицию';
      let preview=[];
      try {const value=decimal(sizeMode.value==='percent' ? percent.value : volume.value);
        preview=hybrid ? parentClosePreview(row,dataAccount(),accountStates,sizeMode.value,value).preview
          : closeVolumes(row,dataAccount(),mirror.checked,sizeMode.value,value);}
      catch(error) {preview=[{account:dataAccount(),error:error.message}];}
      const previewKey=JSON.stringify(preview);
      if(previewKey!==closePreviewKey){
        closePreviewKey=previewKey;closePreview.replaceChildren();
        for (const item of preview) {
          const line=document.createElement('div');
          line.textContent=`Счёт ${item.account}: ${item.error || `закрыть ${item.volume} лота · останется ${item.remaining} лота · шаг ${item.step}`}`;
          closePreview.append(line);
        }
        closeSummary.textContent=preview.map(item=>`${preview.length>1?item.account+': ':''}${item.error || `${item.volume} л. → остаток ${item.remaining} л.`}`).join(' · ');
      }
      closeSummary.hidden=!row?.__isPosition;
      partial.disabled = full.disabled || blocked || hybrid && !capabilities.includes(PARENT_PARTIAL_CAPABILITY)
        || !capabilities.includes('partial-close') || !preview.length || preview.some(item=>item.error) || preview.every(item=>!item.volume);
      partial.textContent=sizeMode.value==='percent' ? `Закрыть ${percent.value || '…'}%` : 'Закрыть часть';
      partial.title=hybrid && !capabilities.includes(PARENT_PARTIAL_CAPABILITY)?'Нужно обновить приёмник VPS для закрытия части всего набора.':'';
      let triggerPrice=0;try{triggerPrice=decimal(trigger.value);}catch{}
      let scheduledPreview=[];
      try{scheduledPreview=closeVolumes(row,dataAccount(),!hybrid && mirror.checked,sizeMode.value,decimal(sizeMode.value==='percent'?percent.value:volume.value));}catch{}
      schedule.disabled = full.disabled || blocked || !scheduledPreview.length || scheduledPreview.some(item=>item.error) || !capabilities.includes('schedule-close') || !(triggerPrice>0)
        || rules.some(rule=>String(rule.ticket)===String(row?.ticket) && rule.state==='armed');
      conditionalTitle.textContent=hybrid?'Закрыть часть выбранного билета при достижении цены':'Закрыть часть при достижении цены';
      for (const field of [sl,tp,price,volume,percent,sizeMode,select,trigger,comparison]) field.disabled = busy || !row;
      percent.disabled ||= !row?.__isPosition || sizeMode.value!=='percent';
      sizeMode.disabled ||= !row?.__isPosition;
      price.disabled ||= hybrid || row?.__isPosition === true;
      volume.disabled ||= row?.__isPosition !== true || sizeMode.value==='percent';
      trigger.disabled ||= row?.__isPosition !== true;
      comparison.disabled ||= row?.__isPosition !== true;
      accounts.disabled = busy;
      for(const button of presets.querySelectorAll('button')){button.disabled=busy || !row?.__isPosition;button.setAttribute('aria-pressed',String(sizeMode.value==='percent' && Number(percent.value)===Number(button.dataset.percent)));}
      presets.hidden=!row?.__isPosition;
      takeSection.hidden=!hybrid || !row?.__isPosition;
      const takeRule=parentTake(),takeStates=Object.values(takeRule?.accounts || {});
      const takeActive=takeStates.some(item=>['armed','firing','unknown'].includes(item.state));
      const takeSupported=capabilities.includes('entry-parent-partial-take-v1');
      let takePlan=[],takeValue=0;
      try{takeValue=decimal(takePrice.value);takePlan=parentClosePreview(row,dataAccount(),accountStates,'percent',decimal(takePercent.value)).preview;}catch{}
      takeSave.disabled=busy || !hybrid || !takeSupported || !connected || Boolean(fullBlock) || !(takeValue>0)
        || !takePlan.length || takePlan.some(item=>item.error) || takeActive;
      takeCancel.disabled=busy || !takeSupported || !takeStates.some(item=>item.state==='armed') || takeStates.some(item=>['firing','unknown'].includes(item.state));
      takeCancel.hidden=!takeActive;
      takeSave.textContent=`Сохранить TP ${takePercent.value || '…'}%`;
      takePrice.disabled=takePercent.disabled=busy || !hybrid || takeActive;
      const takeExplanation=!takeSupported?'Нужно обновить приёмник VPS.':takeRule?
        Object.entries(takeRule.accounts).map(([account,value])=>`${account}: ${{armed:'ожидает цену',firing:'закрывается',completed:'исполнен',cancelled:'отменён',unknown:'результат неизвестен',rejected:'отклонён'}[value.state] || value.state}${value.reason?' · '+value.reason:''}`).join(' · '):
        'Процент остатка каждого счёта при достижении цены. Нужна связь VPS с MT5; Mac можно закрыть. SL и TP сохраняются.';
      if(takeNote.textContent!==takeExplanation)takeNote.textContent=takeExplanation;
      const takePreviewText=takePlan.map(item=>`${item.account}: ${item.error || `закрыть ${item.volume} л. · остаток ${item.remaining} л.`}`).join(' · ');
      if(takePreview.textContent!==takePreviewText)takePreview.textContent=takePreviewText;
      for (const button of conditions.querySelectorAll('button')) button.disabled=busy;
      if(!partialClose){
        for(const element of [partial,partialRow,closeRow,closePreview,closeSummary,customSize])element.hidden=true;
      }
      if(!priceClose){conditional.hidden=true;takeSection.hidden=true;}
      refresh.disabled = busy;
      let chartRow=row;try{chartRow=protectionView(row);}catch{chartRow={};}
      const minute=chartHistory(row);
      chart.update({identity:row?`${dataAccount()}:${row.ticket}:${row.type}:${row.entryPlan?.id || ''}:${row.lots}`:'',
        position:row?.__isPosition===true || !!navigationTarget,side:row?.type,entry:chartRow?.open_price,current:row?.currentPrice,observedAt,
        sl:sl.value,tp:tp.value,partialTp:takePrice.value,partialPercent:takePercent.value,
        partialEnabled:hybrid && takeSupported && !takeActive,tickSize:row?.__isPosition?chartTickSize(row.symbol):null,
        minuteHistory:minute.data,historyState:minute,
        enabled:!modify.disabled,blocked:protectionBlocked});
    }
    function choose(preserve = false, keepEditedPrices = false) {
      const row = target(),hybrid=isEntryPlanRow(row);
      if(takeDraftParent!==(row?.entryPlan?.id || '')){takeDraftParent=row?.entryPlan?.id || '';takeDirty=false;}
      if(!takeDirty){const rule=parentTake();takePrice.value=rule?String(rule.targetPrice):'';takePercent.value=String(rule?.closePercent || 50);}
      title.textContent=navigationTarget ? `Стоп и тейк · ${navigationTarget.symbol}` : 'Позиции · MT5';
      select.hidden=!!navigationTarget;
      let protectionRow=row;try{protectionRow=protectionView(row);}catch{protectionRow={};}
      const confirmedEmpty=connected && !rows.length && !navigationTarget;
      detail.textContent = row ? (hybrid ? `Набор БМЛ/СМЛ · средняя ${protectionRow.open_price ?? '—'} · часть #${row.ticket}: ${row.lots} лота` : `Вход ${row.open_price} · ${row.lots} лота${row.__isPosition ? ` · P/L ${row.pnl ?? '—'} USD` : ''}`) : '';
      selectionSummary.textContent=row ? `${hybrid?'Средняя':'Вход'} ${protectionRow.open_price ?? '—'}`
        : confirmedEmpty ? 'Позиций и заявок нет' : 'Проверяем позиции и заявки…';
      protection.querySelector('strong').textContent=hybrid?'SL/TP · весь набор':'Стоп и тейк';
      if(!preserve){dirty.clear();breakeven=false;previousTake='';}
      for(const [key,field,value] of [['sl',sl,hybrid && breakeven ? protectionRow?.open_price : protectionRow?.sl],['tp',tp,protectionRow?.tp],['price',price,row?.open_price]]){
        const next=String(value ?? '');
        if(!keepEditedPrices && !dirty.has(key) && (!preserve || document.activeElement!==field) && field.value!==next)field.value=next;
      }
      const step = Number(row?.quantityStep), lots = Number(row?.lots);
      volume.step = step > 0 ? step : 'any'; volume.min = row?.minQty || '0';
      if (!preserve) {
        volume.value = step > 0 ? Number((Math.floor((lots/2+1e-9)/step)*step).toFixed(8)) : '';
        trigger.value = row?.currentPrice ?? ''; comparison.value = row?.type==='sell' ? 'lte' : 'gte';
        mirror.checked=hybrid ? row.entryPlan?.accounts?.length>1 : Boolean(row?.mirrorAccount);
        percent.value='50';
      }
      mirrorLabel.hidden=navigationTarget && !row ? false : hybrid ? !(row.entryPlan?.accounts?.length>1) : !row?.mirrorAccount;
      mirrorLabel.firstChild.textContent=hybrid ? 'SL/TP на всех счетах ' : (row?.mirrorAccounts?.length>1 ? 'Все счета ' : 'Оба счёта ');
      modify.textContent = breakeven && hybrid ? 'Сохранить безубыток' : 'Сохранить';
      modify.title=hybrid?'Сохранить SL/TP всего набора на выбранных счетах':row?.__isPosition?'Сохранить SL/TP':'Сохранить заявку';
      toEntry.title=hybrid ? 'Подготовить стоп по средней цене всего набора; при сохранении VPS заново проверит среднюю каждого счёта' : 'Подставить цену входа в поле стопа; затем нажми «Сохранить»';
      protectionNote.textContent=hybrid ? 'SL/TP — для всего набора БМЛ/СМЛ на выбранных счетах. Стоп во вход — по общей средней каждого счёта. 0 снимает тейк. Частичное закрытие — только выбранная часть.' : 'SL / TP — цены; 0 снимает уровень.';
      renderConditions();
      buttons();
    }
    const dataAccount = () => accounts.value;
    select.onchange = () => {navigationTarget=null;choose();}; volume.oninput = buttons; trigger.oninput=buttons; mirror.onchange=buttons;
    percent.oninput=buttons; sizeMode.onchange=buttons;
    for(const [key,field] of [['sl',sl],['tp',tp],['price',price]])field.oninput=()=>{if(key==='sl')breakeven=false;dirty.add(key);if(isEntryPlanRow(target()))modify.textContent=breakeven?'Сохранить безубыток':'Сохранить';buttons();};
    noTake.onchange=()=>{
      if(noTake.disabled)return;
      tp.value=noTake.checked?'0':previousTake;
      tp.dispatchEvent(new Event('input',{bubbles:true}));
      if(!noTake.checked)tp.focus();
    };
    toEntry.onclick=()=>{
      const row=target();if(busy || !row?.__isPosition)return;
      if(isEntryPlanRow(row)) {
        const blocked=(capabilities.includes(EXACT_PROTECTION_CAPABILITY) ? exactEntryProtectionBlock : entryProtectionBlock)(
          {row,account:dataAccount(),linked:mirror.checked,capabilities,commands,rules,timeRules,accountStates});
        if(blocked){showMessage(blocked,'Проверка');return;}
        sl.value=String(entryPlanMetadata(row).averagePrice);breakeven=true;dirty.delete('sl');
        modify.textContent='Сохранить безубыток';
        showMessage('Показана средняя всего набора. При сохранении VPS заново рассчитает её на каждом выбранном счёте.','Нажми «Сохранить безубыток»');
      } else {sl.value=String(row.open_price);dirty.add('sl');showMessage('Стоп подставлен по цене входа. Нажми «Сохранить», чтобы отправить.','Нажми «Сохранить»');}
      buttons();
    };
    accounts.onchange = () => {
      const wanted=navigationTarget && {...navigationTarget,account:accounts.value};
      rows=[];connected=false;cachedDisplay=false;select.replaceChildren();choose();
      if(wanted){retarget(wanted,false);return;}
      const requestedGeneration=++generation;
      status.textContent='Проверяем позиции выбранного счёта…';
      reload(false,true).catch(error=>{if(generation===requestedGeneration)status.textContent=error.message;});
    };
    function reload(preserveResult=false,supersede=false) {
      if(readPromise && !supersede)return readPromise;
      const pending=readState(preserveResult).finally(()=>{if(readPromise===pending)readPromise=null;});
      readPromise=pending;
      return readPromise;
    }
    function updateOptions(field,options) {
      const wanted=new Set(options.map(option=>String(option.value)));
      for(const option of [...field.options])if(!wanted.has(option.value))option.remove();
      for(const [index,item] of options.entries()){
        let option=[...field.options].find(option=>option.value===String(item.value));
        if(!option){option=document.createElement('option');option.value=item.value;}
        if(option.textContent!==item.text)option.textContent=item.text;
        if(field.options[index]!==option)field.insertBefore(option,field.options[index] || null);
      }
    }
    function updateAccountDisplay(data) {
      const selected=String(data.account);
      accountStates=data.accountStates || {};
      const listed=Array.isArray(data.accounts) ? data.accounts : [...accounts.options].map(option=>option.value);
      const known=new Set([...listed,...Object.keys(accountStates),selected].map(String).filter(account=>/^[1-9][0-9]{0,19}$/.test(account)));
      const options=[...known].map(account=>{
        const health=accountStates[account];
        const ready=health ? health.connected===true && health.evidenceReady!==false : data.status==='ok' && account===selected;
        const suffix=health?.connectionState==='unavailable' ? ' · недоступен'
          : ready ? '' : ' · проверяем';
        return {value:account,text:account+suffix};
      });
      updateOptions(accounts,options);
      accounts.value=selected;
      alerts=data.alerts || [];
      alertsView.textContent=alerts.map(alert=>`${alert.symbol || (alert.account ? 'Счёт '+alert.account : 'Связанные счета')}: ${alert.reason}`).join(' · ');
      alertsView.hidden=!alerts.length;
    }
    async function readState(preserveResult = false) {
      loading=true;
      buttons();
      const requestGeneration=generation, selectedAccount=accounts.value,
        requestedAccount=navigationTarget?.account || selectedAccount,atCommand=commandGeneration;
      try {
      const data = await ipc.invoke('mt5-vps:orders',requestedAccount || undefined);
      if (generation!==requestGeneration || accounts.value!==selectedAccount || atCommand!==commandGeneration) return;
      if(data.account!=null && requestedAccount && String(data.account)!==requestedAccount)throw Error('Получены данные другого счёта MT5');
      const previous=target();
      const hasPartialState=data.status==='rejected' && /^[1-9][0-9]{0,19}$/.test(String(data.account))
        && typeof data.server==='string' && data.server && data.accountStates && !Array.isArray(data.accountStates);
      if(hasPartialState)updateAccountDisplay(data);
      const exact=data.status==='ok' && navigationTarget && data.orders.some(row=>matchesTarget(row,navigationTarget,data.account));
      if(exact)allowPeerTarget=false;
      else if(navigationTarget && allowPeerTarget && (data.status==='ok' || !previous)) {
        const peer=peerProtectionTarget(data,navigationTarget);
        // A peer row locates the parent only. Load the peer's own controls and
        // receipt barriers before enabling any action on its position.
        if(peer)return retarget(peer,false);
      }
      if(hasPartialState){
        connected=false;cachedDisplay=!!previous;
        const health=accountStates[String(data.account)];
        status.textContent=`Счёт ${data.account} · ${health?.connectionState==='unavailable' ? 'недоступен' : 'проверяем позиции и заявки…'}`;
        status.title=data.reason || '';
        if(!previous)choose(true);
        return;
      }
      if (data.status !== 'ok') throw Error(data.reason || 'Нет связи с MT5');
      if(requestedAccount && data.account==null)throw Error('Получены данные другого счёта MT5');
      const selected = select.value;
      rows = data.orders; capabilities = data.capabilities; commands = data.commands; rules=data.rules || []; parentTakes=data.parentTakes || []; timeRules=data.timeRules || [];connected=true;cachedDisplay=false;
      nativeObservedAt=Number.isFinite(data.checkedAt) ? data.checkedAt : undefined;
      observedAt=nativeObservedAt ?? Date.now();
      if(lastProtectionReceipt?.context.account===String(data.account)) {
        // An active view may serve a cached native read even after the save.
        if(nativeObservedAt<lastProtectionReceipt.checkedAt)
          rows=protectionReceiptView(rows,lastProtectionReceipt.result,{...lastProtectionReceipt.context,checkedAt:nativeObservedAt});
        else lastProtectionReceipt=null;
      }
      updateAccountDisplay(data);
      status.textContent = `Счёт ${data.account} · проверено ${new Date(data.checkedAt || Date.now()).toLocaleTimeString()} · обновление каждые 2 секунды${capabilities.includes('close') ? '' : ' · приёмник VPS требует обновления'}`;
      status.title='';
      updateOptions(select,rows.map((row,index)=>({value:String(row.ticket),text:`${row.symbol} · ${row.type.toUpperCase()} · ${row.lots} л.${rows.length>1 || isEntryPlanRow(row)?' · '+(isEntryPlanRow(row)?'часть ':'№')+(index+1):''}`})));
      if(navigationTarget) {
        const exact=rows.find(row=>matchesTarget(row,navigationTarget,data.account));
        select.value=exact ? String(exact.ticket) : '';
        if(!exact)showMessage('Позиция выбранного набора пока не подтверждена. Обнови список.','Проверка');
      } else if (rows.some(row => String(row.ticket) === selected) && select.value!==selected) select.value = selected;
      const nextHistoryKey=JSON.stringify(commands.slice(0,20));
      if(nextHistoryKey!==historyKey){
      historyKey=nextHistoryKey;history.replaceChildren();
      for (const command of commands.slice(0,20)) {
        const line = document.createElement('p');
        line.textContent = `${new Date(command.at).toLocaleString()} · ${command.operation} #${command.ticket} · ${command.result.status}${command.result.reason ? ' · '+command.result.reason : ''}`;
        if (command.result.accountResults) line.textContent += ' · '+Object.entries(command.result.accountResults).map(([account,result])=>`${account}: ${result.status}`).join(' · ');
        const timings=timingLines(command.result);
        if (timings.length) {const detail=document.createElement('div');detail.style.cssText='font-size:12px;color:var(--muted);white-space:pre-line';detail.textContent=timings.join('\n');line.append(detail);}
        history.append(line);
      }
      if (!commands.length) history.textContent = 'Команд управления ещё нет.';
      }
      const current=target();
      const preserve=previous && current && String(previous.ticket)===String(current.ticket)
        && previous.__isPosition===current.__isPosition && Number(previous.lots)===Number(current.lots)
        && previous.entryPlan?.id===current.entryPlan?.id;
      if (previous && current && !preserve && !preserveResult) showMessage('Объём позиции изменился. Проверь заново параметры закрытия.','Объём изменился');
      choose(preserve);
      } catch(error){
        if(generation!==requestGeneration || atCommand!==commandGeneration)return;
        connected=false;cachedDisplay=!!target();if(!cachedDisplay)choose(true);throw error;
      }
      finally {if(generation===requestGeneration){loading=false;buttons();}}
    }
    function renderConditions() {
      const nextKey=JSON.stringify([dataAccount(),select.value,rules.filter(rule=>String(rule.ticket)===String(select.value)),timeRules.filter(rule=>String(rule.ticket)===String(select.value))]);
      if(nextKey===conditionsKey)return;
      conditionsKey=nextKey;
      conditions.replaceChildren();
      const stateNames={armed:'Ожидает цены',firing:'Отправляется',completed:'Выполнено',cancelled:'Отменено',rejected:'Отклонено',unknown:'Результат неизвестен — проверь MT5'};
      for (const rule of rules.filter(rule=>String(rule.ticket)===String(select.value))) {
        const line=document.createElement('p');
        line.textContent=`#${rule.ticket} · ${rule.closePercent ? rule.closePercent+'% · ' : ''}${rule.volume} лота · ${rule.comparison==='gte' ? '≥' : '≤'} ${rule.targetPrice} · ${stateNames[rule.state] || rule.state}${rule.result?.reason ? ' · '+rule.result.reason : ''}`;
        for (const text of timingLines({accountResults:rule.executionResults || {[dataAccount()]:rule.result}})) {
          const detail=document.createElement('div');detail.style.cssText='font-size:12px;color:var(--muted)';detail.textContent=text;line.append(detail);
        }
        if (rule.state==='armed') {
          const button=document.createElement('button'); button.textContent='Отменить условие';
          button.onclick=()=>run('cancel-close',{ruleId:rule.id},rows.find(row=>String(row.ticket)===String(rule.ticket)) || {ticket:rule.ticket,symbol:rule.symbol,__isPosition:true});
          line.append(button);
        }
        conditions.append(line);
      }
      for (const rule of timeRules.filter(rule=>String(rule.ticket)===String(select.value))) {
        const line=document.createElement('p');line.dataset.timeRule=rule.id;
        const names={armed:'Ожидает срока',opening:'Отправляется открытие',firing:'Закрывается',completed:'Завершено',rejected:'Отклонено',unknown:'Результат неизвестен — проверь MT5'};
        line.textContent=`#${rule.ticket || '…'} · ${rule.closeLocalDate} ${rule.closeLocalTime} Нью-Йорк · закрыть остаток / отменить лимитку · ${names[rule.state] || rule.state}${rule.result?.reason ? ' · '+rule.result.reason : ''}`;
        conditions.append(line);
      }
      conditionsPanel.hidden=!conditions.childElementCount;
      conditionsTitle.textContent=`Условия сделки · ${conditions.childElementCount}`;
      if ([...rules,...timeRules].some(rule=>String(rule.ticket)===String(select.value) && ['unknown','firing'].includes(rule.state))) conditionsPanel.open=true;
    }
    async function run(action, extra = {}, selected = target()) {
      if (busy || !selected) return;
      const clickAt=Date.now(),selectedAccount=dataAccount();
      commandBusy=true;busy = true; buttons(); showMessage(readPromise ? 'Проверяю позицию…' : 'Отправляю команду…');
      speed.textContent='';
      let submitted=false;
      try {
        // A periodic read must not swallow a click. Wait for that same read,
        // then revalidate the captured target; never queue a second POST.
        const exactClose=['entry-close','entry-partial-close','entry-take'].includes(action) && isEntryPlanRow(selected) && capabilities.includes('entry-exact-parent-close-v1');
        const exactProtection=action==='entry-protection' && capabilities.includes(EXACT_PROTECTION_CAPABILITY);
        if(readPromise && !exactClose && !exactProtection)await readPromise;
        const current=target();
        if(dataAccount()!==selectedAccount || !current || String(current.ticket)!==String(selected.ticket)
          || current.__isPosition!==selected.__isPosition || !exactClose && Number(current.lots)!==Number(selected.lots))
          throw Error('Позиция или её объём изменились. Проверь параметры заново.');
        const hybrid=isEntryPlanRow(current),wholeProtection=action==='entry-protection',wholeClose=['entry-close','entry-partial-close'].includes(action),wholeTake=action==='entry-take';
        if(action==='entry-partial-close' && !capabilities.includes(PARENT_PARTIAL_CAPABILITY))throw Error('Обнови приёмник VPS для закрытия части всего набора.');
        if((wholeProtection || wholeClose || wholeTake) && current.entryPlan?.id!==selected.entryPlan?.id)throw Error('Набор БМЛ/СМЛ изменился. Обнови список.');
        const blocked=exactProtection ? exactEntryProtectionBlock({row:current,account:dataAccount(),linked:mirror.checked,capabilities,commands,rules,timeRules,accountStates})
          : exactClose ? exactEntryCloseBlock({row:current,account:dataAccount(),capabilities,commands,rules,timeRules})
          : (connected ? managementBlock({row:current,account:dataAccount(),linked:!hybrid && mirror.checked,commands,rules,timeRules,alerts,accountStates}) : 'Нет подтверждённой связи с MT5. Обнови список.')
            || ((wholeProtection || wholeClose) ? entryProtectionBlock({row:current,account:dataAccount(),linked:wholeClose || mirror.checked,capabilities,commands,accountStates}) : '');
        if(blocked){showMessage(blocked,'Проверка');return;}
        showMessage('Отправляю команду…');
        submitted=true;
        const commandTarget={...selected,singleAccount:hybrid && !wholeProtection ? true : !mirror.checked,clickAt};
        const result = await ipc.invoke('mt5-vps:manage',{action,target:commandTarget,account:selectedAccount,...extra});
        commandGeneration++;
        if(wholeTake && result.status==='ok' && result.rule){parentTakes=[result.rule,...parentTakes.filter(rule=>rule.id!==result.rule.id)];takeDirty=false;}
        const previousRows=rows;
        if(wholeProtection) {
          const context={target:commandTarget,account:selectedAccount,server:accountStates[selectedAccount]?.server};
          rows=protectionReceiptView(rows,result,{...context,checkedAt:nativeObservedAt});
          if(rows!==previousRows)lastProtectionReceipt={context,result,checkedAt:result.accountResults[selectedAccount].protectionProof.checkedAt};
        }
        if(result?.id)commands=[{id:result.id,at:clickAt,operation:action,ticket:wholeProtection||wholeClose||wholeTake?`entry:${current.entryPlan.id}`:String(current.ticket),
          payload:{entryId:current.entryPlan?.id},result},...commands.filter(row=>row.id!==result.id)];
        const savedProtection=result.status==='ok' && (action==='modify' || wholeProtection);
        if(savedProtection){for(const key of Object.keys(extra.changes || {}))dirty.delete(key);breakeven=false;}
        const partlyProtected=wholeProtection && result.status!=='ok'
          && Object.values(result.accountResults || {}).some(outcome=>outcome.status==='ok' && outcome.noPosition!==true);
        const closedPart=action==='entry-partial-close' && result.status==='ok'
          ? 'MT5 подтвердил закрытие части набора. '+Object.entries(result.accountResults || {}).map(([account,value])=>
            value.noPosition===true ? `${account}: нет позиции`
              : `${account}: закрыто ${Number(value.filledVolume).toLocaleString('ru-RU',{maximumFractionDigits:8})} л. · остаток ${Number(value.remainingVolume).toLocaleString('ru-RU',{maximumFractionDigits:8})} л.`).join(' · ') : '';
        const resultText=partlyProtected ? 'MT5 подтвердил изменение защиты на части счетов.'
          : result.status === 'ok' ? (closedPart || (wholeTake ? (extra.take.operation==='arm'?'Частичный тейк сохранён на VPS. SL и обычный TP сохранены.':'Частичный тейк отменён.') : action==='schedule-close' ? 'Условие сохранено на VPS.' : action==='cancel-close' ? 'Условие отменено.' : 'MT5 подтвердил выполнение.')) : `${result.status === 'unknown' ? 'Результат неизвестен. Повтор заблокирован.' : 'Команда отклонена.'} ${result.reason || ''}`;
        const accountText=closedPart ? '' : accountControlDetails(result);
        showMessage([resultText,accountText].filter(Boolean).join(' '),partlyProtected?'Частично подтверждено':result.status==='ok'?'Готово':result.status==='unknown'?'Проверка':'Ошибка');
        speed.textContent=timingLines(result).join('\n');
        // Without fresh prices in the receipt, keep displaying the edited
        // prices until the post-command read supplies the broker values.
        choose(true,savedProtection && rows===previousRows);
        const earlierRead=readPromise;
        const refreshAfterCommand=async()=>{
          if(earlierRead)await earlierRead.catch(()=>{});
          return reload(true);
        };
        // Only a committed success releases actions before the display read.
        // Unknown/refused outcomes retain the original refresh barrier.
        if(result.status==='ok')refreshAfterCommand().catch(error=>{status.textContent='Обновление состояния: '+error.message;});
        else await refreshAfterCommand();
      } catch (error) { showMessage(submitted ? 'Не удалось получить результат. Обнови список и проверь журнал: '+error.message : 'Команда не отправлена: '+error.message,submitted?'Проверка':'Ошибка'); }
      finally {
        commandBusy=false;busy=false;buttons();
        if(pendingTarget && dialog.open){const wanted=pendingTarget;pendingTarget=null;retarget(wanted);}
      }
    }
    modify.onclick = () => {
      const row = target(); if (!row) return;
      try {
        const hybrid=isEntryPlanRow(row),fields=breakeven && hybrid ? new Set([...dirty].filter(key=>key!=='sl')) : dirty;
        const changes=protectionChanges(row,{sl:sl.value,tp:tp.value,price:price.value},fields);
        if(!Object.keys(changes).length && !(hybrid && breakeven)){showMessage('Сначала измени стоп или тейк.');return;}
        run(hybrid?'entry-protection':'modify',{changes,...(hybrid?{operation:breakeven?'breakeven':'modify'}:{})});
      }catch(error){showMessage(error.message,'Ошибка');}
    };
    const closeSize=()=>sizeMode.value==='percent' ? {percent:String(decimal(percent.value))} : {volume:String(decimal(volume.value))};
    for(const field of [takePrice,takePercent])field.oninput=()=>{takeDirty=true;buttons();};
    takeSave.onclick=()=>{try{run('entry-take',{take:{operation:'arm',targetPrice:decimal(takePrice.value),closePercent:decimal(takePercent.value)}});}catch(error){showMessage(error.message,'Ошибка');}};
    takeCancel.onclick=()=>{const rule=parentTake();if(rule)run('entry-take',{take:{operation:'cancel',ruleId:rule.id}});};
    partial.onclick = () => {try{
      const row=target();
      if(isEntryPlanRow(row)){
        const value=decimal(sizeMode.value==='percent'?percent.value:volume.value);
        const preview=parentClosePreview(row,dataAccount(),accountStates,sizeMode.value,value);
        if(preview.preview.some(item=>item.error))throw Error('Проверь объёмы всего набора на каждом счёте.');
        run('entry-partial-close',{closePercent:preview.percent});
      }else run('close',closeSize());
    }catch(error){showMessage(error.message,'Ошибка');}};
    full.onclick = () => isEntryPlanRow(target()) ? run('entry-close') : run('close',{volume:target()?.lots});
    cancel.onclick = () => run('cancel');
    schedule.onclick = () => {try{run('schedule-close',{...closeSize(),targetPrice:String(decimal(trigger.value)),comparison:comparison.value});}catch(error){showMessage(error.message,'Ошибка');}};
    refresh.onclick = async () => {
      if(busy)return;
      const requestedGeneration=generation;
      for(const symbol of chartAttempts)if(!chartReads.has(symbol) && !chartSpecifications.has(symbol))chartAttempts.delete(symbol);
      // A display refresh cannot lock an already captured exact protection intent.
      busy=!(isEntryPlanRow(target()) && capabilities.includes(EXACT_PROTECTION_CAPABILITY));buttons();
      try{await reload();}catch(error){if(generation===requestedGeneration)status.textContent=error.message;}
      finally{if(generation===requestedGeneration){busy=commandBusy;buttons();}}
    };
    const poll=setInterval(() => {
      if (dialog.open && !document.hidden && !busy && !loading) reload().catch(error=>{status.textContent=error.message;});
    },2000);
    retarget=async (wanted,canUsePeer=true)=>{
      if(commandBusy){pendingTarget=wanted;return;}
      showMessage('');speed.textContent='';
      navigationTarget=wanted;allowPeerTarget=canUsePeer;const requestedGeneration=++generation;
      title.textContent=`Стоп и тейк · ${wanted.symbol}`;
      dirty.clear();breakeven=false;
      const age=Date.now()-observedAt,fresh=connected && age>=-250 && age<=2500;
      // Exact protection proves the captured tuple on the VPS. Reopening its
      // display need not wait for a new GET; legacy actions retain freshness.
      const cached=String(accounts.value)===wanted.account && (fresh || capabilities.includes(EXACT_PROTECTION_CAPABILITY))
        ? rows.find(row=>matchesTarget(row,wanted,accounts.value)) : null;
      cachedDisplay=!!cached && !fresh;
      if(accounts.value!==wanted.account){
        rows=[];select.replaceChildren();
        if(![...accounts.options].some(option=>option.value===wanted.account)){
          const option=document.createElement('option');option.value=option.textContent=wanted.account;accounts.append(option);
        }
        accounts.value=wanted.account;
      }
      if(!cached || cachedDisplay)connected=false;
      if(!cached)status.textContent='Проверяем позиции выбранного счёта…';
      select.value=cached?String(cached.ticket):'';choose();
      busy=!cached;buttons();
      try {if(generation!==requestedGeneration)return;await reload(false,true);}
      catch(error){if(generation===requestedGeneration)status.textContent=error.message;}
      finally{if(generation===requestedGeneration){busy=commandBusy;buttons();}}
    };
    dialog.addEventListener('cancel',()=>{navigationTarget=null;chart.cancel();});
    dialog.addEventListener('close',()=>{if(!dialog.open){navigationTarget=null;chart.cancel();}});
    window.addEventListener('beforeunload',()=>{clearInterval(poll);minuteHistory.close();ipc.removeListener?.('instrument:updated',quoteHandler);chart.destroy();},{once:true});
    if(navigationTarget){
      title.textContent=`Стоп и тейк · ${navigationTarget.symbol}`;select.hidden=true;
      const account=document.createElement('option');account.value=account.textContent=navigationTarget.account;accounts.append(account);
    }
    document.body.append(dialog); dialog.showModal(); refresh.onclick();
  };
  launch.onclick=openPanel;
}
module.exports = {mountManagement,peerProtectionTarget};

const {closeVolumes}=require('./sizing');
const {timingLines}=require('./timings');
const {managementBlock,decimal,protectionChanges,isEntryPlanRow,entryPlanMetadata,entryProtectionBlock,protectionView}=require('./managementPolicy');
function mountManagement(ipc, toolbar) {
  const launch = document.createElement('button');
  launch.id = 'mt5-manage-button';
  launch.textContent = 'Позиции · MT5';
  toolbar.append(launch);
  let dialog;
  launch.onclick = () => {
    if (dialog) { dialog.showModal(); dialog.querySelector('#mt5-refresh').click(); return; }
    dialog = document.createElement('dialog');
    dialog.id = 'mt5-management';
    dialog.style.cssText = 'box-sizing:border-box;width:min(520px,94vw);max-width:94vw;min-width:0;max-height:88vh;overflow:auto;overflow-wrap:anywhere;color:var(--text);background:var(--panel,#20242b);border:1px solid var(--border);border-radius:12px;padding:14px;font-size:13px';
    const make = (tag, text) => { const element = document.createElement(tag); if (text) element.textContent = text; dialog.append(element); return element; };
    const title=make('h2','Позиции · MT5');
    const status = make('p','Проверяю подключение…');
    const alertsView = make('p'); alertsView.id='mt5-account-alerts'; alertsView.setAttribute('role','alert');
    const accounts = make('select'); accounts.id = 'mt5-account'; accounts.style.cssText = 'box-sizing:border-box;display:block;width:100%;min-width:0;padding:8px;margin-bottom:12px';
    const refresh = make('button','↻');refresh.title='Обновить позиции';refresh.setAttribute('aria-label','Обновить позиции');
    refresh.id = 'mt5-refresh';
    const dismiss = make('button','×');dismiss.setAttribute('aria-label','Закрыть окно позиций');
    dismiss.style.marginLeft = '10px'; dismiss.onclick = () => dialog.close();
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
      #mt5-management #mt5-full-close{border-color:#ac5c48}
      #mt5-management #mt5-account-alerts{font-size:12px;color:#e0b96b}
      #mt5-management #mt5-management-block{font-size:12px;color:#e0b96b}
      #mt5-management .mt5-protection-fields{display:grid;grid-template-columns:repeat(2,minmax(0,1fr))}
      #mt5-management .mt5-protection-fields input{width:100%!important}
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
      #mt5-management #mt5-full-close{width:100%}
      #mt5-management #mt5-command-result:empty{display:none}
      #mt5-management [hidden]{display:none!important}
    `;
    const header=make('div');header.className='mt5-row';header.prepend(title);header.append(refresh,dismiss);title.style.marginRight='auto';dialog.prepend(header);
    const accountRow=make('div');accountRow.className='mt5-row';accountRow.append(accounts,mirrorLabel);header.after(alertsView,accountRow,blockNote,select,selectionSummary);
    status.className=detail.className='mt5-note';status.style.fontVariantNumeric='tabular-nums';label.textContent='Позиция или заявка';label.className='mt5-note';
    label.hidden=true;select.setAttribute('aria-label','Позиция или заявка');accounts.setAttribute('aria-label','Счёт');
    accounts.style.cssText='max-width:100%;min-width:0';select.style.padding='6px';
    const section=(text)=>{const block=make('div');block.className='mt5-section';const heading=document.createElement('strong');heading.textContent=text;block.append(heading);return block;};
    const protection=section('Стоп и тейк');fields.className='mt5-fields mt5-protection-fields';fields.removeAttribute('style');
    const protectionActions=document.createElement('div');protectionActions.className='mt5-action';protectionActions.append(modify,toEntry);protection.append(fields,protectionActions);protectionNote.className='mt5-note';
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
    let rows = [], capabilities = [], commands = [], rules = [], timeRules=[], alerts = [], accountStates={}, connected=false, busy = false, loading = false, generation = 0;
    const dirty=new Set();
    let readPromise=null,breakeven=false;
    let closePreviewKey=null,historyKey=null,conditionsKey=null;
    const target = () => rows.find(row => String(row.ticket) === select.value);
    function buttons() {
      const row = target(),hybrid=isEntryPlanRow(row);
      volume.closest('label').hidden=sizeMode.value==='percent';percent.closest('label').hidden=sizeMode.value!=='percent';
      price.closest('label').hidden=hybrid || Boolean(row?.__isPosition);partial.hidden=full.hidden=!row?.__isPosition;cancel.hidden=Boolean(row?.__isPosition);
      protection.hidden=closure.hidden=!row;conditional.hidden=!row?.__isPosition;closeRow.hidden=!row?.__isPosition;closePreview.hidden=!row?.__isPosition;
      closure.querySelector('strong').textContent=row?.__isPosition ? (hybrid?'Закрыть выбранную часть':'Закрыть часть'):'Отложенная заявка';
      const blocked = connected ? managementBlock({row,account:dataAccount(),linked:!hybrid && mirror.checked,commands,rules,timeRules,alerts,accountStates}) : 'Нет подтверждённой связи с MT5. Обнови список.';
      const protectionBlocked=blocked || entryProtectionBlock({row,account:dataAccount(),linked:mirror.checked,capabilities,commands,accountStates});
      blockNote.textContent=protectionBlocked;blockNote.hidden=!protectionBlocked || !row;
      mirror.disabled=busy || (hybrid ? !(row.entryPlan?.accounts?.length>1) : !row?.mirrorAccount);
      modify.disabled = busy || protectionBlocked || !row || !hybrid && !capabilities.includes('modify');
      toEntry.disabled=busy || !row?.__isPosition || hybrid && Boolean(protectionBlocked);toEntry.hidden=!row?.__isPosition;
      cancel.disabled = busy || blocked || !row || row.__isPosition || !capabilities.includes('cancel');
      full.disabled = busy || blocked || !row?.__isPosition || !capabilities.includes('close') || hybrid && Boolean(entryProtectionBlock({row,account:dataAccount(),linked:true,capabilities,commands,accountStates}));
      full.textContent=hybrid ? 'Закрыть всё · все счета' : 'Закрыть всю позицию';
      let preview=[];
      try {preview=closeVolumes(row,dataAccount(),!hybrid && mirror.checked,sizeMode.value,decimal(sizeMode.value==='percent' ? percent.value : volume.value));}
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
      partial.disabled = full.disabled || !capabilities.includes('partial-close') || !preview.length || preview.some(item=>item.error);
      partial.textContent=sizeMode.value==='percent' ? `Закрыть ${percent.value || '…'}%` : 'Закрыть часть';
      let triggerPrice=0;try{triggerPrice=decimal(trigger.value);}catch{}
      schedule.disabled = partial.disabled || !capabilities.includes('schedule-close') || !(triggerPrice>0)
        || rules.some(rule=>String(rule.ticket)===String(row?.ticket) && rule.state==='armed');
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
      for (const button of conditions.querySelectorAll('button')) button.disabled=busy;
      refresh.disabled = busy;
    }
    function choose(preserve = false) {
      const row = target(),hybrid=isEntryPlanRow(row);
      let protectionRow=row;try{protectionRow=protectionView(row);}catch{protectionRow={};}
      detail.textContent = row ? (hybrid ? `Набор БМЛ/СМЛ · средняя ${protectionRow.open_price ?? '—'} · часть #${row.ticket}: ${row.lots} лота` : `Вход ${row.open_price} · ${row.lots} лота${row.__isPosition ? ` · P/L ${row.pnl ?? '—'} USD` : ''}`) : 'Открытых позиций и заявок по SPX500/USTEC нет.';
      selectionSummary.textContent=row ? `${hybrid?'Средняя':'Вход'} ${protectionRow.open_price ?? '—'}` : 'Позиций и заявок нет';
      protection.querySelector('strong').textContent=hybrid?'SL/TP · весь набор':'Стоп и тейк';
      if(!preserve){dirty.clear();breakeven=false;}
      for(const [key,field,value] of [['sl',sl,hybrid && breakeven ? protectionRow?.open_price : protectionRow?.sl],['tp',tp,protectionRow?.tp],['price',price,row?.open_price]]){
        const next=String(value ?? '');
        if(!dirty.has(key) && (!preserve || document.activeElement!==field) && field.value!==next)field.value=next;
      }
      const step = Number(row?.quantityStep), lots = Number(row?.lots);
      volume.step = step > 0 ? step : 'any'; volume.min = row?.minQty || '0';
      if (!preserve) {
        volume.value = step > 0 ? Number((Math.floor((lots/2+1e-9)/step)*step).toFixed(8)) : '';
        trigger.value = row?.currentPrice ?? ''; comparison.value = row?.type==='sell' ? 'lte' : 'gte';
        mirror.checked=hybrid ? row.entryPlan?.accounts?.length>1 : Boolean(row?.mirrorAccount);
        percent.value='50';
      }
      mirrorLabel.hidden=hybrid ? !(row.entryPlan?.accounts?.length>1) : !row?.mirrorAccount;
      mirrorLabel.firstChild.textContent=hybrid ? 'SL/TP на всех счетах ' : (row?.mirrorAccounts?.length>1 ? 'Все счета ' : 'Оба счёта ');
      modify.textContent = breakeven && hybrid ? 'Сохранить безубыток' : 'Сохранить';
      modify.title=hybrid?'Сохранить SL/TP всего набора на выбранных счетах':row?.__isPosition?'Сохранить SL/TP':'Сохранить заявку';
      toEntry.title=hybrid ? 'Подготовить стоп по средней цене всего набора; при сохранении VPS заново проверит среднюю каждого счёта' : 'Подставить цену входа в поле стопа; затем нажми «Сохранить»';
      protectionNote.textContent=hybrid ? 'SL/TP — для всего набора БМЛ/СМЛ на выбранных счетах. Стоп во вход — по общей средней каждого счёта. 0 снимает тейк. Частичное закрытие — только выбранная часть.' : 'SL / TP — цены; 0 снимает уровень.';
      renderConditions();
      buttons();
    }
    const dataAccount = () => accounts.value;
    select.onchange = () => choose(); volume.oninput = buttons; trigger.oninput=buttons; mirror.onchange=buttons;
    percent.oninput=buttons; sizeMode.onchange=buttons;
    for(const [key,field] of [['sl',sl],['tp',tp],['price',price]])field.oninput=()=>{if(key==='sl')breakeven=false;dirty.add(key);if(isEntryPlanRow(target()))modify.textContent=breakeven?'Сохранить безубыток':'Сохранить';buttons();};
    toEntry.onclick=()=>{
      const row=target();if(busy || !row?.__isPosition)return;
      if(isEntryPlanRow(row)) {
        const blocked=entryProtectionBlock({row,account:dataAccount(),linked:mirror.checked,capabilities,commands,accountStates});
        if(blocked){showMessage(blocked,'Проверка');return;}
        sl.value=String(entryPlanMetadata(row).averagePrice);breakeven=true;dirty.delete('sl');
        modify.textContent='Сохранить безубыток';
        showMessage('Показана средняя всего набора. При сохранении VPS заново рассчитает её на каждом выбранном счёте.','Нажми «Сохранить безубыток»');
      } else {sl.value=String(row.open_price);dirty.add('sl');showMessage('Стоп подставлен по цене входа. Нажми «Сохранить», чтобы отправить.','Нажми «Сохранить»');}
    };
    accounts.onchange = () => {
      generation++; rows=[]; select.replaceChildren(); choose();
      refresh.click();
    };
    function reload(preserveResult=false) {
      if(readPromise)return readPromise;
      readPromise=readState(preserveResult).finally(()=>{readPromise=null;});
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
    async function readState(preserveResult = false) {
      loading=true;
      buttons();
      const requestGeneration=generation, requestedAccount=accounts.value;
      try {
      const data = await ipc.invoke('mt5-vps:orders',requestedAccount || undefined);
      if (generation!==requestGeneration || accounts.value!==requestedAccount) return;
      if (data.status !== 'ok') throw Error(data.reason || 'Нет связи с MT5');
      const selected = select.value, previous=target();
      rows = data.orders; capabilities = data.capabilities; commands = data.commands; rules=data.rules || []; timeRules=data.timeRules || [];alerts=data.alerts || [];accountStates=data.accountStates || {};connected=true;
      alertsView.textContent=alerts.map(alert=>`${alert.symbol || (alert.account ? 'Счёт '+alert.account : 'Связанные счета')}: ${alert.reason}`).join(' · ');
      alertsView.hidden=!alerts.length;
      updateOptions(accounts,(data.accounts || [data.account]).map(account=>({value:String(account),text:String(account)})));
      if(accounts.value!==String(data.account))accounts.value=String(data.account);
      status.textContent = `Счёт ${data.account} · проверено ${new Date(data.checkedAt || Date.now()).toLocaleTimeString()} · обновление каждые 2 секунды${capabilities.includes('close') ? '' : ' · приёмник VPS требует обновления'}`;
      updateOptions(select,rows.map((row,index)=>({value:String(row.ticket),text:`${row.symbol} · ${row.type.toUpperCase()} · ${row.lots} л.${rows.length>1 || isEntryPlanRow(row)?' · '+(isEntryPlanRow(row)?'часть ':'№')+(index+1):''}`})));
      if (rows.some(row => String(row.ticket) === selected) && select.value!==selected) select.value = selected;
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
      } catch(error){connected=false;throw error;} finally { loading=false; buttons(); }
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
      busy = true; buttons(); showMessage(readPromise ? 'Проверяю позицию…' : 'Отправляю команду…');
      speed.textContent='';
      let submitted=false;
      try {
        // A periodic read must not swallow a click. Wait for that same read,
        // then revalidate the captured target; never queue a second POST.
        if(readPromise)await readPromise;
        const current=target();
        if(dataAccount()!==selectedAccount || !current || String(current.ticket)!==String(selected.ticket)
          || current.__isPosition!==selected.__isPosition || Number(current.lots)!==Number(selected.lots))
          throw Error('Позиция или её объём изменились. Проверь параметры заново.');
        const hybrid=isEntryPlanRow(current),wholeProtection=action==='entry-protection',wholeClose=action==='entry-close';
        if((wholeProtection || wholeClose) && current.entryPlan?.id!==selected.entryPlan?.id)throw Error('Набор БМЛ/СМЛ изменился. Обнови список.');
        const blocked=(connected ? managementBlock({row:current,account:dataAccount(),linked:!hybrid && mirror.checked,commands,rules,timeRules,alerts,accountStates}) : 'Нет подтверждённой связи с MT5. Обнови список.')
          || ((wholeProtection || wholeClose) ? entryProtectionBlock({row:current,account:dataAccount(),linked:wholeClose || mirror.checked,capabilities,commands,accountStates}) : '');
        if(blocked){showMessage(blocked,'Проверка');return;}
        showMessage('Отправляю команду…');
        submitted=true;
        const result = await ipc.invoke('mt5-vps:manage',{action,target:{...selected,singleAccount:hybrid && !wholeProtection ? true : !mirror.checked,clickAt},account:accounts.value,...extra});
        if(result.status==='ok' && ['modify','entry-protection'].includes(action)){for(const key of Object.keys(extra.changes || {}))dirty.delete(key);breakeven=false;}
        const resultText=result.status === 'ok' ? (action==='schedule-close' ? 'Условие сохранено на VPS.' : action==='cancel-close' ? 'Условие отменено.' : 'MT5 подтвердил выполнение.') : `${result.status === 'unknown' ? 'Результат неизвестен. Повтор заблокирован.' : 'Команда отклонена.'} ${result.reason || ''}`;
        const accountText=result.accountResults ? ' '+Object.entries(result.accountResults).map(([account,value])=>`${account}: ${value.status}`).join(' · ') : '';
        showMessage(resultText+accountText,result.status==='ok'?'Готово':result.status==='unknown'?'Проверка':'Ошибка');
        speed.textContent=timingLines(result).join('\n');
        await reload(true);
      } catch (error) { showMessage(submitted ? 'Не удалось получить результат. Обнови список и проверь журнал: '+error.message : 'Команда не отправлена: '+error.message,submitted?'Проверка':'Ошибка'); }
      finally { busy = false; buttons(); }
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
    partial.onclick = () => {try{run('close',closeSize());}catch(error){showMessage(error.message,'Ошибка');}};
    full.onclick = () => isEntryPlanRow(target()) ? run('entry-close') : run('close',{volume:target()?.lots});
    cancel.onclick = () => run('cancel');
    schedule.onclick = () => {try{run('schedule-close',{...closeSize(),targetPrice:String(decimal(trigger.value)),comparison:comparison.value});}catch(error){showMessage(error.message,'Ошибка');}};
    refresh.onclick = async () => { if (busy) return; busy = true; buttons(); try { await reload(); } catch (error) { status.textContent = error.message; } finally { busy = false; buttons(); } };
    const poll=setInterval(() => {
      if (dialog.open && !document.hidden && !busy && !loading) reload().catch(error=>{status.textContent=error.message;});
    },2000);
    window.addEventListener('beforeunload',()=>clearInterval(poll),{once:true});
    document.body.append(dialog); dialog.showModal(); refresh.onclick();
  };
}
module.exports = {mountManagement};

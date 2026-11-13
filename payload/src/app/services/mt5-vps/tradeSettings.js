function mountTradeSettings(ipc,toolbar,{accounts=true,timeExit=true,openInstrumentSettings}={}) {
  const launch=document.createElement('button');launch.id='mt5-trade-settings-button';launch.textContent=accounts?'Счета MT5':'Закрытие по времени · MT5';toolbar.append(launch);
  let dialog;
  launch.onclick=()=>{
    if (dialog) {dialog.showModal();dialog.querySelector('#mt5-settings-refresh').click();return;}
    dialog=document.createElement('dialog');dialog.id='mt5-trade-settings';
    dialog.style.cssText='box-sizing:border-box;width:min(520px,92vw);max-height:85vh;overflow:auto;color:var(--text);background:var(--panel,#20242b);border:1px solid var(--border);border-radius:12px;padding:16px;font-size:14px';
    const make=(tag,text,parent=dialog)=>{const e=document.createElement(tag);if(text)e.textContent=text;parent.append(e);return e;};
    make('h2',launch.textContent);
    const style=make('style');style.textContent=`
      #mt5-trade-settings h2{font-size:18px;margin:0 0 8px}
      #mt5-trade-settings p{margin:6px 0;font-size:12px;line-height:1.4}
      #mt5-trade-settings p:empty{display:none}
      #mt5-trade-settings fieldset{min-width:0;margin:0 0 8px;border:1px solid var(--border);border-radius:8px;padding:8px}
      #mt5-trade-settings input:not([type=checkbox]),#mt5-trade-settings select,#mt5-trade-settings button{box-sizing:border-box;max-width:100%;padding:5px 7px;border:1px solid var(--border);border-radius:7px;color:var(--text);background:var(--bg,#111);font-size:13px}
      #mt5-trade-settings button,#mt5-trade-settings summary{cursor:pointer}
      #mt5-trade-settings button:disabled{opacity:.45;cursor:default}
      #mt5-trade-settings .account-controls{display:flex;flex-wrap:wrap;align-items:center;gap:6px}
      #mt5-trade-settings .account-controls label{display:inline-flex;align-items:center;gap:4px}
      #mt5-trade-settings .account-row{padding:6px 0;border-bottom:1px solid var(--border)}
      #mt5-trade-settings .account-row:last-child{border:0}
      #mt5-trade-settings .account-footer{margin-top:6px}
      #mt5-trade-settings details{margin:8px 0}
      #mt5-trade-settings details[open]>summary{margin-bottom:6px}
      #mt5-trade-settings #mt5-copy-transfer[hidden]{display:none}
      #mt5-trade-settings [data-tradehub-role][hidden]{display:none}
      #mt5-trade-settings [data-tradehub-role]{margin-top:6px}
    `;
    const status=make('p','Проверяю подключение…');status.setAttribute('role','status');
    const primaryRiskPanel=make('fieldset');const primaryTitle=make('legend','Основной счёт',primaryRiskPanel);
    const primaryMode=make('select','',primaryRiskPanel);
    primaryMode.id='mt5-primary-risk-mode';
    primaryMode.setAttribute('aria-label','Режим риска основного счёта');
    for (const [value,label] of [['usd','Доллары из карточки'],['balance','Процент от баланса']]) {
      make('option',label,primaryMode).value=value;
    }
    const primaryPercent=make('input','',primaryRiskPanel);
    primaryPercent.id='mt5-primary-risk-percent';
    primaryPercent.type='number';primaryPercent.min='0.001';primaryPercent.max='100';primaryPercent.step='any';
    primaryPercent.setAttribute('aria-label','Риск основного счёта, % баланса');
    primaryPercent.style.cssText='width:80px;margin-left:8px';primaryPercent.value='0.2';
    const primarySave=make('button','Сохранить риск',primaryRiskPanel);
    primarySave.id='mt5-save-primary-risk';
    primarySave.style.marginLeft='8px';
    const copies=make('fieldset');make('legend','Копирование новых сделок',copies);
    const copiesRows=make('div','',copies);copiesRows.id='mt5-followers';
    const copiesActions=make('div','',copies);copiesActions.className='account-controls account-footer';
    const saveCopies=make('button','Сохранить',copiesActions);saveCopies.id='mt5-save-followers';
    const transferStart=make('button','Перенести копирование',copiesActions);transferStart.id='mt5-copy-transfer-start';
    const transfer=make('div','',copies);transfer.id='mt5-copy-transfer';transfer.hidden=true;
    const transferFields=make('div','',transfer);transferFields.className='account-controls account-footer';
    const sourceLabel=make('label','С ',transferFields),source=make('select','',sourceLabel);source.setAttribute('aria-label','С какого счёта перенести копирование');
    const targetLabel=make('label','На ',transferFields),target=make('select','',targetLabel);target.setAttribute('aria-label','На какой счёт перенести копирование');
    const transferSummary=make('p','',transfer);
    const transferActions=make('div','',transfer);transferActions.className='account-controls';
    const transferSave=make('button','Перенести',transferActions);transferSave.id='mt5-copy-transfer-save';
    const transferCancel=make('button','Отмена',transferActions);
    const recorderPanel=make('details');recorderPanel.id='mt5-tradehub-roles';
    const recorderLabel=make('summary','Роли Trade Hub',recorderPanel);
    recorderLabel.id='mt5-chart-recorder-label';
    const recorderControls=make('div','',recorderPanel);recorderControls.className='account-controls';
    make('label','Уровни и скриншоты',recorderControls).htmlFor='mt5-chart-recorder';
    const recorderAccount=make('select','',recorderControls);recorderAccount.id='mt5-chart-recorder';
    const recorderSave=make('button','Сохранить',recorderControls);recorderSave.id='mt5-save-chart-recorder';
    const recorderStatus=make('p','',recorderPanel);recorderStatus.setAttribute('role','status');
    const roleEditors={};
    for(const [name,label] of [['history','История для замеров'],['ticks','Тики']]) {
      const row=make('div','',recorderPanel);row.dataset.tradehubRole=name;row.hidden=true;
      const controls=make('div','',row);controls.className='account-controls';
      const selectId='mt5-tradehub-role-'+name;
      make('label',label,controls).htmlFor=selectId;
      const select=make('select','',controls);select.id=selectId;
      const save=make('button','Сохранить',controls);
      const status=make('p','',row);status.setAttribute('role','status');
      roleEditors[name]={row,select,save,status};
    }
    if(accounts && openInstrumentSettings) {
      const instruments=make('button','Инструменты · уровни');instruments.id='mt5-candle-instruments-button';
      instruments.onclick=()=>{dialog.close();openInstrumentSettings();};
    }
    const enrollment=make('details');enrollment.id='mt5-enrollment';make('summary','Добавить счёт',enrollment);
    const discover=make('button','Найти терминалы на VPS',enrollment);discover.id='mt5-discover-accounts';
    const catalogStatus=make('p','Сначала войди в новый счёт в отдельном терминале на VPS.',enrollment);
    const discovered=make('div','',enrollment);discovered.id='mt5-account-catalog';
    const exitPanel=make('details');exitPanel.id='mt5-time-exit';make('summary','Автозакрытие',exitPanel);
    const exitLabel=make('label','На всех счетах ',exitPanel);
    const exitEnabled=make('input','',exitLabel);exitEnabled.type='checkbox';exitEnabled.id='mt5-time-exit-enabled';
    const exitTime=make('input','',exitPanel);exitTime.type='time';exitTime.id='mt5-time-exit-clock';exitTime.value='15:00';exitTime.style.margin='0 8px';exitTime.setAttribute('aria-label','Время закрытия Нью-Йорка');make('span','Нью-Йорк',exitPanel);
    make('p','Для новых сделок VPS выбирает ближайшее наступление указанного времени.',exitPanel);
    const exitSave=make('button','Сохранить',exitPanel);exitSave.id='mt5-save-time-exit';
    primaryRiskPanel.hidden=copies.hidden=recorderPanel.hidden=enrollment.hidden=!accounts;
    exitPanel.hidden=!timeExit;
    const message=make('p');message.id='mt5-settings-result';message.setAttribute('role','status');
    const refresh=make('button','Обновить');refresh.id='mt5-settings-refresh';
    const dismiss=make('button','Закрыть');dismiss.style.marginLeft='8px';dismiss.onclick=()=>dialog.close();
    let busy=false,loading=false,ready=false,readPromise=null,copiesDirty=false,exitDirty=false,primaryRiskDirty=false,account,capabilities=[],catalogBusy=false,catalogScanning=false;
    let accountIds=[],savedCopies=[];
    let recorder,recorderDraft;
    function renderRecorder(next) {
      if(!next){recorder=undefined;recorderDraft=undefined;recorderStatus.textContent='Настройка уровней и снимков недоступна.';return;}
      if(recorder && recorder.revision!==next.revision)recorderDraft=undefined;
      recorder=next;
      const current=recorderAccount.value;
      const options=next.accounts.map(row=>[row.account,row.ready]);
      if(JSON.stringify(options)!==recorderAccount.dataset.options){
        recorderAccount.dataset.options=JSON.stringify(options);
        recorderAccount.replaceChildren();
        for(const [id,ready] of options){
          const option=make('option',ready ? id : `${id} · терминал не готов`,recorderAccount);
          option.value=id;option.disabled=!ready;
        }
      }
      recorderAccount.value=recorderDraft && options.some(([id,ready])=>id===current && ready) ? current : next.account;
      recorderStatus.textContent=next.applied ? `${next.account} · USTEC и SPX500` : `${next.account} · ожидаю подтверждения терминала`;
    }
    function renderTradeHub(data) {
      renderRecorder(data.roles.chart);
      for(const [name,editor] of Object.entries(roleEditors)) {
        const next=data.roles[name];
        editor.row.hidden=!next;
        if(!next){editor.state=undefined;continue;}
        if(editor.state?.revision!==next.revision)editor.draft=undefined;
        editor.state=next;
        const signature=JSON.stringify(next.accounts);
        if(signature!==editor.select.dataset.options){
          editor.select.dataset.options=signature;
          editor.select.replaceChildren();
          const empty=make('option','Выбери счёт',editor.select);empty.value='';
          for(const row of next.accounts){
            const option=make('option',row.ready ? row.account : `${row.account} · не готов`,editor.select);
            option.value=row.account;option.disabled=!row.ready;option.title=row.reason || '';
          }
        }
        editor.select.value=next.accounts.some(row=>row.account===editor.draft && row.ready) ? editor.draft : next.pendingAccount || next.account || '';
        const unavailable=next.accounts.find(row=>row.account===next.account && !row.ready);
        editor.status.textContent=next.pendingAccount ? `Ожидаю подтверждения ${next.pendingAccount}. Текущий счёт ${next.account || 'неизвестен'}.${next.pendingReason ? ` ${next.pendingReason}.` : ''}`
          : !next.account ? 'Активный счёт не подтверждён.'
          : unavailable ? `${next.account} · ${unavailable.reason}` : `${next.account} · ${next.applied ? 'подтверждён' : 'ожидаю подтверждения'}`;
      }
    }
    async function readRecorder() {
      if(!accounts)return;
      if(capabilities.includes('tradehub-roles-v1')) {
        try {
          const result=await ipc.invoke('mt5-vps:tradehub-roles');
          if(result.status!=='ok')throw Error(result.reason);
          renderTradeHub(result.tradeHubRoles);
        }catch(error){recorder=undefined;recorderStatus.textContent=error.message;
          for(const editor of Object.values(roleEditors)){editor.row.hidden=true;editor.state=undefined;}}
        return;
      }
      for(const editor of Object.values(roleEditors)){editor.row.hidden=true;editor.state=undefined;}
      if(!capabilities.includes('chart-recorder-v1')){
        recorder=undefined;recorderStatus.textContent='Требуется обновление приёмника VPS.';return;
      }
      try {
        const result=await ipc.invoke('mt5-vps:chart-recorder');
        if(result.status!=='ok')throw Error(result.reason);
        renderRecorder(result.chartRecorder);
      }catch(error){recorder=undefined;recorderStatus.textContent=error.message;}
    }
    function updateTransfer() {
      const enabled=savedCopies.filter(row=>row.enabled);
      const available=accountIds.filter(id=>!enabled.some(row=>String(row.account)===id));
      transferStart.hidden=!enabled.length || !available.length;
      transferStart.disabled=busy || !ready || copiesDirty || !capabilities.includes('scaled-accounts');
      if(transfer.hidden)return;
      for(const [select,ids] of [[source,enabled.map(row=>String(row.account))],[target,available]]) {
        if([...select.options].slice(1).map(option=>option.value).join(',')===ids.join(',') && select.options.length)continue;
        const previous=select.value;
        select.replaceChildren();make('option','Выбери счёт',select).value='';
        for(const id of ids)make('option',id,select).value=id;
        select.value=ids.includes(previous)?previous:'';
      }
      const policy=enabled.find(row=>String(row.account)===source.value);
      transferSave.disabled=transferStart.disabled || !policy || !target.value;
      transferSummary.textContent=policy && target.value
        ? `${source.value} → ${target.value} · ${policy.riskPercent==null ? `лот ×${policy.multiplier}` : `риск ${policy.riskPercent}%`}. Только для новых сделок.` : '';
    }
    function controls() {
      // A background GET must never disable a focused editor. Only initial
      // loading and an explicit save lock inputs; drafts remain editable offline.
      for(const field of dialog.querySelectorAll('input,select')) {
        if(!field.closest('[data-connection-profile]'))field.disabled=busy || !ready;
      }
      recorderAccount.disabled=busy || !recorder;
      recorderSave.disabled=recorderAccount.disabled || !recorderDraft || recorderAccount.value===recorder.account;
      for(const editor of Object.values(roleEditors)) {
        editor.select.disabled=busy || !editor.state?.accounts.some(row=>row.ready);
        editor.save.disabled=editor.select.disabled || !editor.draft || editor.select.value===(editor.state.pendingAccount || editor.state.account);
      }
      saveCopies.disabled=busy || !ready || !capabilities.includes('scaled-accounts');
      primarySave.disabled=busy || !ready;
      primaryPercent.hidden=primaryMode.value!=='balance';
      exitSave.disabled=busy || !ready || !capabilities.includes('time-exit');refresh.disabled=busy || loading;
      discover.disabled=busy || loading || catalogBusy || catalogScanning || !capabilities.includes('account-enrollment');
      for(const button of discovered.querySelectorAll('button'))button.disabled=busy || loading || catalogBusy || catalogScanning;
      updateTransfer();
    }
    function reload() {
      if(readPromise)return readPromise;
      readPromise=readState().finally(()=>{readPromise=null;});return readPromise;
    }
    async function readState() {
      loading=true;controls();
      try {
        const data=await ipc.invoke('mt5-vps:trade-settings');if(data.status!=='ok')throw Error(data.reason || 'Нет связи с MT5');
        ready=true;
        account=data.account;
        capabilities=data.capabilities || [];status.textContent='';
        primaryTitle.textContent=`Основной · ${data.account}`;
        if (!primaryRiskDirty && !busy && !primaryRiskPanel.contains(document.activeElement)) {
          primaryMode.value=data.primaryRisk?.mode || 'usd';
          if (data.primaryRisk?.percent!=null) primaryPercent.value=String(data.primaryRisk.percent);
        }
        if(!capabilities.includes('account-enrollment'))catalogStatus.textContent='Для добавления счетов нужно установить обновление приёмника VPS.';
        if(!exitDirty && !busy && !exitPanel.contains(document.activeElement)){
          exitEnabled.checked=data.timeExit?.enabled ?? true;
          const next=data.timeExit?.time || '15:00';if(exitTime.value!==next)exitTime.value=next;
        }
        accountIds=(data.accounts || []).slice(1).map(String);
        renderCopies(data.followers || []);
        await readRecorder();
      } finally {loading=false;controls();}
    }
    function renderCopies(rows,force=false) {
      savedCopies=rows;
      const existing=new Map([...copiesRows.querySelectorAll('[data-copy-account]')].map(line=>[line.dataset.copyAccount,line]));
      for(const [id,line] of existing)if(!accountIds.includes(id) && !copiesDirty && !line.contains(document.activeElement) && !busy)line.remove();
      for(const id of accountIds){
        const configured=rows.find(row=>String(row.account)===id);
        let line=existing.get(id);
        if(line && !force && (copiesDirty || busy || line.contains(document.activeElement)))continue;
        if(!line){
          line=make('div','',copiesRows);line.dataset.copyAccount=id;line.className='account-controls account-row';
          const label=make('label','',line);
          const enabled=make('input','',label);enabled.type='checkbox';enabled.setAttribute('aria-label',`Копировать на счёт ${id}`);
          make('span',id,label);
          const mode=make('select','',line);mode.setAttribute('aria-label',`Расчёт объёма ${id}`);
          for(const [value,text] of [['multiplier','Лот ×'],['balance','Риск, % баланса']])make('option',text,mode).value=value;
          const factor=make('input','',line);factor.type='number';factor.min='0.001';factor.max='100';factor.step='any';factor.style.width='72px';factor.setAttribute('aria-label',`Множитель или риск в процентах ${id}`);
          enabled.onchange=factor.oninput=()=>{copiesDirty=true;transfer.hidden=true;controls();};
          mode.onchange=()=>{factor.value=mode.value==='balance' ? '0.2' : '1';factor.oninput();};
        }
        line.querySelector('input[type=checkbox]').checked=Boolean(configured?.enabled);
        line.querySelector('select').value=configured?.riskPercent==null ? 'multiplier' : 'balance';
        const factor=line.querySelector('input[type=number]'),next=String(configured?.riskPercent ?? configured?.multiplier ?? 1);
        if(factor.value!==next)factor.value=next;
      }
      let empty=copiesRows.querySelector('[data-empty-copies]');
      if(copiesRows.querySelector('[data-copy-account]'))empty?.remove();
      else if(!empty){empty=make('p','Дополнительных счетов пока нет.',copiesRows);empty.dataset.emptyCopies='true';}
    }
    async function save(channel,value,kind){
      if(busy || !ready)return;busy=true;controls();
      try{
        // Preserve the click and its captured draft if a background read is
        // pending. Save exactly once after it, with the normal server checks.
        if(readPromise)await readPromise.catch(()=>{});
        const result=await ipc.invoke(channel,value);if(result.status!=='ok')throw Error(result.reason);
        if(kind==='time')exitDirty=false;
        else if(kind==='risk'){
          primaryRiskDirty=false;
          toolbar.dispatchEvent(new CustomEvent('mt5-primary-risk-changed',{detail:result.primaryRisk,bubbles:true}));
        } else {
          copiesDirty=false;
          if(kind==='transfer')transfer.hidden=true;
          if(result.followers)renderCopies(result.followers,true);
        }
        message.textContent=kind==='transfer' ? `Копирование перенесено на ${value.to}.` : 'Сохранено для следующих сделок.';
        try{await reload();}catch(error){status.textContent=error.message;}
      }catch(error){message.textContent=error.message;}finally{busy=false;controls();}
    }
    exitEnabled.onchange=exitTime.oninput=()=>{exitDirty=true;};
    recorderAccount.onchange=()=>{recorderDraft=recorderAccount.value;controls();};
    recorderSave.onclick=async()=>{
      if(recorderSave.disabled)return;
      const selection={account:recorderAccount.value,revision:recorder.revision};
      busy=true;controls();
      try {
        if(readPromise)await readPromise.catch(()=>{});
        const result=capabilities.includes('tradehub-roles-v1')
          ? await ipc.invoke('mt5-vps:save-tradehub-role',{role:'chart',...selection})
          : await ipc.invoke('mt5-vps:save-chart-recorder',selection);
        if(result.status!=='ok')throw Error(result.reason);
        recorderDraft=undefined;
        if(result.tradeHubRoles)renderTradeHub(result.tradeHubRoles);
        else renderRecorder(result.chartRecorder);
      }catch(error){
        await readRecorder();
        message.textContent=error.message;
      }finally{busy=false;controls();}
    };
    for(const [name,editor] of Object.entries(roleEditors)) {
      editor.select.onchange=()=>{editor.draft=editor.select.value;controls();};
      editor.save.onclick=async()=>{
        if(editor.save.disabled)return;
        const selection={role:name,account:editor.select.value,revision:editor.state.revision};
        busy=true;controls();
        try {
          if(readPromise)await readPromise.catch(()=>{});
          const result=await ipc.invoke('mt5-vps:save-tradehub-role',selection);
          if(result.status!=='ok')throw Error(result.reason);
          editor.draft=undefined;renderTradeHub(result.tradeHubRoles);
        }catch(error){await readRecorder();message.textContent=error.message;}
        finally{busy=false;controls();}
      };
    }
    primaryMode.onchange=()=>{primaryRiskDirty=true;controls();};
    primaryPercent.oninput=()=>{primaryRiskDirty=true;};
    primarySave.onclick=()=>save('mt5-vps:save-primary-risk',{account,mode:primaryMode.value,percent:primaryPercent.value},'risk');
    exitSave.onclick=()=>save('mt5-vps:save-time-exit',{enabled:exitEnabled.checked,time:exitTime.value},'time');
    saveCopies.onclick=()=>save('mt5-vps:save-followers',[...copiesRows.querySelectorAll('[data-copy-account]')].map(line=>{
      const balance=line.querySelector('select').value==='balance',value=Number(line.querySelector('input[type=number]').value);
      return {account:line.dataset.copyAccount,multiplier:balance ? 1 : value,enabled:line.querySelector('input[type=checkbox]').checked,...(balance ? {riskPercent:value} : {})};
    }),'copies');
    transferStart.onclick=()=>{transfer.hidden=false;updateTransfer();source.focus();};
    transferCancel.onclick=()=>{transfer.hidden=true;transferStart.focus();};
    source.onchange=target.onchange=updateTransfer;
    transferSave.onclick=()=>{
      const policy=savedCopies.find(row=>String(row.account)===source.value);
      if(transferSave.disabled || !policy)return;
      save('mt5-vps:transfer-copying',{account,from:source.value,to:target.value,
        multiplier:policy.multiplier,riskPercent:policy.riskPercent},'transfer');
    };
    async function catalog(start=false) {
      if(!accounts || catalogBusy)return;catalogBusy=true;controls();
      try {
        const data=await ipc.invoke(start?'mt5-vps:account-discovery':'mt5-vps:account-catalog');
        if(data.status!=='ok')throw Error(data.reason || 'Не удалось прочитать счета VPS');
        catalogScanning=Boolean(data.scanning);discovered.replaceChildren();
        catalogStatus.textContent=data.error || (catalogScanning?'Проверяю запущенные терминалы…':data.scannedAt?'Проверено '+new Date(data.scannedAt).toLocaleTimeString()+' · новые счета добавляются без включения копирования.':'Нажми «Найти терминалы на VPS».');
        for(const row of data.candidates || []) {
          const line=make('div','',discovered);line.style.cssText='margin:8px 0;font-size:13px;line-height:1.4';
          const platform=row.platform==='mt4'?'MT4':'MT5';
          make('span',`${platform} · ${row.account || 'нет подключённого счёта'}${row.server?' · '+row.server:''}${row.currency?' · '+row.currency:''}`,line);
          if(row.state==='available'){
            const button=make('button',row.platform==='mt4'?'Подготовить MT4':'Добавить счёт',line);button.style.marginLeft='8px';
            button.onclick=async()=>{
              if(busy || loading || catalogBusy)return;busy=true;controls();
              try{
                const result=await ipc.invoke('mt5-vps:account-register',{candidateId:row.candidateId});
                if(result.status!=='ok')throw Error(result.reason || 'Нет подтверждения регистрации. Обнови список; копирование не включалось.');
                message.textContent=result.registration?.executionReady?'Счёт добавлен. Копирование можно включить выше.':'MT4 зарегистрирован для подготовки.';
                await reload();await catalog();
              }catch(error){message.textContent=error.message+' После потери ответа сначала обнови список.';}
              finally{busy=false;controls();}
            };
          }else make('div',row.state==='registered'?'Уже подключён':row.reason || 'Терминал недоступен',line);
        }
        for(const row of data.registered || []) {
          if(row.platform!=='mt4')continue;
          make('p',`MT4 · ${row.account} · ${row.server} · подготовлен, копирование недоступно`,discovered);
        }
        if(!catalogScanning && data.scannedAt && !(data.candidates || []).length)make('p','Запущенные терминалы не найдены.',discovered);
      }catch(error){catalogScanning=false;catalogStatus.textContent=error.message;}
      finally{catalogBusy=false;controls();}
    }
    discover.onclick=()=>catalog(true);
    enrollment.ontoggle=()=>{if(enrollment.open && capabilities.includes('account-enrollment'))catalog();};
    refresh.onclick=()=>reload().catch(error=>{status.textContent=error.message;});
    const poll=setInterval(()=>{if(dialog.open && !document.hidden && !busy && !loading){refresh.onclick();if(enrollment.open && catalogScanning)catalog();}},2000);
    window.addEventListener('beforeunload',()=>clearInterval(poll),{once:true});
    document.body.append(dialog);dialog.showModal();
    reload().catch(error=>{status.textContent=error.message;});
  };
}
module.exports={mountTradeSettings};

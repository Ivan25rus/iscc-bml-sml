function mountTradeSettings(ipc,toolbar) {
  const launch=document.createElement('button');launch.id='mt5-trade-settings-button';launch.textContent='Счета и время · MT5';toolbar.append(launch);
  let dialog;
  launch.onclick=()=>{
    if (dialog) {dialog.showModal();dialog.querySelector('#mt5-settings-refresh').click();return;}
    dialog=document.createElement('dialog');dialog.id='mt5-trade-settings';
    dialog.style.cssText='box-sizing:border-box;width:min(520px,92vw);max-height:85vh;overflow:auto;color:var(--text);background:var(--panel,#20242b);border:1px solid var(--border);border-radius:12px;padding:16px;font-size:14px';
    const make=(tag,text,parent=dialog)=>{const e=document.createElement(tag);if(text)e.textContent=text;parent.append(e);return e;};
    const title=make('h2','Счета и время · MT5');title.style.cssText='font-size:18px;margin:0 0 12px';
    const style=make('style');style.textContent='#mt5-trade-settings fieldset{border:1px solid var(--border);border-radius:8px;padding:12px}#mt5-trade-settings input:not([type=checkbox]),#mt5-trade-settings button{box-sizing:border-box;padding:6px 8px;border:1px solid var(--border);border-radius:7px;color:var(--text);background:var(--bg,#111);font-size:13px}#mt5-trade-settings button{cursor:pointer}#mt5-trade-settings button:disabled{opacity:.45;cursor:default}';
    const status=make('p','Проверяю подключение…');status.style.fontSize='12px';
    const copies=make('fieldset');make('legend','Лот к основному счёту',copies);
    const copiesRows=make('div','',copies);copiesRows.id='mt5-followers';
    const copiesNote=make('p','×1 — тот же лот, ×0,7 — 70% от основного. Для новых сделок.',copies);copiesNote.style.fontSize='12px';
    const saveCopies=make('button','Сохранить коэффициенты',copies);saveCopies.id='mt5-save-followers';
    const enrollment=make('fieldset');enrollment.style.marginTop='12px';make('legend','Добавить счёт · MT5 / MT4',enrollment);
    const discover=make('button','Найти терминалы на VPS',enrollment);discover.id='mt5-discover-accounts';
    const catalogStatus=make('p','Войди в новый счёт в отдельном терминале на VPS. Пароль остаётся в терминале.',enrollment);catalogStatus.style.fontSize='12px';
    const discovered=make('div','',enrollment);discovered.id='mt5-account-catalog';
    const mt4Note=make('p','MT4: добавь ISCC_AccountProbe на один график. Сейчас доступна подготовка подключения; торговое копирование MT4 ещё не включено.',enrollment);mt4Note.style.cssText='font-size:12px;line-height:1.4';
    const exitPanel=make('fieldset');exitPanel.id='mt5-time-exit';exitPanel.style.marginTop='12px';make('legend','Автозакрытие · новые сделки',exitPanel);
    const exitLabel=make('label','На всех счетах ',exitPanel);
    const exitEnabled=make('input','',exitLabel);exitEnabled.type='checkbox';exitEnabled.id='mt5-time-exit-enabled';
    const exitTime=make('input','',exitPanel);exitTime.type='time';exitTime.id='mt5-time-exit-clock';exitTime.value='15:00';exitTime.style.margin='0 8px';exitTime.setAttribute('aria-label','Время закрытия Нью-Йорка');make('span','Нью-Йорк',exitPanel);
    const exitNote=make('p','Новая сделка закрывается в ближайшее следующее указанное время Нью-Йорка. После 15:00 срок 15:00 выбирается на завтра. VPS закрывает остаток и отменяет лимитку даже без Mac. Ранее созданные сроки не меняются.',exitPanel);exitNote.style.cssText='font-size:12px;line-height:1.4';
    const exitSave=make('button','Сохранить время',exitPanel);exitSave.id='mt5-save-time-exit';
    const message=make('p');message.id='mt5-settings-result';message.setAttribute('role','status');
    const refresh=make('button','Обновить');refresh.id='mt5-settings-refresh';
    const dismiss=make('button','Закрыть');dismiss.style.marginLeft='8px';dismiss.onclick=()=>dialog.close();
    let busy=false,loading=false,ready=false,readPromise=null,copiesDirty=false,exitDirty=false,capabilities=[],catalogBusy=false,catalogScanning=false;
    function controls() {
      // A background GET must never disable a focused editor. Only initial
      // loading and an explicit save lock inputs; drafts remain editable offline.
      for(const field of dialog.querySelectorAll('input'))field.disabled=busy || !ready;
      saveCopies.disabled=busy || !ready || !capabilities.includes('scaled-accounts');
      exitSave.disabled=busy || !ready || !capabilities.includes('time-exit');refresh.disabled=busy || loading;
      discover.disabled=busy || loading || catalogBusy || catalogScanning || !capabilities.includes('account-enrollment');
      for(const button of discovered.querySelectorAll('button'))button.disabled=busy || loading || catalogBusy || catalogScanning;
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
        capabilities=data.capabilities || [];status.textContent=`Основной счёт ${data.accounts?.[0] || data.account} · изменения для следующих сделок`;
        if(!capabilities.includes('account-enrollment'))catalogStatus.textContent='Для добавления счетов нужно установить обновление приёмника VPS.';
        if(!exitDirty && !busy && !exitPanel.contains(document.activeElement)){
          exitEnabled.checked=data.timeExit?.enabled ?? true;
          const next=data.timeExit?.time || '15:00';if(exitTime.value!==next)exitTime.value=next;
        }
        {
          const accounts=(data.accounts || []).slice(1).map(String);
          const existing=new Map([...copiesRows.querySelectorAll('[data-copy-account]')].map(line=>[line.dataset.copyAccount,line]));
          for(const [account,line] of existing)if(!accounts.includes(account) && !copiesDirty && !line.contains(document.activeElement) && !busy)line.remove();
          for(const account of accounts){
            const configured=(data.followers || []).find(x=>String(x.account)===String(account));
            if(existing.has(account)){
              const line=existing.get(account);
              if(!copiesDirty && !busy && !line.contains(document.activeElement)){
                line.querySelector('input[type=checkbox]').checked=Boolean(configured?.enabled);
                const factor=line.querySelector('input[type=number]'),next=String(configured?.multiplier ?? 1);
                if(factor.value!==next)factor.value=next;
              }
              continue;
            }
            const line=make('label','',copiesRows);line.dataset.copyAccount=String(account);line.style.cssText='display:flex;align-items:center;gap:8px;margin:8px 0';
            const enabled=make('input','',line);enabled.type='checkbox';enabled.checked=Boolean(configured?.enabled);enabled.setAttribute('aria-label',`Копировать на счёт ${account}`);
            make('span',`Счёт ${account} · ×`,line);
            const factor=make('input','',line);factor.type='number';factor.min='0.001';factor.max='100';factor.step='any';factor.value=configured?.multiplier ?? 1;factor.style.width='80px';factor.setAttribute('aria-label',`Коэффициент лота ${account}`);
            enabled.onchange=factor.oninput=()=>{copiesDirty=true;};
          }
          let empty=copiesRows.querySelector('[data-empty-copies]');
          if(copiesRows.querySelector('[data-copy-account]'))empty?.remove();
          else if(!empty){empty=make('p','Дополнительные счета подключаются отдельными терминалами на VPS.',copiesRows);empty.dataset.emptyCopies='true';}
        }
      } finally {loading=false;controls();}
    }
    async function save(channel,value,kind){
      if(busy || !ready)return;busy=true;controls();
      try{
        // Preserve the click and its captured draft if a background read is
        // pending. Save exactly once after it, with the normal server checks.
        if(readPromise)await readPromise.catch(()=>{});
        const result=await ipc.invoke(channel,value);if(result.status!=='ok')throw Error(result.reason);
        if(kind==='time')exitDirty=false;else copiesDirty=false;
        message.textContent='Сохранено для следующих сделок.';await reload();
      }catch(error){message.textContent=error.message;}finally{busy=false;controls();}
    }
    exitEnabled.onchange=exitTime.oninput=()=>{exitDirty=true;};
    exitSave.onclick=()=>save('mt5-vps:save-time-exit',{enabled:exitEnabled.checked,time:exitTime.value},'time');
    saveCopies.onclick=()=>save('mt5-vps:save-followers',[...copiesRows.querySelectorAll('[data-copy-account]')].map(line=>({account:line.dataset.copyAccount,multiplier:Number(line.querySelector('input[type=number]').value),enabled:line.querySelector('input[type=checkbox]').checked})),'copies');
    async function catalog(start=false) {
      if(catalogBusy)return;catalogBusy=true;controls();
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
                message.textContent=result.registration?.executionReady?'Счёт добавлен. Выбери коэффициент и включи копирование выше.':'MT4 зарегистрирован для подготовки. Торговое копирование пока недоступно.';
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
    refresh.onclick=()=>reload().catch(error=>{status.textContent=error.message;});
    const poll=setInterval(()=>{if(dialog.open && !document.hidden && !busy && !loading){refresh.onclick();if(catalogScanning)catalog();}},2000);
    window.addEventListener('beforeunload',()=>clearInterval(poll),{once:true});
    document.body.append(dialog);dialog.showModal();
    reload().then(()=>{if(capabilities.includes('account-enrollment'))catalog();}).catch(error=>{status.textContent=error.message;});
  };
}
module.exports={mountTradeSettings};

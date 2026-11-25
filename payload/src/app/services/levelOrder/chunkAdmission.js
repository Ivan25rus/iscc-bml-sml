const {randomUUID}=require('crypto');
const {performance}=require('perf_hooks');
const {calculateLimitBidTradePlan,calculateChunkSizing,resolveLevelOrderDefaults}=require('./strategy');
const {validateTrackedEntry}=require('./manualEntry');
const {calculateMarketZoneTradePlan,resolveMarketChunkPercent}=require('./marketZone');
const {freshExecutionSnapshot,entrySpecification}=require('../mt5-vps/quoteSnapshot');
const {primaryRisk,capturePrimaryRisk}=require('../mt5-vps/primaryRisk');
const {CAPABILITY,PRIMARY_RISK_CAPABILITY}=require('../mt5-vps/entryAdmission');
const {GOLD_ENTRY_CAPABILITY}=require('../mt5-vps/tradingSymbols');
const {captureEntrySettings}=require('../mt5-vps/chunkEntryClient');
const {keepsLtLevel,openIndexSymbols,hasEntryPosition,isOpenPosition,freshPositionAt}=require('../mt5-vps/cardIdentity');
const {indexSymbol}=require('../mt5-vps/approachPreparation');

async function verifyPositionPresence(adapter, readAccountOrders, accounts, symbol, preparation) {
  const started=performance.now(), at=Date.now();
  const uncertain='Состояние позиции MT5 уточняется. Повтори вход после сверки.';
  let states;
  const matches=(row,account)=>row && String(row.account)===account && row.server===adapter.config?.server;
  try {
    if(adapter.capabilities?.includes('position-presence-v1') && accounts.length) {
      preparation.positionVerificationMode='symbol';
      const query=new URLSearchParams({symbol,accounts:accounts.join(',')});
      const reply=await adapter.request('/position-presence?'+query);
      if(!matches(reply,String(adapter.config?.account)) || reply.symbol!==symbol)throw Error(uncertain);
      states=accounts.map(account=>{
        const row=reply.positionStates?.[account];
        return matches(row,account) && row.symbol===symbol && freshPositionAt(row.checkedAt)
          && typeof row.hasPosition==='boolean' ? row : null;
      });
    } else {
      preparation.positionVerificationMode='account-orders';
      const reads=await Promise.allSettled(accounts.map(async account=>{
        const view=await readAccountOrders(account);
        if(!matches(view,account) || !Array.isArray(view.orders))throw Error(uncertain);
        return {checkedAt:at,hasPosition:view.orders.some(order=>isOpenPosition(order,symbol))};
      }));
      states=reads.map(read=>read.status==='fulfilled' ? read.value : null);
    }
  } catch {
    throw Error(uncertain);
  } finally {
    preparation.positionVerificationMs=Number((performance.now()-started).toFixed(2));
  }
  if(!freshPositionAt(at))throw Error(uncertain);
  const open=states.filter(row=>row?.hasPosition);
  if(open.length)return Math.max(...open.map(row=>row.checkedAt));
  if(!states.length || states.includes(null))throw Error(uncertain);
  return 0;
}

// Explicit arming only. Quotes, startup recovery and polling never call arm.
function createChunkAdmission({getAdapter,readAccountOrders,getGroups,getConfig,getSnapshot,orderCalculator,validateTrade,getQuantityLimit,log=()=>{}}) {
  const inflight=new Map();
  async function arm(payload={}) {
    const id=String(payload.requestId || randomUUID());
    if (inflight.has(id)) return inflight.get(id);
    const task=(async()=>{
      // Elapsed stage times include I/O and scheduling. admissionMs is their
      // containing interval, not another delay to add to the stage values.
      const started=performance.now(),preparation={};
      let planStarted;
      const elapsed=since=>Number((performance.now()-since).toFixed(2));
      const dispatch=Date.now()-payload.clickAt;
      if(Number.isFinite(payload.clickAt) && dispatch>=0 && dispatch<=60000)preparation.ipcDispatchMs=dispatch;
      let ticker='';
      let positionEvidence;
      try {
        const action=String(payload.action || '').toUpperCase();
        const entryMode=payload.entryMode ?? 'breakout';
        const zone=entryMode==='side-zone';
        ticker=String(payload.ticker || '').trim().toUpperCase();
        if (!['breakout','side-zone'].includes(entryMode) || !(zone ? ['MB','MS'] : ['BS','SS']).includes(action)
          || payload.provider && payload.provider!=='mt5-vps' || !/^[A-Za-z0-9_.:-]{1,100}$/.test(id))
          throw Error('Условный вход требует MT5 VPS, выбранного режима и явной цены входа');
        const trackedStarted=performance.now();
        const tracked=validateTrackedEntry({groups:getGroups(),symbol:ticker,level:payload.level,
          entryPrice:zone ? payload.level : payload.entryPrice});
        preparation.trackedValidationMs=elapsed(trackedStarted);
        if (!tracked.ok) throw Error(tracked.reason);
        const adapter=getAdapter();
        if(payload.separateLt!==undefined && (payload.separateLt!==true || !zone))
          throw Error('Отдельный уровень ЛТ требует БМЛ/СМЛ');
        const known=[...(adapter.chunkLifecycleEntries?.values() || [])].filter(entry=>entry.symbol===adapter.brokerSymbol?.(ticker) && keepsLtLevel(entry));
        if(payload.separateLt===true && known.some(entry=>Math.abs(entry.level-tracked.level)<1e-8))
          throw Error('Этот уровень уже закреплён за сделкой. Выбери другой ЛТ.');
        if(!payload.separateLt) {
          const brokerSymbol=symbol=>adapter.brokerSymbol(symbol);
          const symbol=indexSymbol(ticker);
          const evidence=openIndexSymbols(adapter.managementStateView,brokerSymbol,adapter.managementStateAt,
            [adapter.config?.account,...(adapter.remoteAccounts || [])]);
          let openAt=evidence.positionTimes[symbol];
          // Historical fills can trigger a read but cannot certify a live
          // position. A normal management reply may omit a busy account.
          if(!openAt && (known.some(entry=>hasEntryPosition(entry,evidence)) || evidence.uncertainSymbols.includes(symbol))) {
            openAt=await verifyPositionPresence(adapter,readAccountOrders,evidence.accounts,brokerSymbol(symbol),preparation);
          }
          if(openAt) {
            positionEvidence={symbol,checkedAt:openAt};
            throw Error('Позиция открыта. Для нового входа нажми «Другой ЛТ».');
          }
        }
        const entrySettings=captureEntrySettings(adapter,ticker);
        const primaryAccount=entrySettings.account;
        const policy=primaryRisk(adapter.config);
        const instrumentType=payload.instrumentType || 'FX';
        const nativeRisk=zone && policy.mode==='balance' && instrumentType==='FX' && typeof getQuantityLimit==='function'
          && [CAPABILITY,PRIMARY_RISK_CAPABILITY,'independent-account-engines-v5','entry-initial-sizing-only-v1']
            .every(value=>adapter.capabilities?.includes(value));
        const config=getConfig();
        const defaults=resolveLevelOrderDefaults(config,ticker);
        const chunkPercent=zone ? resolveMarketChunkPercent(config,ticker) : Number(payload.chunkPercent ?? defaults.chunkPercent);
        const maxGap=zone ? undefined : Number(defaults.chunkMaxGap ?? 2);
        const quantityLimit=nativeRisk ? getQuantityLimit({instrumentType}) : undefined;
        // Gold uses the order size in each account's MT5 specification. A zero
        // quantity limit still disables entry, and an explicit Max lot still applies.
        const useBrokerGoldVolume=nativeRisk && ticker==='XAUUSD' && adapter.capabilities.includes(GOLD_ENTRY_CAPABILITY)
          && (quantityLimit==null || quantityLimit>0);
        const {riskUsd,balanceRisk}=policy.mode==='balance' && !nativeRisk
          ? await capturePrimaryRisk(adapter,payload.riskUsd) : {riskUsd:payload.riskUsd};
        const snapshotStarted=performance.now();let snapshot;
        try {snapshot=await getSnapshot(ticker,payload);}
        finally {preparation.snapshotReadMs=elapsed(snapshotStarted);}
        if(zone) {
          if(!entrySpecification(snapshot,'mt5-vps',ticker))throw Error('Нет подтверждённой спецификации инструмента MT5');
        } else if (!freshExecutionSnapshot(snapshot,'mt5-vps',ticker)) throw Error('Котировка MT5 устарела. Дождись свежих Bid/Ask');
        planStarted=performance.now();
        if (!zone && (!Number.isFinite(maxGap) || maxGap<0))
          throw Error('В настройках Level Orders укажи допустимое отклонение цены');
        const args={action,ticker,instrumentType,
          level:tracked.level,entryPrice:tracked.entryPrice,riskUsd,
          stopOffsetPts:payload.stopOffsetPts,stopMode:payload.stopMode,maxLot:zone ? payload.maxLot ?? 0 : 0,
          minLot:snapshot.metadata?.quantityStep,takeProfitPts:payload.takeProfitPts,
          bid:snapshot.quote?.bid,ask:snapshot.quote?.ask,tickSize:snapshot.metadata?.tickSize,
          lot:snapshot.metadata?.contractSize,orderCalculator};
        const plan=zone ? calculateMarketZoneTradePlan({...args,entryMode,maxGap,chunkPercent,maxOffset:tracked.maxOffset,
          zoneMultiplier:payload.zoneMultiplier ?? 1,minQty:snapshot.metadata?.minQty ?? snapshot.metadata?.quantityStep,
          maxQty:snapshot.metadata?.maxQty,previewWithoutSize:nativeRisk}) : calculateLimitBidTradePlan(args);
        if (!plan.ok) throw Error(plan.reason);
        const sizing=zone ? plan : calculateChunkSizing({totalQty:plan.totalQty,chunkPercent,
          quantityStep:snapshot.metadata?.quantityStep,minQty:snapshot.metadata?.minQty ?? snapshot.metadata?.quantityStep});
        if (!sizing.ok) throw Error(sizing.reason);
        const chunkVolume=zone ? plan.chunkVolume : sizing.chunkVolume;
        const rule=validateTrade({symbol:ticker,side:plan.orderSide,type:zone ? 'market' : 'stop',price:zone ? plan.worstPrice : plan.referencePrice,
          qty:nativeRisk ? 0 : plan.totalQty,sl:plan.stopPts,tp:plan.takeProfitPts,tickSize:plan.tickSize,
          instrumentType},snapshot.quote,{deferMarketPrice:zone});
        if (!rule.ok) throw Error(rule.reason);
        const entryPlan={...plan,chunkPercent,chunkVolume,maxGap:zone ? plan.maxGap : maxGap,
          ...(payload.separateLt===true ? {separateLt:true} : {}),
          ...(zone && Number(args.maxLot)>0 ? {maxLot:Number(args.maxLot)} : {}),
          ...(nativeRisk ? {primaryRiskRequest:{account:primaryAccount,percent:policy.percent,
            quantityStep:Number(args.minLot),minQty:Number(snapshot.metadata.minQty ?? args.minLot),
            maxQty:Number(snapshot.metadata.maxQty),pointValue:Number(args.lot),maxLot:Number(args.maxLot),
            ...(!useBrokerGoldVolume ? {quantityLimit} : {})}} : {}),
          ...(balanceRisk ? {balanceRisk} : {}),
          ...(plan.executionPolicy==='hybrid-market-limit-v1' ? {fillPriority:defaults.fillPriority} : {})};
        preparation.planValidationMs=elapsed(planStarted);
        preparation.admissionMs=elapsed(started);
        const reply=await adapter.armChunkEntry(entryPlan,{requestId:id,clickAt:payload.clickAt,preparation,entrySettings});
        const result={...reply,timings:{...preparation,...reply?.timings}};
        const confirmedSizing=nativeRisk && reply?.entry?.primaryRiskSizing;
        const loggedPlan=confirmedSizing ? {...entryPlan,riskUsd:confirmedSizing.riskUsd,
          totalQty:confirmedSizing.volume,chunkVolume:confirmedSizing.chunkVolume,
          balanceRisk:Object.fromEntries(['account','balance','credit','riskBase','percent','capturedAt']
            .filter(key=>confirmedSizing[key]!==undefined).map(key=>[key,confirmedSizing[key]]))} : entryPlan;
        log({kind:'chunk-entry-arm',reqId:id,plan:loggedPlan,result});
        return result;
      } catch(error) {
        if(planStarted!==undefined && preparation.planValidationMs===undefined)preparation.planValidationMs=elapsed(planStarted);
        const result={status:'rejected',reason:error.message,...(positionEvidence ? {positionEvidence} : {}),
          timings:{...preparation,admissionMs:elapsed(started)}};
        log({kind:'chunk-entry-admission',reqId:id,symbol:ticker,result});return result;
      }
    })();
    inflight.set(id,task);
    try {return await task;} finally {inflight.delete(id);}
  }
  return {arm,get busy(){return inflight.size>0;}};
}

module.exports={createChunkAdmission};

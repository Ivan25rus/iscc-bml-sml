const {randomUUID}=require('crypto');
const {performance}=require('perf_hooks');
const {calculateLimitBidTradePlan,calculateChunkSizing,resolveLevelOrderDefaults}=require('./strategy');
const {validateTrackedEntry}=require('./manualEntry');
const {calculateMarketZoneTradePlan,resolveMarketChunkPercent}=require('./marketZone');
const {freshExecutionSnapshot,entrySpecification}=require('../mt5-vps/quoteSnapshot');

// Explicit arming only. Quotes, startup recovery and polling never call arm.
function createChunkAdmission({getAdapter,getGroups,getConfig,getSnapshot,orderCalculator,validateTrade,log=()=>{}}) {
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
      try {
        const action=String(payload.action || '').toUpperCase();
        const entryMode=payload.entryMode ?? 'breakout';
        const zone=entryMode==='side-zone';
        const ticker=String(payload.ticker || '').trim().toUpperCase();
        if (!['breakout','side-zone'].includes(entryMode) || !(zone ? ['MB','MS'] : ['BS','SS']).includes(action)
          || payload.provider && payload.provider!=='mt5-vps' || !/^[A-Za-z0-9_.:-]{1,100}$/.test(id))
          throw Error('Условный вход требует MT5 VPS, выбранного режима и явной цены входа');
        const trackedStarted=performance.now();
        const tracked=validateTrackedEntry({groups:getGroups(),symbol:ticker,level:payload.level,
          entryPrice:zone ? payload.level : payload.entryPrice});
        preparation.trackedValidationMs=elapsed(trackedStarted);
        if (!tracked.ok) throw Error(tracked.reason);
        const snapshotStarted=performance.now();let snapshot;
        try {snapshot=await getSnapshot(ticker,payload);}
        finally {preparation.snapshotReadMs=elapsed(snapshotStarted);}
        if(zone) {
          if(!entrySpecification(snapshot,'mt5-vps',ticker))throw Error('Нет подтверждённой спецификации инструмента MT5');
        } else if (!freshExecutionSnapshot(snapshot,'mt5-vps',ticker)) throw Error('Котировка MT5 устарела. Дождись свежих Bid/Ask');
        planStarted=performance.now();
        const config=getConfig();
        const defaults=resolveLevelOrderDefaults(config,ticker);
        const chunkPercent=zone ? resolveMarketChunkPercent(config,ticker) : Number(payload.chunkPercent ?? defaults.chunkPercent);
        const maxGap=zone ? undefined : Number(defaults.chunkMaxGap ?? 2);
        if (!zone && (!Number.isFinite(maxGap) || maxGap<0))
          throw Error('В настройках Level Orders укажи допустимое отклонение цены');
        const args={action,ticker,instrumentType:payload.instrumentType || 'FX',
          level:tracked.level,entryPrice:tracked.entryPrice,riskUsd:payload.riskUsd,
          stopOffsetPts:payload.stopOffsetPts,stopMode:payload.stopMode,maxLot:zone ? payload.maxLot ?? 0 : 0,
          minLot:snapshot.metadata?.quantityStep,takeProfitPts:payload.takeProfitPts,
          bid:snapshot.quote?.bid,ask:snapshot.quote?.ask,tickSize:snapshot.metadata?.tickSize,
          lot:snapshot.metadata?.contractSize,orderCalculator};
        const plan=zone ? calculateMarketZoneTradePlan({...args,entryMode,maxGap,chunkPercent,maxOffset:tracked.maxOffset,
          zoneMultiplier:payload.zoneMultiplier ?? 1,minQty:snapshot.metadata?.minQty ?? snapshot.metadata?.quantityStep,
          maxQty:snapshot.metadata?.maxQty}) : calculateLimitBidTradePlan(args);
        if (!plan.ok) throw Error(plan.reason);
        const sizing=zone ? plan : calculateChunkSizing({totalQty:plan.totalQty,chunkPercent,
          quantityStep:snapshot.metadata?.quantityStep,minQty:snapshot.metadata?.minQty ?? snapshot.metadata?.quantityStep});
        if (!sizing.ok) throw Error(sizing.reason);
        const chunkVolume=zone ? plan.chunkVolume : sizing.chunkVolume;
        const rule=validateTrade({symbol:ticker,side:plan.orderSide,type:zone ? 'market' : 'stop',price:zone ? plan.worstPrice : plan.referencePrice,
          qty:plan.totalQty,sl:plan.stopPts,tp:plan.takeProfitPts,tickSize:plan.tickSize,
          instrumentType:payload.instrumentType || 'FX'},snapshot.quote,{deferMarketPrice:zone});
        if (!rule.ok) throw Error(rule.reason);
        const entryPlan={...plan,chunkPercent,chunkVolume,maxGap:zone ? plan.maxGap : maxGap,
          ...(plan.executionPolicy==='hybrid-market-limit-v1' ? {fillPriority:defaults.fillPriority} : {})};
        preparation.planValidationMs=elapsed(planStarted);
        preparation.admissionMs=elapsed(started);
        const reply=await getAdapter().armChunkEntry(entryPlan,{requestId:id,clickAt:payload.clickAt,preparation});
        const result={...reply,timings:{...preparation,...reply?.timings}};
        log({kind:'chunk-entry-arm',reqId:id,plan:entryPlan,result});
        return result;
      } catch(error) {
        if(planStarted!==undefined && preparation.planValidationMs===undefined)preparation.planValidationMs=elapsed(planStarted);
        const result={status:'rejected',reason:error.message,timings:{...preparation,admissionMs:elapsed(started)}};
        log({kind:'chunk-entry-admission',reqId:id,result});return result;
      }
    })();
    inflight.set(id,task);
    try {return await task;} finally {inflight.delete(id);}
  }
  return {arm,get busy(){return inflight.size>0;}};
}

module.exports={createChunkAdmission};

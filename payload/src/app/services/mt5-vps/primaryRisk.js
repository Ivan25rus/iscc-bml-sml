const {checkEntryRisk}=require('./entryRisk');

function normalizePrimaryRisk(value) {
  if (value == null || value.mode==='usd') return {mode:'usd'};
  const percent=Number(value.percent);
  if (value.mode!=='balance' || !Number.isFinite(percent) || percent<=0 || percent>100)
    throw Error('Укажи риск основного счёта больше 0 и не больше 100% баланса');
  return {mode:'balance',percent};
}

function primaryRisk(config={}) {
  return normalizePrimaryRisk(config.primaryRiskByAccount?.[String(config.account)]);
}

async function capturePrimaryRisk(adapter, cardRisk) {
  const config=adapter.config || {};
  const policy=primaryRisk(config);
  if (policy.mode==='usd') return {riskUsd:cardRisk};
  const account=String(config.account), server=config.server;
  // Health reads native funding directly. Floating P&L is not part of the risk base.
  const started=Date.now();
  const health=await adapter.request('/health',undefined,{readTimeoutMs:2500});
  const capturedAt=Date.now();
  const info=health?.accountInfo;
  const balance=info?.balance;
  const includeCredit=info?.riskIncludesCredit===true;
  const riskBase=includeCredit ? info.riskBase : balance;
  if (capturedAt-started>2500 || capturedAt<started || health?.connected===false
      || health?.tradingEnabled!==true || String(health?.account)!==account || health?.server!==server
      || !Number.isFinite(balance) || !Number.isFinite(riskBase) || riskBase<=0
      || (includeCredit && (!Number.isFinite(info.credit) || info.credit<0 ||
        Math.abs(riskBase-(balance+info.credit))>1e-7)))
    throw Error(includeCredit ? 'Не удалось подтвердить свежий баланс с кредитом основного счёта MT5'
      : 'Не удалось подтвердить свежий баланс основного счёта MT5');
  const current=primaryRisk(adapter.config);
  if (String(adapter.config.account)!==account || adapter.config.server!==server
      || current.mode!==policy.mode || current.percent!==policy.percent)
    throw Error('Настройки основного счёта изменились. Запусти новый вход заново');
  const riskUsd=Math.floor(riskBase*policy.percent)/100;
  if (!Number.isFinite(riskUsd) || riskUsd<=0) throw Error('Расчётный риск меньше одного цента');
  checkEntryRisk(config,riskUsd);
  return {riskUsd,balanceRisk:{account,balance,percent:policy.percent,capturedAt,
    ...(includeCredit ? {credit:info.credit,riskBase} : {})}};
}

module.exports={normalizePrimaryRisk,primaryRisk,capturePrimaryRisk};

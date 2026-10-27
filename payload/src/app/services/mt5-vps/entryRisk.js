function entryRiskLimit(config={}) {
  const value=Number(config.entryRiskLimitUsd ?? 0);
  if(!Number.isFinite(value) || value<0)throw Error('Дополнительный предел риска должен быть неотрицательным');
  return value;
}
function checkEntryRisk(config,risk) {
  const limit=entryRiskLimit(config);
  if(limit>0 && risk>limit+1e-8)throw Error(`Риск нового набора $${risk} выше дополнительного предела $${limit}. Открытые сделки сохраняются`);
  return limit;
}
module.exports={entryRiskLimit,checkEntryRisk};

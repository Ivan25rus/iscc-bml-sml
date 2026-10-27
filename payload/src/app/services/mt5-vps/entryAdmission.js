// The cached capability selects a route only. The VPS validates this complete
// contract, real accounts and the captured plan before saving an opening intent.
const CAPABILITY='atomic-entry-admission-v1';
function requiredCapabilities(request) {
  const required=['chunked-breakout'];
  if(request.fillPriority!==undefined)required.push('entry-fill-priority-v1');
  if(['zone','boundary','side-zone'].includes(request.entryMode)) {
    required.push('market-zone-entry');
    if(request.stopMode==='entry')required.push('market-zone-entry-stop');
  }
  if(request.entryMode==='boundary')required.push('market-offset-entry');
  if(request.entryMode==='side-zone')required.push('market-side-zone-entry');
  if(request.executionPolicy==='market-limit')required.push('market-limit-side-zone-entry');
  if(request.executionPolicy==='hybrid-market-limit-v1')required.push('hybrid-market-limit-v1');
  if(request.accountExecution==='independent-v2')required.push('independent-account-entry-v2');
  if(request.accountExecution==='independent-v3')required.push('parallel-account-entry-v3');
  if(request.accountExecution==='independent-v4')required.push('independent-account-lanes-v4');
  if(request.accountExecution==='independent-v5')required.push('independent-account-engines-v5','entry-initial-sizing-only-v1');
  return required;
}
function validateHealth(health,operation,request) {
  if(!health || health.connected===false || operation==='arm' && health.tradingEnabled===false
    || (operation==='arm' ? requiredCapabilities(request) : ['chunked-breakout']).some(value=>!health.capabilities?.includes(value)))
    throw Error('Обновите приёмник VPS для выбранного набора и проверьте подключение');
}
module.exports={CAPABILITY,requiredCapabilities,validateHealth};

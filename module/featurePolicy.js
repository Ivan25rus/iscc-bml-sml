'use strict';
const {resolve}=require('../selection');

function createFeaturePolicy(features){
  const selection=resolve(features),chosen=new Set(selection.features);
  const has=id=>chosen.has(id);
  // Only newly installed addon routes use this policy. Existing host routes,
  // active plans, stop/cancel/reconciliation and saved rules stay available.
  function allow(channel,payload={}){
    if(channel==='mt5-vps:manage'){
      if(['cancel-close','cancel-time-exit'].includes(payload.action))return has('management');
      if(payload.action==='schedule-close')return has('price-close');
      if(payload.action==='close')return has('management') && (has('partial-close') ||
        payload.percent===undefined && Number(payload.volume)>0 &&
        Number(payload.volume)===Number(payload.target?.lots));
      return has('management');
    }
    if(['mt5-vps:save-followers','mt5-vps:account-discovery','mt5-vps:account-catalog','mt5-vps:account-register'].includes(channel))return has('accounts');
    if(channel==='mt5-vps:save-time-exit')return has('time-exit');
    return true;
  }
  return {features:selection.features,has,allow};
}
module.exports={createFeaturePolicy};

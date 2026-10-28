'use strict';
const {createFeaturePolicy}=require('./featurePolicy');

function mount({ipc, toolbar, features}) {
  if (!ipc || typeof ipc.invoke !== 'function' || !toolbar)
    throw Error('Provide the host IPC and toolbar');
  const policy=createFeaturePolicy(features);
  const selectedIPC={invoke:(channel,...args)=>policy.allow(channel,args[0]) ? ipc.invoke(channel,...args)
    : Promise.resolve({status:'rejected',reason:'Функция не выбрана при установке'})};
  if(policy.has('management'))require('../payload/src/app/services/mt5-vps/management')
    .mountManagement(selectedIPC,toolbar,{partialClose:policy.has('partial-close'),priceClose:policy.has('price-close')});
  if(policy.has('accounts') || policy.has('time-exit'))require('../payload/src/app/services/mt5-vps/tradeSettings')
    .mountTradeSettings(selectedIPC,toolbar,{accounts:policy.has('accounts'),timeExit:policy.has('time-exit')});
}
module.exports = {mount};

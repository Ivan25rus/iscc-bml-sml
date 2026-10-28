'use strict';
const {createFeaturePolicy}=require('./featurePolicy');

// The host supplies its existing adapter, saved LT groups, risk validator and
// fresh snapshots. Merely loading the addon never connects or arms a plan.
function createBmlSml({enabled = () => false, features=['bml-sml'], ...host}) {
  const policy=createFeaturePolicy(features);
  let admission;
  return {
    async arm(payload = {}) {
      if (!policy.has('bml-sml') || !enabled()) return {status:'rejected', reason:'BML/SML выключен'};
      if (!['MB','MS'].includes(payload.action) || payload.entryMode !== 'side-zone'
          || payload.zoneMultiplier !== undefined && payload.zoneMultiplier !== 1)
        return {status:'rejected', reason:'Новый BML/SML использует уровень LT и обычный люфт'};
      admission ||= require('../payload/src/app/services/levelOrder/chunkAdmission').createChunkAdmission(host);
      return admission.arm({...payload, zoneMultiplier:1});
    },
    list: () => host.getAdapter().listChunkEntries(),
    stop: entryId => host.getAdapter().stopChunkEntry(entryId),
    continue: (entryId,account,options) => host.getAdapter().continueChunkEntry(entryId,account,options),
    modifyProtection: (target,changes,options) => host.getAdapter().modifyEntryProtection(target,changes,options),
    closeEntry: (entry,options) => host.getAdapter().closeEntryPosition(entry,options)
  };
}
module.exports = {createBmlSml,
  calculateMarketZoneTradePlan:(...args)=>require('../payload/src/app/services/levelOrder/marketZone').calculateMarketZoneTradePlan(...args),
  resolveMarketChunkPercent:(...args)=>require('../payload/src/app/services/levelOrder/marketZone').resolveMarketChunkPercent(...args),
  createChunkAdmission:(...args)=>require('../payload/src/app/services/levelOrder/chunkAdmission').createChunkAdmission(...args)};
Object.defineProperty(module.exports,'MT5VPSAdapter',{enumerable:true,
  get:()=>require('../payload/src/app/services/mt5-vps/client').MT5VPSAdapter});

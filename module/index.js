'use strict';
const planner = require('../payload/src/app/services/levelOrder/marketZone');
const {createChunkAdmission} = require('../payload/src/app/services/levelOrder/chunkAdmission');
const {MT5VPSAdapter} = require('../payload/src/app/services/mt5-vps/client');

// The host supplies its existing adapter, saved LT groups, risk validator and
// fresh snapshots. Merely loading the addon never connects or arms a plan.
function createBmlSml({enabled = () => false, ...host}) {
  const admission = createChunkAdmission(host);
  return {
    async arm(payload = {}) {
      if (!enabled()) return {status:'rejected', reason:'BML/SML выключен'};
      if (!['MB','MS'].includes(payload.action) || payload.entryMode !== 'side-zone'
          || payload.zoneMultiplier !== undefined && payload.zoneMultiplier !== 1)
        return {status:'rejected', reason:'Новый BML/SML использует уровень LT и обычный люфт'};
      return admission.arm({...payload, zoneMultiplier:1});
    },
    list: () => host.getAdapter().listChunkEntries(),
    stop: entryId => host.getAdapter().stopChunkEntry(entryId),
    continue: (entryId,account,options) => host.getAdapter().continueChunkEntry(entryId,account,options),
    modifyProtection: (target,changes,options) => host.getAdapter().modifyEntryProtection(target,changes,options),
    closeEntry: (entry,options) => host.getAdapter().closeEntryPosition(entry,options)
  };
}
module.exports = {...planner, createChunkAdmission, createBmlSml, MT5VPSAdapter};

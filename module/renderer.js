'use strict';
const {mountManagement} = require('../payload/src/app/services/mt5-vps/management');
const {mountTradeSettings} = require('../payload/src/app/services/mt5-vps/tradeSettings');

function mount({ipc, toolbar}) {
  if (!ipc || typeof ipc.invoke !== 'function' || !toolbar)
    throw Error('Provide the host IPC and toolbar');
  mountManagement(ipc, toolbar);
  mountTradeSettings(ipc, toolbar);
}
module.exports = {mount};

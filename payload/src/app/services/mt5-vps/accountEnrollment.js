// Enrollment never enables copying or calls a trading endpoint. A lost reply
// is resolved by refreshing the server catalog, without an automatic POST retry.
function createAccountEnrollment({getAdapter,isTradingBusy}) {
  let registering=false;
  async function run(action,payload) {
    try {
      const adapter=getAdapter();
      if (!adapter.config.enabled) throw Error('Подключение VPS выключено');
      if (action==='catalog') return {status:'ok',...await adapter.request('/account-catalog')};
      if (registering || isTradingBusy() || adapter.inFlight.size || adapter.connectionRestarting || adapter.accountEnrollmentBusy) throw Error('Дождись завершения текущей команды');
      if (action==='discover') return {status:'ok',...await adapter.request('/account-discovery',{}, {enrollment:true})};
      if (action!=='register' || !payload || Object.keys(payload).length!==1 || !/^[a-f0-9]{64}$/.test(payload.candidateId)) throw Error('Выбери найденный терминал');
      registering=true;
      // Prevent a new opening/reconnect while the account list is changing.
      adapter.accountEnrollmentBusy=true;
      try {
        const response=await adapter.request('/account-register',{candidateId:payload.candidateId},{enrollment:true});
        return {status:'ok',...response};
      } finally {registering=false;adapter.accountEnrollmentBusy=false;}
    } catch(error) {return {status:'rejected',reason:error.message};}
  }
  return {run,get busy(){return registering;}};
}
module.exports={createAccountEnrollment};

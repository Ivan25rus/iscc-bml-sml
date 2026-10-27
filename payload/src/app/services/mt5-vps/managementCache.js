// Exact watch replies belong to their caller. This cache is only the shared
// display/target view, and must never move an account back to an older read.
class ManagementCache {
  constructor() { this.value=null;this.generation=0;this.accounts=new Map(); }
  accept(account,view,generation) {
    const old=this.accounts.get(account),epoch=view?.managementEpoch || null;
    if(old) {
      if(epoch!==old.epoch) {
        if(old.retired.has(epoch) || generation<old.generation || old.epoch && !epoch)return old.view;
      } else if(Number.isFinite(view?.checkedAt) && Number.isFinite(old.view?.checkedAt)) {
        if(view.checkedAt<old.view.checkedAt)return old.view;
      } else if(generation<old.generation)return old.view;
    }
    const retired=new Set(old?.retired);
    if(old?.epoch && old.epoch!==epoch)retired.add(old.epoch);
    while(retired.size>8)retired.delete(retired.values().next().value);
    this.accounts.set(account,{view,epoch,generation:Math.max(generation,old?.generation || 0),retired});
    return view;
  }
  commit(value,generation) {
    const states=value.accountStates || {[String(value.account)]:value};
    const selected={};
    for(const [account,view] of Object.entries(states))selected[account]=this.accept(account,view,generation);
    const primary=String(value.account);
    const primaryAccepted=selected[primary]===states[primary];
    const base=!this.value || primaryAccepted && generation>=this.generation ? value : this.value;
    const accounts={...(this.value?.accountStates || {}),...selected};
    const own=accounts[String(value.account)] || value;
    const result={...base};
    for(const key of ['orders','checkedAt','snapshotAgeMs','connected','evidenceReady','connectionState',
      'rules','timeRules','openingStates','managementEpoch']) {
      if(Object.hasOwn(own,key))result[key]=own[key];else delete result[key];
    }
    if(value.accountStates)result.accountStates=accounts;
    this.value=result;this.generation=Math.max(generation,this.generation);
    return result;
  }
}
module.exports={ManagementCache};

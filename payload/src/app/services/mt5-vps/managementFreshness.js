function managementFreshness(state,receivedAt,now=Date.now()) {
  const ageView=view=>{
    if(!view || !Number.isFinite(view.checkedAt) && !Number.isFinite(view.snapshotAgeMs))return view;
    const age=Number.isFinite(view.checkedAt) ? now-view.checkedAt : view.snapshotAgeMs+Math.max(0,now-receivedAt);
    const fresh=age>=-250 && age<=2500;
    return {...view,snapshotAgeMs:Math.max(0,age),connected:view.connected!==false && fresh,
      ...(fresh ? {} : {reason:'Состояние счёта уточняется',openingStates:{}})};
  };
  const states=state.accountStates && Object.fromEntries(Object.entries(state.accountStates).map(([id,view])=>[id,ageView(view)]));
  const primary=ageView(state);
  if(!states)return primary;
  const own=states[String(state.account)];
  return {...primary,accountStates:states,...(own ? {connected:own.connected,snapshotAgeMs:own.snapshotAgeMs} : {})};
}
module.exports={managementFreshness};

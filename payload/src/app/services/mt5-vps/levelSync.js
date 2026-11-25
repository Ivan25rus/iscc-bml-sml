// Publish the desktop's existing LT configuration. No quote, broker or order
// reads are introduced; the normal adapter still owns TLS and authentication.
const {randomUUID,createHash}=require('crypto');
const {performance}=require('node:perf_hooks');
const {isTradingSymbol,normalizeTradingSymbol,GOLD_ENTRY_CAPABILITY}=require('./tradingSymbols');
const CAPABILITY='lt-level-sync-v1';
const SCHEMA='iscc-lt-snapshot-v1';
const LEASE_MS=60000;
const HEARTBEAT_MS=15000;
const MAX_BYTES=65536;

function publicationTime(adapter,wall,at=performance.now()){
  // Reuse the server clock learned from the normal authenticated quote stream.
  // This converts the publication timestamp only; no quote becomes fresher.
  const anchor=adapter.entryIntentClock,age=at-anchor?.at;
  return anchor&&Number.isFinite(anchor.capturedAt)&&Number.isFinite(age)&&age>=0&&age<=60000?anchor.capturedAt+age:wall;
}

function projectGroups(snapshot={}, {includeGold = true} = {}) {
  const groups=[];let count=0;
  for(const input of snapshot.config?.groups || []) {
    const symbol=normalizeTradingSymbol(input.ticker);
    if(!isTradingSymbol(symbol) || !includeGold && symbol==='XAUUSD')continue;
    const key=String(input.key || '');
    if(!key || key.length>128)throw Error('Invalid LT group key');
    const levels=[...new Set((input.levels || []).filter(value=>Number.isFinite(value)&&value>0))];
    const validOffset=Number.isFinite(input.maxOffset)&&input.maxOffset>=0;
    count+=levels.length;
    groups.push({key,symbol,source:input.source==='tv-mt5-candles'?'tv-mt5-candles':'manual',
      enabled:input.enabled!==false&&validOffset,maxOffset:validOffset?input.maxOffset:0,levels});
  }
  if(groups.length>32 || count>2048 || new Set(groups.map(group=>group.key)).size!==groups.length)
    throw Error('LT snapshot exceeds limits or repeats a group key');
  return groups;
}

function createLevelSync({levelTrack,getAdapter,getMobileProfile,isBusy=()=>false,clock=Date.now,
  sessionId=randomUUID(),setTimer=setInterval,clearTimer=clearInterval,onState=()=>{}}={}) {
  if(typeof levelTrack?.snapshot!=='function')throw Error('LT source required');
  let closed=false,pending=false,sequence=0,lastSentAt=0,lastFingerprint='',retryAt=0;
  let lastAdapter,mobileSupported=false,mobileProfile,mobileDirty=true,confirmedProfileId=null,profileRetryAt=0;
  let state={status:'waiting',reason:'Ожидаем поддержку синхронизации LT на VPS'};
  function publish(next){state={...state,...next};try{onState({...state});}catch{}}
  async function flush(){
    if(closed || pending || clock()<retryAt)return;
    let adapter;
    try{adapter=getAdapter();}catch{return;}
    if(!adapter?.config?.enabled || adapter.closed)return;
    // Capabilities are learned by existing status reads. Never add a health
    // round trip or borrow execution preparation just for this publication.
    if(!adapter.capabilities?.includes(CAPABILITY))return;
    try{if(isBusy() || adapter.inFlight?.size)return;}catch{return;}
    if(adapter!==lastAdapter){
      lastAdapter=adapter;lastFingerprint='';mobileSupported=false;mobileDirty=true;confirmedProfileId=null;profileRetryAt=0;
    }
    let groups,fingerprint;
    try{groups=projectGroups(levelTrack.snapshot(),{includeGold:adapter.capabilities.includes(GOLD_ENTRY_CAPABILITY)});fingerprint=createHash('sha256').update(JSON.stringify(groups)).digest('hex');}
    catch(error){publish({status:'error',reason:error.message});retryAt=clock()+HEARTBEAT_MS;return;}
    if(mobileSupported && mobileDirty && getMobileProfile){
      mobileDirty=false;
      try{mobileProfile=getMobileProfile(adapter.config);}
      catch{mobileProfile=undefined;}
    }
    const profileToSend=mobileSupported && clock()>=profileRetryAt && mobileProfile?.profileId!==confirmedProfileId ? mobileProfile : undefined;
    if(!profileToSend && fingerprint===lastFingerprint && clock()-lastSentAt<HEARTBEAT_MS)return;
    const localSentAt=clock();
    const payload={schema:SCHEMA,sessionId,sequence:++sequence,sentAt:publicationTime(adapter,localSentAt),leaseMs:LEASE_MS,groups};
    if(profileToSend)payload.mobileProfile=profileToSend;
    if(Buffer.byteLength(JSON.stringify(payload))>MAX_BYTES){publish({status:'error',reason:'LT snapshot is too large'});retryAt=clock()+HEARTBEAT_MS;return;}
    pending=true;
    try{
      const reply=await adapter.request('/lt-levels',payload);
      if(closed)return;
      const receipt=reply?.ltReceipt;
      if(receipt?.schema!==SCHEMA || receipt.sessionId!==sessionId || receipt.sequence!==payload.sequence)
        throw Error('VPS did not confirm the exact LT snapshot');
      mobileSupported=receipt.mobileProfileSync===true;
      confirmedProfileId=mobileSupported ? receipt.mobileProfileId ?? null : null;
      lastFingerprint=fingerprint;lastSentAt=localSentAt;retryAt=0;
      const profileFailed=profileToSend && (receipt.mobileProfileRejected || confirmedProfileId!==profileToSend.profileId);
      if(profileFailed)profileRetryAt=clock()+HEARTBEAT_MS;
      publish({status:'synced',reason:'LT переданы на VPS',sequence,at:clock(),groups:groups.length,levels:groups.reduce((n,g)=>n+g.levels.length,0),
        mobileProfileError:profileFailed ? 'VPS не подтвердил настройки мобильной ИСКК' : null});
    }catch(error){
      if(!closed){retryAt=clock()+5000;publish({status:'error',reason:'Синхронизация LT: '+error.message});}
    }finally{pending=false;}
  }
  // Read the existing local list once a second, without subscribing to quote
  // events or forcing hidden desktop windows to construct state notifications.
  const timer=setTimer(()=>{void flush();},1000);timer.unref?.();
  void flush();
  return {flush,invalidateMobileProfile(){mobileDirty=true;profileRetryAt=0;},snapshot:()=>({...state,confirmedProfileId}),
    stop(){if(closed)return;closed=true;clearTimer(timer);}};
}

module.exports={CAPABILITY,SCHEMA,LEASE_MS,HEARTBEAT_MS,publicationTime,projectGroups,createLevelSync};

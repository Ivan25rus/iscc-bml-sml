const https = require('https');
const {decodeReadGzip,MAX_REPLY_BYTES}=require('./readCompression');
const {managementFreshness}=require('./managementFreshness');
const {resolveSecureConfig,tlsOptions,connectionURL} = require('./secureAuth');
const { randomUUID } = require('crypto');
const { EventEmitter } = require('events');
const { performance } = require('perf_hooks');
const {followers,percentVolume}=require('./sizing');
const {ENTRY_PROTECTION_CAPABILITY,isEntryPlanRow,entryPlanMetadata,entryProtectionBlock}=require('./managementPolicy');
const { ExecutionAdapter } = require('../brokerage/comps/base');
const {EXACT_CLOSE_CAPABILITY,accountIds,controlEntry}=require('./entryControlState');

const INDEX_SYMBOLS = new Set(['SPX', 'SPX500', 'USTEC']);
function isIndex(symbol) { return INDEX_SYMBOLS.has(String(symbol || '').toUpperCase().split(':').pop()); }

function entryCloseContext(state,entry,symbol,config) {
  const parent=state.entryPlans?.find(row=>row.id===entry.id);
  if(!parent || parent.executionPolicy!=='hybrid-market-limit-v1' || parent.symbol!==symbol
    || !parent.accounts || Array.isArray(parent.accounts) || !Object.keys(parent.accounts).length
    || parent.positionsConfirmed!==true || parent.hasOpenPositions!==true || parent.phase==='unknown'
    || parent.protectionPending || !Array.isArray(parent.children) || parent.children.some(child=>['sending','unknown'].includes(child.state)))throw Error('Нет подтверждённых данных позиций всего набора БМЛ/СМЛ. Обнови список.');
  const accounts=Object.keys(parent.accounts);
  if(accounts.length>32 || accounts.some(value=>!/^[1-9][0-9]{0,19}$/.test(value)))throw Error('Некорректные счета набора БМЛ/СМЛ.');
  const primary=state.accountStates?.[String(config.account)] || state;
  if(!primary.connected || !primary.capabilities?.includes(ENTRY_PROTECTION_CAPABILITY))throw Error('Обнови и подключи основной приёмник VPS для управления БМЛ/СМЛ.');
  for(const account of accounts) {
    const view=state.accountStates?.[account];
    if(!view?.connected || view.evidenceReady===false || String(view.account)!==account || view.server!==config.server || !view.capabilities?.includes(ENTRY_PROTECTION_CAPABILITY))throw Error('Нет подтверждённой связи с каждым счётом набора БМЛ/СМЛ.');
  }
  return {parent,accounts};
}

class MT5VPSAdapter extends ExecutionAdapter {
  constructor(config = {}) {
    super();
    this.config = resolveSecureConfig(config);
    this.provider = 'mt5-vps';
    this.commandJournal = null;
    this.inFlight = new Set();
    this.knownOrders = new Map();
    this.eventBus = new EventEmitter();
    this.lastStatus = { connected: false, message: 'MT5 VPS: не проверено' };
    this.snapshots = new Map();
    this.snapshotInflight = new Map();
    this.candleSnapshots = new Map(); this.candleSnapshotInflight = new Map();
    this.closed = false;
    this.streamRequest = null;
    this.streamTimer = null;
    this.streamFailures = 0;
    this.streamDiagnostics = {attempts:0,frames:0,discardedFrames:0,disconnects:0,lastFrameAt:null,maxGapMs:0};
    this.quoteContinuity=require('./quoteContinuity').createQuoteContinuity();
    this.agents = null;
    this.warming = null;
    this.quoteFast = false;
    this.capabilities = [];
    this.capabilitiesAt = 0;
    this.managementSnapshot = null;
    this.managementLeaseUntil = 0;
    this.managementHasPositions = false;
    this.quoteModePending = null;
  }
  on(...args) { return this.eventBus.on(...args); }
  emitLifecycle(name, value) {
    if (this.eventBus.listenerCount(name)) this.eventBus.emit(name, value);
    else this.events?.emit(name, value);
  }
  brokerSymbol(symbol) {
    const name = String(symbol || '').toUpperCase().split(':').pop();
    if (!INDEX_SYMBOLS.has(name)) throw Error('MT5 VPS принимает только SPX и USTEC');
    return name === 'USTEC' ? this.config.ustecSymbol : this.config.spxSymbol;
  }
  candleSymbol(symbol) { return require('../tvListener/candle-mappings').brokerName(symbol); }
  async validateCandleSymbols(symbols) {
    if (!Array.isArray(symbols) || symbols.length > 32) throw Error('Допускается до 32 инструментов переноса');
    const names = [...new Set(symbols.map(symbol => this.candleSymbol(symbol)))];
    if (!this.capabilities.includes('candle-levels-mappings')) await this.health();
    if (!this.capabilities.includes('candle-levels-mappings')) throw Error('Обновите приёмник на VPS для произвольных пар ТВ → MT5');
    const reply = await this.request('/candle-symbols?names=' + encodeURIComponent(JSON.stringify(names)));
    if (!Array.isArray(reply.symbols) || names.some(name => !reply.symbols.some(row => row.name === name)))
      throw Error('MT5 не подтвердил точные имена инструментов');
    return reply.symbols;
  }
  async getCandleSnapshot(symbol) {
    const name = this.candleSymbol(symbol);
    // Reuse the existing stream for its two instruments, without changing it.
    if (name === this.config.ustecSymbol || name === this.config.spxSymbol) {
      const data = await this.snapshot(name === this.config.ustecSymbol ? 'USTEC' : 'SPX500');
      const cached = this.snapshots.get(name);
      return { ...data, symbol: name, provider: 'mt5-vps', quoteUpdatedAt: cached?.data === data ? cached.at : Date.now() };
    }
    const cached = this.candleSnapshots.get(name);
    if (cached && Date.now() - cached.quoteUpdatedAt < 250) return cached;
    if (!this.candleSnapshotInflight.has(name)) {
      if (this.candleSnapshotInflight.size >= 32) throw Error('Слишком много одновременных чтений свечных инструментов');
      const task = (async () => {
        if (!this.capabilities.includes('candle-levels-mappings')) await this.health();
        if (!this.capabilities.includes('candle-levels-mappings')) throw Error('Нужно обновить чтение инструментов на VPS');
        const data = await this.request('/candle-snapshot?symbol=' + encodeURIComponent(name));
        if (data.symbol !== name || !Number.isFinite(data.capturedAt) || Math.abs(Date.now() - data.capturedAt) > 1500
          || !Number.isFinite(data.quote?.bid) || data.quote.bid <= 0 || !Number.isFinite(data.quote?.ask) || data.quote.ask < data.quote.bid)
          throw Error('Нет свежей котировки выбранного инструмента MT5');
        const result = { ...data, provider: 'mt5-vps', quoteUpdatedAt: Date.now() };
        this.candleSnapshots.delete(name); this.candleSnapshots.set(name, result);
        while (this.candleSnapshots.size > 32) this.candleSnapshots.delete(this.candleSnapshots.keys().next().value);
        return result;
      })().finally(() => this.candleSnapshotInflight.delete(name));
      this.candleSnapshotInflight.set(name, task);
    }
    return this.candleSnapshotInflight.get(name);
  }
  request(route, payload, {execution = false, enrollment = false, readTimeoutMs} = {}) {
    if(readTimeoutMs!==undefined && (payload || !Number.isInteger(readTimeoutMs) || readTimeoutMs<2500 || readTimeoutMs>15000 || !['/health','/management','/diagnostics','/entry-events'].includes(route.split('?')[0])))
      return Promise.reject(Error('Extended diagnostic timeout is restricted to read-only health/management/diagnostics'));
    const cfg = this.config;
    if (!cfg.enabled || !cfg.endpoint || !cfg.token || !cfg.caFile || !cfg.account || !cfg.server) return Promise.reject(Error('Подключение MT5 VPS не настроено'));
    let url;
    try { url = connectionURL(cfg); } catch (error) { return Promise.reject(error); }
    if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash) return Promise.reject(Error('MT5 VPS требует HTTPS'));
    const [pathname, query = ''] = route.split('?');
    url.pathname = (cfg.routePrefix || '') + pathname;
    url.search = query;
    const tls = tlsOptions(cfg);
    if (!this.agents) this.agents = {
      commands: new https.Agent({keepAlive:true, maxSockets:4, maxFreeSockets:2, ...tls}),
      execution: new https.Agent({keepAlive:true, maxSockets:2, maxFreeSockets:2, scheduling:'fifo', ...tls}),
      quotes: new https.Agent({keepAlive:true, maxSockets:1, maxFreeSockets:0, ...tls})
    };
    const body = payload ? Buffer.from(JSON.stringify(payload)) : null;
    let agent=execution ? this.agents.execution : this.agents.commands;
    const entryControl=execution && body && (['/entry-plan-stop','/entry-plan-continue'].includes(pathname)
      || ['/entry-plan','/entry-plan-admit'].includes(pathname) && payload.entryMode==='side-zone');
    if(entryControl && Object.values(agent.sockets || {}).reduce((count,rows)=>count+rows.length,0)>=agent.maxSockets) {
      // Keep the prepared sockets on the ordinary path. If both are occupied,
      // a paused health read must not queue a new BML/SML or its cancellation.
      const pool=pathname==='/entry-plan-stop' ? 'entryStop' : 'entryControl';
      this.agents[pool] ||= new https.Agent({keepAlive:true,maxSockets:2,maxFreeSockets:2,scheduling:'fifo',...tls});
      agent=this.agents[pool];
    }
    const started = Date.now(), startedPerf = performance.now();
    const timing = {startedAt:started};
    return new Promise((resolve, reject) => {
      let deadline,readTimer,request,readFinished=false;
      const traceRead=!body && ['/health','/management','/snapshot','/orders','/diagnostics'].includes(pathname);
      const readDetail=()=>({account:String(cfg.account),route:pathname,startedAt:started,elapsedMs:Number((performance.now()-startedPerf).toFixed(2))});
      const finishRead=ok=>{
        if(readFinished)return;readFinished=true;clearTimeout(deadline);clearTimeout(readTimer);
        if(traceRead && (!ok || performance.now()-startedPerf>=750))this.recordQuoteStream('read-complete',ok ? 'slow-read' : 'failed-read',{...readDetail(),ok});
      };
      const fail=error=>{
        if(readFinished)return;
        finishRead(false);
        this.lastStatus={connected:false,message:error.message,at:Date.now()};
        // A broken reply settles once. The caller retains any uncertain trade
        // result; neither transport cleanup nor status polling resends it.
        if(request && !request.destroyed)request.destroy(error);
        reject(error);
      };
      request = https.request(url, {
        method: body ? 'POST' : 'GET', ...tls, agent,
        headers: { authorization: `Bearer ${cfg.token}`, ...(body ? { 'content-type': 'application/json', 'content-length': body.length } : {'accept-encoding':'gzip'}) }
      }, response => {
        response.on('error',fail);
        response.on('aborted',()=>fail(Error('MT5 VPS: ответ соединения прерван')));
        response.on('close',()=>{if(!response.complete)fail(Error('MT5 VPS: ответ соединения прерван'));});
        const encoding=(response.headers?.['content-encoding'] || 'identity').toLowerCase();
        if(encoding!=='identity' && (body || encoding!=='gzip')){response.resume?.();fail(Error('Неподдерживаемое сжатие ответа MT5'));return;}
        // Full bounded entry journals can exceed 1 MiB. Only reads receive the
        // larger budget; command replies keep their existing 1 MiB limit.
        const maxReplyBytes=body ? 1024*1024 : MAX_REPLY_BYTES;
        let data = '', size = 0;const chunks=[];
        response.on('data', chunk => { if(readFinished)return;size += chunk.length; if (size > maxReplyBytes) fail(Error('Слишком большой ответ MT5')); else if(encoding==='gzip')chunks.push(Buffer.from(chunk));else data += chunk; });
        const complete = data => {
          if(readFinished)return;
          try {
            const result = JSON.parse(data);
            if (response.statusCode !== 200) throw Error(result.error || `MT5 VPS: ${response.statusCode}`);
            if (Number(result.account) !== Number(cfg.account) || result.server !== cfg.server) throw Error('MT5 VPS: другой счёт или сервер');
            if (Array.isArray(result.capabilities)) { this.capabilities = result.capabilities; this.capabilitiesAt = Date.now(); }
            this.lastStatus = { connected: true, message: `MT5 VPS · ${result.account} · ${Date.now()-started} мс${result.tradingEnabled === false ? ' · только чтение' : ''}`, at:Date.now() };
            timing.totalMs = Number((performance.now()-startedPerf).toFixed(2));
            Object.defineProperty(result,'transportTiming',{value:timing,enumerable:false});
            finishRead(true);
            resolve(result);
          } catch (error) { fail(error); }
        };
        response.on('end', () => {
          if(readFinished)return;
          if(encoding==='gzip')decodeReadGzip(Buffer.concat(chunks)).then(complete,fail);
          else complete(data);
        });
      });
      request.on('socket',socket => {
        timing.socketWaitMs = Number((performance.now()-startedPerf).toFixed(2));
        timing.reusedSocket = request.reusedSocket;
        if (!request.reusedSocket) socket.once('secureConnect',() => { timing.tlsReadyMs = Number((performance.now()-startedPerf).toFixed(2)); });
      });
      request.on('finish',() => { timing.sentAt = Date.now(); timing.writeMs = Number((performance.now()-startedPerf).toFixed(2)); });
      // Waiting longer for an execution reply never renews or resends its command.
      const enrollmentWait=enrollment && ['/account-discovery','/account-register'].includes(pathname) && body;
      deadline = setTimeout(() => fail(Error('MT5 VPS: время ожидания истекло')),enrollmentWait ? 15000 : execution && body ? 5000 : readTimeoutMs ?? 2500);
      if(traceRead){readTimer=setTimeout(()=>this.recordQuoteStream('read-pending','slow-read',readDetail()),750);readTimer.unref?.();}
      request.on('error', fail);
      if (body) request.write(body);
      request.end();
    });
  }
  acceptQuoteFrame(frame, {discardStale = false} = {}) {
    const cfg = this.config;
    if (Number(frame.account) !== Number(cfg.account) || frame.server !== cfg.server) throw Error('MT5 VPS: другой счёт или сервер');
    const allowed = new Set([cfg.spxSymbol,cfg.ustecSymbol]);
    const entries = Object.entries(frame.snapshots || {});
    if (!entries.length) throw Error('MT5 VPS: пустой поток котировок');
    require('./entryIntentClock').rememberServerClock(this,frame.capturedAt);
    for (const [symbol, data] of entries) {
      if (!allowed.has(symbol) || Number(data.account) !== Number(cfg.account) || data.server !== cfg.server
          || !Number.isFinite(data.quote?.bid) || data.quote.bid <= 0 || !Number.isFinite(data.quote?.ask) || data.quote.ask < data.quote.bid) {
        throw Error('MT5 VPS: некорректная котировка');
      }
    }
    const at = Date.now();
    // A queued old frame is never a usable quote. Keep reading the same
    // authenticated stream so a fresh frame behind it can restore prices.
    if (!Number.isFinite(frame.capturedAt) || Math.abs(at-frame.capturedAt) > 1500) {
      if (discardStale && Number.isFinite(frame.capturedAt) && at-frame.capturedAt > 1500) return false;
      throw Error('MT5 VPS: поток котировок устарел');
    }
    for (const [symbol, data] of entries) {
      this.snapshots.set(symbol,{data,at});
      this.eventBus.emit('quote:updated',{symbol,quote:data.quote,metadata:data.metadata,at});
    }
    return true;
  }
  recordQuoteStream(event, reason, timing) {
    const row={at:new Date().toISOString(),event,reason,...this.streamDiagnostics,...(timing ? {timing} : {})};
    this.eventBus.emit('quote:stream-status',row);
    if (!this.quoteLogFile) return;
    return require('./diagnosticLog').diagnosticLog.append(this.quoteLogFile,row);
  }
  ensureQuoteStream() {
    const cfg = this.config;
    if (this.closed || this.streamRequest || this.streamTimer || !cfg.enabled || !cfg.endpoint || !cfg.token || !cfg.caFile) return;
    let request, freshnessTimer, backlogTimer, loopTimer, reason='connection-closed', accepted=false, discarding=false;
    const fail=(code,error)=>{if (!request?.destroyed) {reason=code;request?.destroy(error);}};
    const watchFreshness=()=>{
      clearTimeout(freshnessTimer);
      freshnessTimer=setTimeout(()=>fail(accepted ? 'no-fresh-frames' : 'connection-or-first-frame-timeout',Error('Quote stream freshness timeout')),5000);
      freshnessTimer.unref?.();
    };
    try {
      const url = connectionURL(cfg);
      if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash) return;
      url.pathname = '/quotes';
      const tls = tlsOptions(cfg);
      if (!this.agents) this.agents = {
        commands: new https.Agent({keepAlive:true,maxSockets:4,maxFreeSockets:2,...tls}),
        execution: new https.Agent({keepAlive:true,maxSockets:2,maxFreeSockets:2,scheduling:'fifo',...tls}),
        quotes: new https.Agent({keepAlive:true,maxSockets:1,maxFreeSockets:0,...tls})
      };
      this.streamDiagnostics.attempts++;
      let lastLoop=performance.now();
      loopTimer=setInterval(()=>{
        const now=performance.now(),delay=now-lastLoop-250;lastLoop=now;
        if(delay>=500)this.recordQuoteStream('local-delay','mac-event-loop',{delayMs:Number(delay.toFixed(2)),continuity:this.quoteContinuity.summary()});
      },250);
      loopTimer.unref?.();
      request = https.request(url,{method:'GET',agent:this.agents.quotes,...tls,
        headers:{authorization:`Bearer ${cfg.token}`}}, response => {
        if (response.statusCode !== 200) { response.resume(); fail('http-'+response.statusCode,Error('Quote stream unavailable')); return; }
        let buffer = '';
        response.setEncoding('utf8');
        response.on('data',chunk => {
          try {
            buffer += chunk;
            if (buffer.length > 131072) throw Error('Quote stream frame too large');
            let boundary;
            while ((boundary=buffer.indexOf('\n')) >= 0) {
              const line = buffer.slice(0,boundary); buffer = buffer.slice(boundary+1);
              if (line) {
                const frame=JSON.parse(line);
                const usable=this.acceptQuoteFrame(frame,{discardStale:true});
                const timing=this.quoteContinuity.observe(frame.capturedAt,Date.now(),performance.now(),usable,this.streamDiagnostics.attempts);
                this.eventBus.emit('quote:timing',timing);
                if (timing.arrivalGapMs>=750 || timing.captureGapMs>=750) this.recordQuoteStream('frame-gap','capture-vs-arrival',timing);
                // Authenticated expired frames prove this stream is still alive.
                // Let its queue drain instead of adding another TLS handshake.
                // They never refresh a quote or renew any trading deadline.
                watchFreshness();
                if (!usable) {
                  if (!backlogTimer) {
                    backlogTimer=setTimeout(()=>fail('stale-backlog-timeout',Error('Quote backlog did not recover')),15000);
                    backlogTimer.unref?.();
                  }
                  this.streamDiagnostics.discardedFrames++;
                  if (!discarding) this.recordQuoteStream('delayed','expired-frame',timing);
                  discarding=true;
                  continue;
                }
                clearTimeout(backlogTimer);backlogTimer=null;
                const now=Date.now(),previous=this.streamDiagnostics.lastFrameAt;
                const gap=previous===null ? 0 : Math.max(0,now-previous);
                this.streamDiagnostics.maxGapMs=Math.max(this.streamDiagnostics.maxGapMs,gap);
                this.streamDiagnostics.lastFrameAt=now;
                this.streamDiagnostics.frames++;
                this.streamFailures=0;
                if (!accepted || discarding || gap>=1000) this.recordQuoteStream('receiving',discarding ? 'backlog-recovered' : !accepted ? 'first-frame' : 'gap-recovered',timing);
                accepted=true;discarding=false;
              }
            }
          } catch (error) { fail('invalid-frame',error); }
        });
        response.on('error',error => fail('response-error',error));
        response.on('end',() => fail('response-ended'));
        response.on('close',() => {if (!request.destroyed) fail('response-closed');});
      });
      this.streamRequest = request;
      watchFreshness();
      request.on('error',() => {if(reason==='connection-closed')reason='transport-error';});
      request.on('close',() => {
        clearTimeout(freshnessTimer);
        clearTimeout(backlogTimer);
        clearInterval(loopTimer);
        if (this.streamRequest !== request) return;
        this.streamRequest = null;
        const hadQuotes = this.snapshots.size > 0;
        this.snapshots.clear();
        if (hadQuotes) this.eventBus.emit('quote:stale');
        if (!this.closed) {
          this.streamDiagnostics.disconnects++;
          this.recordQuoteStream('disconnected',reason);
          const retryMs=Math.min(5000,250*2**Math.min(this.streamFailures++,5));
          this.streamTimer = setTimeout(() => { this.streamTimer=null; this.ensureQuoteStream(); },retryMs);
          this.streamTimer.unref?.();
        }
      });
      request.end();
    } catch { clearTimeout(freshnessTimer);clearTimeout(backlogTimer);clearInterval(loopTimer); request?.destroy(); }
  }
  close() {
    this.closed = true;
    clearTimeout(this.streamTimer);
    this.streamTimer = null;
    this.streamRequest?.destroy();
    this.agents?.commands.destroy(); this.agents?.execution?.destroy(); this.agents?.entryControl?.destroy(); this.agents?.entryStop?.destroy(); this.agents?.quotes.destroy();
    this.linkedWarmConnection?.close();
    this.snapshots.clear();
  }
  async health() {
    const result = await this.request('/health');
    if (Array.isArray(result.capabilities)) { this.capabilities = result.capabilities; this.capabilitiesAt = Date.now(); }
    if (!this.config.routePrefix && result.capabilities?.includes('quote-stream')) this.ensureQuoteStream();
    return result;
  }
  validateManagementState(state, endpointState=state) {
    if(state?.evidenceReady===false)throw Error('Проверяем состояние сделки');
    if (!state?.connected || Number(state.account) !== Number(this.config.account) || state.server !== this.config.server
      || !Array.isArray(state.orders) || !Array.isArray(state.capabilities)) throw Error('Некорректный снимок счёта MT5');
    // A nested account view describes its native worker. Entry-engine routing
    // belongs to the authenticated endpoint, whose coordinator can support a
    // newer protocol than either native worker. Never replace it with a child.
    if(Array.isArray(endpointState?.capabilities)
      && (Number(endpointState.account)!==Number(this.config.account) || endpointState.server!==this.config.server))
      throw Error('Некорректный источник возможностей MT5');
  }
  acceptManagementState(state, endpointState=state) {
    this.validateManagementState(state,endpointState);
    const old=this.managementNativeState;
    if(old && old.managementEpoch===state.managementEpoch && Number.isFinite(old.checkedAt)
      && Number.isFinite(state.checkedAt) && state.checkedAt<old.checkedAt)return false;
    this.managementNativeState={checkedAt:state.checkedAt,managementEpoch:state.managementEpoch};
    this.managementSnapshot = {at:Date.now(),rows:JSON.parse(JSON.stringify(state.orders))};
    this.managementHasPositions = state.orders.some(row=>row.__isPosition);
    if(Array.isArray(endpointState?.capabilities)) {
      this.capabilities = [...endpointState.capabilities]; this.capabilitiesAt = Date.now();
    }
    return true;
  }
  cacheManagementResponse(raw,generation) {
    const state=managementFreshness(raw,Date.now());
    this.validateManagementState(state.accountStates?.[String(this.config.account)] || state,state);
    this.managementCache ||= new (require('./managementCache').ManagementCache)();
    const selected=this.managementCache.commit(raw,generation);
    const current=managementFreshness(selected,Date.now());
    this.acceptManagementState(current.accountStates?.[String(this.config.account)] || current,current);
    this.managementStateView=selected;this.managementStateAt=Date.now();
    return state; // Preserve the exact query's own evidence for its caller.
  }
  async findManagementTarget(target, position) {
    let cached = this.managementSnapshot;
    const age = cached ? Date.now()-cached.at : Infinity;
    if (age < 0 || age > 5000) {
      await this.managementState({maxAgeMs:0});
      cached=this.managementSnapshot;
    }
    const symbol = this.brokerSymbol(target.symbol);
    const row = cached.rows.find(row=>String(row.ticket)===String(target.ticket) && row.symbol===symbol && row.__isPosition===position);
    if (!row) throw Error('Позиция или заявка уже отсутствует. Обнови список.');
    return {...row}; // Only the server snapshot supplies routing and current volume.
  }
  async maintainExecutionWarmth() {
    if (this.closed || (!this.managementHasPositions && Date.now()>=this.managementLeaseUntil)) return;
    if (this.executionWarmAt && Date.now()-this.executionWarmAt<10000) return;
    if (this.executionWarmAttemptAt && Date.now()-this.executionWarmAttemptAt<2000) return;
    this.executionWarmAttemptAt = Date.now();
    await this.warmExecutionConnection();
  }
  async managementState({maxAgeMs=5000,watch}={}) {
    // Ticket histories are requested only for the captured opening group.
    // They must never be satisfied by an ordinary cached management snapshot.
    if (watch?.length) {
      const generation=this.managementRequestGeneration=(this.managementRequestGeneration || 0)+1;
      const raw=await this.request('/management?watch='+encodeURIComponent(JSON.stringify(watch)));
      return this.cacheManagementResponse(raw,generation);
    }
    const age=Date.now()-Number(this.managementStateAt || 0);
    if (this.managementStateView && age>=0 && age<=maxAgeMs) return managementFreshness(this.managementStateView,this.managementStateAt);
    if (!this.managementReading) this.managementReading=(async()=>{
      const generation=this.managementRequestGeneration=(this.managementRequestGeneration || 0)+1;
      const raw=await this.request('/management');
      return this.cacheManagementResponse(raw,generation);
    })().finally(()=>{this.managementReading=null;});
    return this.managementReading;
  }
  async maintainManagementSnapshot() {
    const age=Date.now()-Number(this.managementSnapshot?.at || 0);
    const interested=this.managementHasPositions || Date.now()<this.managementLeaseUntil;
    if (this.closed || (age>=0 && age<(interested ? 2000 : 10000))) return null;
    return this.managementState({maxAgeMs:0});
  }
  async managementOrders() {
    const age=Date.now()-Number(this.managementSnapshot?.at || 0);
    if (!this.managementSnapshot || age<0 || age>5000) await this.managementState({maxAgeMs:0});
    return JSON.parse(JSON.stringify(this.managementSnapshot.rows));
  }
  async warmExecutionConnection() {
    if (this.closed) return;
    if (this.warming) return this.warming;
    // Prime two sockets before allowing the first click. Later heartbeats use
    // FIFO rotation: one socket is checked while the other remains available.
    const hasPair = this.executionPairPrimed && Date.now()-this.executionWarmAt < 20000;
    this.warming = (async () => {
      const results = await Promise.all(Array.from({length:hasPair ? 1 : 2},
        () => this.request('/health',undefined,{execution:true})));
      if (this.closed) return;
      if (this.config.mirrorAccount) {
        const cfg = this.config;
        // Check the secondary connection without starting another quote stream.
        this.linkedWarmConnection ||= new MT5VPSAdapter({...cfg,account:cfg.mirrorAccount,routePrefix:`/accounts/${cfg.mirrorAccount}`,mirrorAccount:null});
        await this.linkedWarmConnection.request('/health');
      }
      if (this.quoteFast) await this.syncQuoteMode();
      this.executionPairPrimed = true;
      this.executionWarmAt = Date.now();
      return results[0];
    })().finally(() => { this.warming=null; });
    return this.warming;
  }
  setQuoteFast(enabled) {
    const next = enabled === true;
    if (this.quoteFast === next) return;
    this.quoteFast = next;
    this.syncQuoteMode().catch(() => {});
  }
  async syncQuoteMode() {
    if (this.closed || !this.capabilities.includes('adaptive-quotes')) return;
    if (this.quoteModePending) return this.quoteModePending;
    // A separate status socket changes cadence; order sockets stay available.
    // If proximity changes during a request, send the latest mode afterwards.
    this.quoteModePending = (async () => {
      let sentFast;
      do {
        sentFast = this.quoteFast;
        await this.request('/quote-mode',{intervalMs:sentFast ? 50 : 100});
      } while (!this.closed && sentFast !== this.quoteFast);
    })().finally(() => { this.quoteModePending=null; });
    return this.quoteModePending;
  }
  async snapshot(symbol) {
    const brokerSymbol = this.brokerSymbol(symbol);
    this.ensureQuoteStream();
    const cached = this.snapshots.get(brokerSymbol);
    if (cached && Date.now()-cached.at < 500) return cached.data;
    if (!this.snapshotInflight.has(brokerSymbol)) {
      const task = this.request(`/snapshot?symbol=${encodeURIComponent(brokerSymbol)}`)
        .finally(() => this.snapshotInflight.delete(brokerSymbol));
      this.snapshotInflight.set(brokerSymbol,task);
    }
    return this.snapshotInflight.get(brokerSymbol);
  }
  async getQuote(symbol) {
    const data = await this.snapshot(symbol);
    const cached = this.snapshots.get(this.brokerSymbol(symbol));
    return {...data.quote, receivedAt:cached?.data === data ? cached.at : Date.now()};
  }
  async getInstrumentMetadata(symbol) { return (await this.snapshot(symbol)).metadata; }
  async armChunkEntry(plan,options={}) { return require('./chunkEntryClient').armChunkEntry(this,plan,options); }
  async stopChunkEntry(entryId,options) { return require('./chunkEntryClient').stopChunkEntry(this,entryId,options); }
  async continueChunkEntry(entryId,account,options) { return require('./chunkEntryClient').continueChunkEntry(this,entryId,account,options); }
  async listChunkEntries() { return require('./chunkEntryClient').listChunkEntries(this); }
  async placeOrder(order) {
    if(this.connectionRestarting || this.accountEnrollmentBusy) return {status:'rejected',provider:this.provider,reason:'Подключение обновляется. Дождись готовности счетов'};
    let submitted = false;
    try {
      const symbol = this.brokerSymbol(order.symbol);
      const tick = Number(order.tickSize), entry = Number(order.price), qty = Number(order.qty);
      const sl = Number(order.sl) * tick, tp = Number(order.tp) * tick;
      if (!['buy','sell'].includes(order.side) || !['market','limit','stop'].includes(order.type) || ![tick,entry,qty,sl].every(v => Number.isFinite(v) && v > 0)) return { status:'rejected',provider:this.provider,reason:'Неверные параметры MT5' };
      const direction = order.side === 'buy' ? 1 : -1;
      const copies=followers(this.config).filter(row=>row.enabled);
      const request = {
        id: order.meta?.requestId || randomUUID(), symbol, side:order.side, type:order.type,
        volume:qty, price:entry, sl:entry-direction*sl,
        tp: Number.isFinite(tp) && tp > 0 ? entry+direction*tp : 0,
        cid:order.meta?.cid || '', expiresAt:Date.now()+(copies.length ? 3000 : 2000)
      };
      if (copies.length) {
        if (this.capabilities.includes('scaled-accounts')) request.mirrorAccounts=copies.map(({account,multiplier})=>({account,multiplier}));
        else if (copies.length===1 && copies[0].multiplier===1) request.mirrorAccount=copies[0].account;
        else throw Error('Обновите приёмник VPS для коэффициентов лота');
      }
      const deadline=require('./timeExit').deadlineForNewTrade(this.config);
      if (deadline) {
        if (!this.capabilities.includes('time-exit')) throw Error('Для закрытия по времени обновите приёмник VPS');
        Object.assign(request,deadline);
      }
      submitted = true;
      const macBeforeSendMs = Number.isFinite(order.meta?.clickAt) ? Math.max(0,Date.now()-order.meta.clickAt) : undefined;
      const response = await this.request('/place', request, {execution:true});
      const { result } = response;
      if (!result || !['ok','rejected','unknown'].includes(result.status)) throw Error('MT5 VPS: некорректное подтверждение');
      return { ...result, provider:this.provider, timings:{...result.timings,server:response.serverTiming || result.timings?.server,
        transport:response.transportTiming,macBeforeSendMs,
        macClickToResultMs:Number.isFinite(order.meta?.clickAt) ? Math.max(0,Date.now()-order.meta.clickAt) : undefined} };
    } catch (error) { return { status:submitted ? 'unknown' : 'rejected',provider:this.provider,reason:error.message }; }
  }
  async listOpenOrders(symbol) {
    const orders = (await this.request('/orders')).orders;
    if (!Array.isArray(orders)) throw Error('MT5 VPS: некорректный список позиций');
    this.managementHasPositions = orders.some(row=>row.__isPosition);
    const current = new Map(orders.map(row => [String(row.ticket), row]));
    for (const row of orders) {
      const previous = this.knownOrders.get(String(row.ticket));
      if (row.__isPosition && (!previous?.__isPosition || previous.lots !== row.lots || previous.sl !== row.sl || previous.tp !== row.tp)) {
        this.emitLifecycle('position:opened', {provider:this.provider,ticket:row.ticket,order:row});
      }
    }
    for (const [ticket, row] of this.knownOrders) {
      if (!current.has(ticket)) this.emitLifecycle(row.__isPosition ? 'position:closed' : 'order:cancelled', {provider:this.provider,ticket});
    }
    this.knownOrders = current;
    return symbol ? orders.filter(row => row.symbol === this.brokerSymbol(symbol)) : orders;
  }
  async listOpenPositions(symbol) { return (await this.listOpenOrders(symbol)).filter(row => row.__isPosition); }
  async findTarget(ticket, symbol, position) {
    const rows = await this.listOpenOrders(symbol);
    const row = rows.find(x => String(x.ticket) === String(ticket) && x.__isPosition === position);
    if (!row) throw Error('Позиция или заявка уже отсутствует. Обнови список.');
    return row;
  }
  validateCommandReceipt(id, result) {
    const record=this.commandJournal?.records.find(row=>row.id===id);
    if(!record || !['entry-protection','entry-close'].includes(record.operation))return result;
    if(!result || typeof result!=='object' || Array.isArray(result) || result.id!==id
      || result.entryId!==record.payload?.entryId || !['ok','rejected','unknown'].includes(result.status))return null;
    if(result.status==='ok') {
      const accounts=record.operation==='entry-close' ? record.payload.capturedAccounts
        : [record.payload.account,...(record.payload.mirrorAccounts || []).map(row=>row.account)];
      if(!Array.isArray(accounts) || !accounts.length || accounts.length>32
        || accounts.some(account=>typeof account!=='string' || !/^[1-9][0-9]{0,19}$/.test(account))
        || new Set(accounts).size!==accounts.length || !result.accountResults || Array.isArray(result.accountResults)
        || Object.keys(result.accountResults).length!==accounts.length
        || accounts.some(account=>result.accountResults[account]?.status!=='ok'))return null;
    }
    return result;
  }
  async command(operation, payload, {clickAt = Date.now(),preparation={}} = {}) {
    if(this.connectionRestarting || this.accountEnrollmentBusy) return {status:'rejected',provider:this.provider,reason:'Подключение обновляется. Дождись готовности счетов'};
    if (!Number.isFinite(clickAt) || Date.now()-clickAt<0 || Date.now()-clickAt>60000) clickAt=Date.now();
    const ticket = String(payload.ticket || '');
    if (!/^[1-9][0-9]{0,19}$/.test(ticket)) return {status:'rejected',provider:this.provider,reason:'Нужен номер конкретной позиции или заявки'};
    if (this.inFlight.has(ticket)) return {status:'unknown',provider:this.provider,reason:'Команда для этого номера уже выполняется'};
    this.inFlight.add(ticket);
    let submitted = false;
    try {
      const healthStarted=performance.now();
      const capabilityAge = Date.now()-this.capabilitiesAt;
      if (!this.capabilitiesAt || capabilityAge<0 || capabilityAge>15000) {
        const health=await this.health();
        this.capabilities=Array.isArray(health.capabilities) ? health.capabilities : [];
        this.capabilitiesAt=Date.now();
      }
      if (!this.capabilities.includes(operation)) return {status:'rejected',provider:this.provider,reason:'Приёмник VPS требует обновления для управления позициями'};
      preparation.healthReadMs=Number((performance.now()-healthStarted).toFixed(2));
      const receiptStarted=performance.now();
      if (this.commandJournal) await this.commandJournal.reconcile(async id => this.validateCommandReceipt(id,(await this.request(`/receipt?id=${encodeURIComponent(id)}`)).result));
      preparation.receiptReadMs=Number((performance.now()-receiptStarted).toFixed(2));
      const request = {...payload,ticket,id:payload.id || randomUUID(),symbol:this.brokerSymbol(payload.symbol),expiresAt:Date.now()+2000};
      let journalStarted;
      const send = async () => {
        preparation.journalBeforeSendMs=Number((performance.now()-journalStarted).toFixed(2));
        const macBeforeSendMs = Math.max(0,Date.now()-clickAt);
        const response = await this.request('/' + operation, request, {execution:true});
        const {result} = response;
        if (!result || !['ok','rejected','unknown'].includes(result.status)) throw Error('MT5 VPS: некорректное подтверждение команды');
        this.managementSnapshot = null; this.managementStateView=null; // A command may have changed the displayed volume or protection.
        return {...result,timings:{...result.timings,server:response.serverTiming || result.timings?.server,transport:response.transportTiming,
          ...preparation,macBeforeSendMs,macClickToResultMs:Math.max(0,Date.now()-clickAt)}};
      };
      let result;
      submitted = true;
      journalStarted=performance.now();
      if (this.commandJournal) result = await this.commandJournal.run(operation, request, send);
      else { try { result = await send(); } catch (error) { result = {status:'unknown',reason:error.message}; } }
      this.managementSnapshot = null; this.managementStateView=null;
      return {...result,provider:this.provider};
    } catch (error) { return {status:submitted ? 'unknown' : 'rejected',provider:this.provider,reason:error.message}; }
    finally { this.inFlight.delete(ticket); }
  }
  async cancelOrder(ticket, symbol) {
    try {
      // Author cancel controls can also target a filled position. Resolve its kind first.
      const rows = await this.listOpenOrders(symbol);
      const row = rows.find(x => String(x.ticket) === String(ticket));
      if (!row) throw Error('Заявка или позиция уже отсутствует');
      if (row.__isPosition) return this.closePosition(row);
      return this.cancelPendingOrder(ticket, row.symbol);
    } catch (error) { return {status:'rejected',provider:this.provider,reason:error.message}; }
  }
  async cancelPendingOrder(ticket, symbol, singleAccount = false, clickAt) {
    try {
      const row = await this.findManagementTarget({ticket,symbol},false);
      const result = await this.command('cancel', {ticket:String(ticket),symbol:row.symbol,...this.mirrorTarget(row,singleAccount)},{clickAt});
      if (result.status === 'ok') this.emitLifecycle('order:cancelled',{provider:this.provider,ticket:String(ticket)});
      return result;
    } catch (error) { return {status:'rejected',provider:this.provider,reason:error.message}; }
  }
  async closePosition(position, _reason, volume, percent, preparation={}) {
    try {
      const lookupStarted=performance.now();
      const row = await this.findManagementTarget(position,true);
      preparation={...preparation,targetLookupMs:Number((performance.now()-lookupStarted).toFixed(2))};
      const qty = percent===undefined ? (volume === undefined ? Number(row.lots) : Number(volume)) : percentVolume(row,percent);
      if (!Number.isFinite(qty) || qty <= 0 || qty > Number(row.lots)+1e-8) throw Error('Неверный объём закрытия в лотах');
      // A manual row comes from the displayed snapshot; do not silently close a changed position.
      const expected = position.lots === undefined ? Number(row.lots) : Number(position.lots);
      const result = await this.command('close', {ticket:String(row.ticket),symbol:row.symbol,volume:qty,expectedVolume:expected,
        ...(percent===undefined ? {} : {closePercent:Number(percent)}),...this.mirrorTarget(row,position.singleAccount)},{clickAt:position.clickAt,preparation});
      if (result.status === 'ok' && Math.abs(qty-Number(row.lots)) < 1e-8) this.emitLifecycle('position:closed',{provider:this.provider,ticket:String(row.ticket)});
      return result;
    } catch (error) { return {status:'rejected',provider:this.provider,reason:error.message}; }
  }
  async modifyOrder(target, changes) {
    try {
      if(isEntryPlanRow(target))return this.modifyEntryProtection(target,changes,{account:String(this.config.account)});
      const row = await this.findManagementTarget(target,target.__isPosition === true);
      if(isEntryPlanRow(row))throw Error('Эта позиция входит в набор БМЛ/СМЛ. Обнови список и выбери изменение защиты всего набора.');
      const values = {};
      for (const key of ['sl','tp','price']) if (Object.hasOwn(changes,key)) {
        const value = Number(changes[key]);
        if (changes[key] === '' || !Number.isFinite(value) || value < 0 || (key === 'price' && value === 0)) throw Error('Неверная цена ' + key.toUpperCase());
        values[key] = value;
      }
      if (!Object.keys(values).length) throw Error('Нет изменений');
      return this.command('modify',{ticket:String(row.ticket),symbol:row.symbol,target:row.__isPosition ? 'position' : 'order',...values,...this.mirrorTarget(row,target.singleAccount)},{clickAt:target.clickAt});
    } catch (error) { return {status:'rejected',provider:this.provider,reason:error.message}; }
  }
  async modifyEntryProtection(target, changes = {}, {account=String(this.config.account),operation='modify'} = {}) {
    let submitted=false,scope,ownsScope=false;
    const clickAt=Number.isFinite(target?.clickAt) && Date.now()-target.clickAt>=0 && Date.now()-target.clickAt<=60000 ? target.clickAt : Date.now();
    try {
      if(this.config.routePrefix)throw Error('Общий стоп и тейк БМЛ/СМЛ отправляются через основной приёмник VPS.');
      if(this.closed || this.connectionRestarting || this.accountEnrollmentBusy)throw Error('Подключение обновляется. Дождись готовности счетов.');
      if(!this.commandJournal)throw Error('Не подключён журнал команд защиты БМЛ/СМЛ.');
      const captured=entryPlanMetadata(target),selected=String(account);
      if(!['modify','breakeven'].includes(operation))throw Error('Неизвестное изменение защиты набора БМЛ/СМЛ.');
      scope=`entry:${captured.id}`;
      if(this.inFlight.has(scope))return {status:'unknown',provider:this.provider,reason:'Команда для всего набора БМЛ/СМЛ уже выполняется.'};
      this.inFlight.add(scope);
      ownsScope=true;
      // Always resolve ownership again on the primary transport, including a
      // separately selected follower. Renderer data never selects the tickets.
      const state=await this.managementState({maxAgeMs:-1});
      const primary=state.accountStates?.[String(this.config.account)] || state;
      if(!primary.connected || !primary.capabilities?.includes(ENTRY_PROTECTION_CAPABILITY))throw Error('Обнови и подключи основной приёмник VPS для защиты БМЛ/СМЛ.');
      const view=state.accountStates?.[selected] || (String(state.account)===selected ? state : null);
      if(!view?.connected || view.evidenceReady===false || view.server!==this.config.server || String(view.account)!==selected || !Array.isArray(view.orders))throw Error('Нет подтверждённых данных выбранного счёта MT5.');
      const row=view.orders.find(row=>String(row.ticket)===String(target.ticket) && row.symbol===this.brokerSymbol(target.symbol) && row.__isPosition===target.__isPosition);
      if(!row)throw Error('Выбранная позиция или заявка уже отсутствует. Обнови список.');
      const plan=entryPlanMetadata(row);
      if(plan.id!==captured.id || plan.side!==captured.side || JSON.stringify(plan.accounts)!==JSON.stringify(captured.accounts))throw Error('Принадлежность позиции набору БМЛ/СМЛ изменилась. Обнови список.');
      if(this.commandJournal)await this.commandJournal.reconcile(async id=>this.validateCommandReceipt(id,(await this.request(`/receipt?id=${encodeURIComponent(id)}`)).result),{ticket:scope});
      if(this.commandJournal.records.some(command=>command.ticket===scope && command.result?.status==='unknown'))return {status:'unknown',provider:this.provider,reason:'Есть неподтверждённая команда для всего набора БМЛ/СМЛ. Повтор заблокирован.'};
      const blocked=entryProtectionBlock({row,account:selected,linked:target.singleAccount!==true,capabilities:view.capabilities,
        commands:this.commandJournal?.records || [],accountStates:state.accountStates || {[selected]:view}});
      if(blocked)throw Error(blocked);
      const values={};
      for(const key of Object.keys(changes)) {
        if(!['sl','tp'].includes(key) || operation==='breakeven' && key==='sl')throw Error('Для всего набора допустимы только стоп и тейк; стоп во вход считает VPS.');
        const value=Number(changes[key]);
        if(typeof changes[key]!=='number' && typeof changes[key]!=='string' || String(changes[key]).trim()==='' || !Number.isFinite(value) || value<0 || key==='sl' && value===0)throw Error('Неверная цена '+key.toUpperCase());
        values[key]=value;
      }
      if(operation==='modify' && !Object.keys(values).length)throw Error('Нет изменений стопа или тейка.');
      const request={id:randomUUID(),entryId:plan.id,symbol:row.symbol,operation,account:selected,...values,expiresAt:Date.now()+2000};
      if(target.singleAccount!==true) {
        const mirrors=plan.accounts.filter(value=>value!==selected);
        if(mirrors.length)request.mirrorAccounts=mirrors.map(account=>({account}));
      }
      // Whole-parent uncertainty is stricter than an ordinary linked ticket:
      // even a known primary result cannot authorize a second parent command.
      const send=async()=>{
        const macBeforeSendMs=Math.max(0,Date.now()-clickAt);
        const response=await this.request('/entry-plan-protection',request,{execution:true});
        const result=response.result;
        if(!result || result.id!==request.id || result.entryId!==plan.id || !['ok','rejected','unknown'].includes(result.status))throw Error('VPS не подтвердил команду защиты всего набора.');
        const accounts=[selected,...(request.mirrorAccounts || []).map(row=>row.account)];
        if(result.status==='ok' && (Object.keys(result.accountResults || {}).length!==accounts.length
          || accounts.some(value=>result.accountResults?.[value]?.status!=='ok')))throw Error('VPS не подтвердил защиту на всех выбранных счетах.');
        const entry=controlEntry(response.entry,result,{id:plan.id,symbol:row.symbol,side:plan.side,accounts:plan.accounts},'entry-protection');
        return {...result,...(entry ? {entry} : {}),timings:{...result.timings,server:response.serverTiming || result.timings?.server,transport:response.transportTiming,
          macBeforeSendMs,macClickToResultMs:Math.max(0,Date.now()-clickAt)}};
      };
      submitted=true;
      const result=this.commandJournal ? await this.commandJournal.run('entry-protection',{...request,ticket:scope},send)
        : await send();
      this.managementSnapshot=null;this.managementStateView=null;
      if(result.entry)require('./entryLifecycle').rememberEntry(this,result.entry);
      return {...result,provider:this.provider};
    } catch(error) {
      if(submitted){this.managementSnapshot=null;this.managementStateView=null;}
      return {status:submitted?'unknown':'rejected',provider:this.provider,reason:error.message};
    } finally {if(ownsScope)this.inFlight.delete(scope);}
  }
  async closeEntryPosition(entry, {clickAt=Date.now()} = {}) {
    let scope,ownsScope=false,submitted=false;
    const preparationStarted=performance.now(),preparation={};
    try {
      if(this.config.routePrefix || this.closed || this.connectionRestarting || this.accountEnrollmentBusy)throw Error('Дождись готовности основного приёмника VPS.');
      if(!this.commandJournal)throw Error('Не подключён журнал команд БМЛ/СМЛ.');
      if(!entry || typeof entry.id!=='string' || !/^[A-Za-z0-9_.:-]{1,100}$/.test(entry.id))throw Error('Нет номера всего набора БМЛ/СМЛ.');
      scope=`entry:${entry.id}`;
      if(this.inFlight.has(scope))return {status:'unknown',provider:this.provider,reason:'Команда для всего набора БМЛ/СМЛ уже выполняется.'};
      this.inFlight.add(scope);ownsScope=true;
      const managementStarted=performance.now();
      let context;
      const now=Date.now(),age=now-Number(this.managementStateAt || 0);
      if(this.capabilities.includes(EXACT_CLOSE_CAPABILITY)) {
        // Only authenticated backend state chooses the captured accounts.
        // The VPS proves current ownership/remaining volume after draining
        // acquisition; an unrelated UI snapshot is not a close prerequisite.
        let parent=this.chunkLifecycleEntries?.get(entry.id)
          || this.managementStateView?.entryPlans?.find(row=>row.id===entry.id);
        if(!parent) {
          const response=await this.request('/entry-plan?id='+encodeURIComponent(entry.id));
          if(String(response.account)!==String(this.config.account) || response.server!==this.config.server
            || !response.capabilities?.includes(EXACT_CLOSE_CAPABILITY))throw Error('VPS не подтвердил точный протокол закрытия набора.');
          parent=response.entry;
        }
        const accounts=accountIds(parent);
        if(!parent || parent.id!==entry.id || parent.symbol!==this.brokerSymbol(entry.symbol)
          || parent.executionPolicy!=='hybrid-market-limit-v1' || !['buy','sell'].includes(parent.side)
          || !accounts || !accounts.includes(String(this.config.account)))throw Error('Не подтверждены исходный набор и состав счетов закрытия.');
        context={parent,accounts};preparation.managementSource='exact-parent-vps-proof';
      }
      if(!context && this.managementStateView && age>=0 && age<=1000) {
        // Reuse only a recent authenticated native proof for this exact parent.
        // The VPS still drains lanes and rechecks ownership, remaining volume
        // and every account before closing; no order is sent from this view.
        const state=managementFreshness(this.managementStateView,this.managementStateAt,now);
        try {
          const candidate=entryCloseContext(state,entry,this.brokerSymbol(entry.symbol),this.config);
          if([String(this.config.account),...candidate.accounts].every(account=>{
            const view=state.accountStates?.[account];
            return Number.isFinite(view?.checkedAt) && now-view.checkedAt>=-250 && now-view.checkedAt<=1000;
          })) {
            context=candidate;preparation.managementSource='recent-native-proof';
            preparation.managementAgeMs=age;
          }
        } catch { /* An incomplete view must take the original fresh-read path. */ }
      }
      if(!context) {
        const state=await this.managementState({maxAgeMs:-1});
        context=entryCloseContext(state,entry,this.brokerSymbol(entry.symbol),this.config);
        preparation.managementSource='fresh-read';
      }
      preparation.managementReadMs=Number((performance.now()-managementStarted).toFixed(3));
      const {parent,accounts}=context;
      const reconciliationStarted=performance.now();
      await this.commandJournal.reconcile(async id=>this.validateCommandReceipt(id,(await this.request(`/receipt?id=${encodeURIComponent(id)}`)).result),{ticket:scope});
      preparation.receiptReconciliationMs=Number((performance.now()-reconciliationStarted).toFixed(3));
      if(this.commandJournal.records.some(command=>command.ticket===scope && command.result?.status==='unknown'))return {status:'unknown',provider:this.provider,reason:'Есть неподтверждённая команда для всего набора БМЛ/СМЛ. Повтор заблокирован.'};
      if(!Number.isFinite(clickAt) || Date.now()-clickAt<0 || Date.now()-clickAt>60000)clickAt=Date.now();
      const request={id:randomUUID(),entryId:parent.id,symbol:parent.symbol,expiresAt:Date.now()+2000};
      submitted=true;
      const journalStarted=performance.now();
      const result=await this.commandJournal.run('entry-close',{...request,ticket:scope,capturedAccounts:accounts},async()=>{
        const macBeforeSendMs=Math.max(0,Date.now()-clickAt);
        preparation.journalBeforeSendMs=Number((performance.now()-journalStarted).toFixed(3));
        preparation.totalMs=Number((performance.now()-preparationStarted).toFixed(3));
        const response=await this.request('/entry-plan-close',request,{execution:true}),value=response.result;
        if(!value || value.id!==request.id || value.entryId!==parent.id || !['ok','rejected','unknown'].includes(value.status)
          || value.status==='ok' && (Object.keys(value.accountResults || {}).length!==accounts.length || accounts.some(account=>value.accountResults?.[account]?.status!=='ok')))throw Error('VPS не подтвердил закрытие всего набора БМЛ/СМЛ.');
        const entry=controlEntry(response.entry,value,{id:parent.id,symbol:parent.symbol,side:parent.side,accounts},'entry-close');
        return {...value,...(entry ? {entry} : {}),timings:{...value.timings,server:response.serverTiming || value.timings?.server,transport:response.transportTiming,
          macBeforeSendMs,macClickToResultMs:Math.max(0,Date.now()-clickAt),macPreparation:{...preparation}}};
      });
      this.managementSnapshot=null;this.managementStateView=null;
      if(result.entry)require('./entryLifecycle').rememberEntry(this,result.entry);
      return {...result,provider:this.provider};
    } catch(error) {
      if(submitted){this.managementSnapshot=null;this.managementStateView=null;}
      return {status:submitted?'unknown':'rejected',provider:this.provider,reason:error.message};
    } finally {if(ownsScope)this.inFlight.delete(scope);}
  }
  mirrorTarget(row, singleAccount = row.singleAccount) {
    if (singleAccount || this.config.routePrefix) return {};
    return row.mirrorAccounts?.length ? {mirrorAccounts:row.mirrorAccounts.map(account=>({account:String(account)}))}
      : row.mirrorAccount ? {mirrorAccount:String(row.mirrorAccount)} : {};
  }
  async listCloseRules() { return (await this.request('/close-rules')).rules; }
  async schedulePartialClose(position, volume, targetPrice, comparison, percent) {
    try {
      const row = await this.findManagementTarget(position,true);
      const qty = percent===undefined ? Number(volume) : percentVolume(row,percent), price = Number(targetPrice);
      if (!Number.isFinite(qty) || qty <= 0 || qty >= Number(row.lots) || !Number.isFinite(price) || price <= 0 || !['gte','lte'].includes(comparison)) throw Error('Неверные условия частичного закрытия');
      return this.command('schedule-close',{ticket:String(row.ticket),symbol:row.symbol,volume:qty,
        expectedVolume:Number(position.lots ?? row.lots),targetPrice:price,comparison,
        ...(percent===undefined ? {} : {closePercent:Number(percent)}),...this.mirrorTarget(row,position.singleAccount)},{clickAt:position.clickAt});
    } catch (error) { return {status:'rejected',provider:this.provider,reason:error.message}; }
  }
  async cancelScheduledClose(position, ruleId) {
    return this.command('cancel-close',{ticket:String(position.ticket),symbol:position.symbol,ruleId,...this.mirrorTarget(position)},{clickAt:position.clickAt});
  }
  async getRiskPositionSnapshot(tracked) {
    const row = await this.findTarget(tracked.ticket, tracked.symbol, tracked.kind !== 'order');
    const metadata = await this.getInstrumentMetadata(row.symbol);
    return {symbol:row.symbol,side:row.type,qty:Number(row.lots),entryPrice:row.open_price,
      currentPrice:row.currentPrice,stopLossPrice:row.sl,unrealizedPnl:row.pnl,...metadata};
  }
}
module.exports = { MT5VPSAdapter, isIndex };

const {freshExecutionSnapshot} = require('./quoteSnapshot');

function indexSymbol(value) {
  const name = String(value || '').toUpperCase().split(':').pop();
  return name === 'SPX' ? 'SPX500' : name;
}

// Preparation sends only a status request. Placement remains a separate click.
function createApproachPreparation({getGroups, warm, onState, onApproach, onNear, clock = Date.now}) {
  const records = new Map();
  let readyAt = null, attemptedAt = null, pending = null, closed = false;
  let wasNear = false;
  function candidate(snapshot) {
    const ticker = indexSymbol(snapshot?.symbol);
    if (!['USTEC','SPX500'].includes(ticker) || !freshExecutionSnapshot(snapshot,'mt5-vps',ticker,clock())) return null;
    const price = (snapshot.quote.bid + snapshot.quote.ask) / 2;
    const nearest = (getGroups() || []).flatMap(group => {
      const offset = Number(group.maxOffset);
      if (group.enabled === false || indexSymbol(group.ticker) !== ticker || !Number.isFinite(offset) || offset < 0) return [];
      return (group.levels || []).filter(level => Number.isFinite(level) && level > 0).map(level => ({
        ticker, key:group.key, level, distance:Math.abs(price-level), maxOffset:offset, threshold:offset*2
      }));
    }).sort((a,b) => a.distance-b.distance)[0];
    return nearest && nearest.distance <= nearest.threshold ? nearest : null;
  }
  function publish(record, value) {
    record.state = {...value,at:clock(),readyAt};
    onState?.({...record.state});
  }
  function evaluate(record) {
    const next = candidate(record.snapshot);
    if (!next) {
      // Stale quotes do not re-arm a dismissed card. Moving away does.
      if (record.latch && freshExecutionSnapshot(record.snapshot,'mt5-vps',record.ticker,clock())) {
        const price = (record.snapshot.quote.bid+record.snapshot.quote.ask)/2;
        const group = (getGroups() || []).find(group => group.key === record.latch.key && group.enabled !== false
          && indexSymbol(group.ticker) === record.ticker && group.levels?.includes(record.latch.level));
        if (!group || Math.abs(price-record.latch.level) > Number(group.maxOffset)*2.25) record.latch = null;
      }
      if (record.state?.phase !== 'idle') publish(record,{ticker:record.ticker,phase:'idle',near:false});
      return null;
    }
    const isReady = readyAt !== null && clock()-readyAt >= 0 && clock()-readyAt < 15000;
    publish(record,{...next,near:true,phase:isReady ? 'ready' : 'preparing'});
    const switched = record.latch && (record.latch.key !== next.key || record.latch.level !== next.level);
    const price = (record.snapshot.quote.bid+record.snapshot.quote.ask)/2;
    const entering = !record.latch || (switched && Math.abs(price-record.latch.level) > record.latch.maxOffset*2.25);
    record.latch = next;
    if (entering) onApproach?.({...record.state});
    return next;
  }
  function maintain() {
    if (closed) return;
    const near = [...records.values()].map(evaluate).some(Boolean);
    if (near !== wasNear) { wasNear = near; onNear?.(near); }
    if (!near || pending || (attemptedAt !== null && clock()-attemptedAt < (readyAt === null ? 2000 : 10000))) return;
    attemptedAt = clock();
    pending = Promise.resolve().then(warm).then(() => {
      if (closed) return;
      readyAt = clock();
      for (const record of records.values()) evaluate(record);
    }, () => {
      if (closed) return;
      readyAt = null;
      for (const record of records.values()) {
        const next = candidate(record.snapshot);
        if (next) publish(record,{...next,near:true,phase:'error'});
      }
    }).finally(() => { pending = null; });
  }
  return {
    update(snapshot) {
      if (closed || snapshot?.provider !== 'mt5-vps') return;
      const ticker = indexSymbol(snapshot.symbol);
      if (!['USTEC','SPX500'].includes(ticker)) return;
      const record = records.get(ticker) || {ticker};
      record.snapshot = snapshot;
      records.set(ticker,record);
      maintain();
    },
    tick:maintain,
    getState(ticker) {
      const record = records.get(indexSymbol(ticker));
      if (!record || !candidate(record.snapshot)) return {phase:'idle',near:false};
      return {...record.state,phase:readyAt !== null && clock()-readyAt >= 0 && clock()-readyAt < 15000 ? 'ready' : 'preparing'};
    },
    close() { closed = true; records.clear(); if (wasNear) { wasNear=false; onNear?.(false); } }
  };
}
module.exports = {createApproachPreparation,indexSymbol};

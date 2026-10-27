function sameSymbol(left,right) {
  const normalize = value => {
    const name = String(value || '').toUpperCase().split(':').pop();
    return name === 'SPX' ? 'SPX500' : name;
  };
  return normalize(left) === normalize(right);
}

// Only a snapshot passed inside the main process may skip the second lookup.
function freshExecutionSnapshot(snapshot,provider,symbol,now=Date.now()) {
  const age = now-Number(snapshot?.quoteUpdatedAt || 0);
  const bid = snapshot?.quote?.bid, ask = snapshot?.quote?.ask;
  return provider === 'mt5-vps' && snapshot?.provider === provider && sameSymbol(snapshot.symbol,symbol)
    && age >= 0 && age <= 750 && Number.isFinite(bid) && bid > 0 && Number.isFinite(ask) && ask >= bid
    ? snapshot : null;
}

// A conditional VPS plan needs instrument specifications, not a Mac price.
// Actual account, lot/risk and fresh executable prices are checked on the VPS.
function entrySpecification(snapshot,provider,symbol) {
  const metadata=snapshot?.metadata;
  return provider==='mt5-vps' && snapshot?.provider===provider && sameSymbol(snapshot.symbol,symbol)
    && metadata && ['tickSize','quantityStep','contractSize'].every(key=>Number.isFinite(metadata[key]) && metadata[key]>0)
    && Number.isFinite(metadata.minQty ?? metadata.quantityStep) && (metadata.minQty ?? metadata.quantityStep)>0
    && Number.isFinite(metadata.maxQty) && metadata.maxQty>=(metadata.minQty ?? metadata.quantityStep)
    ? snapshot : null;
}
function conditionalEntrySnapshot(instrumentInfo,symbol,context={}) {
  const request={...context,provider:'mt5-vps',symbol};
  // A delivery pause must not turn an already confirmed specification into
  // another quote/metadata round trip on the command path. VPS admission
  // still validates the captured plan against each real account's metadata.
  return entrySpecification(instrumentInfo.peek(request),'mt5-vps',symbol)
    || instrumentInfo.get(request,{quote:false});
}
module.exports = {freshExecutionSnapshot,entrySpecification,conditionalEntrySnapshot};

function finiteNumber(value, fallback = null) {
  if (value == null || value === '') return fallback;
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function roundQty(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Number(n.toFixed(10));
}

function normalizeMinLot(value) {
  const n = finiteNumber(value, 1);
  return Number.isFinite(n) && n > 0 ? n : 1;
}

const PRICE_SOURCES = new Set(['bid', 'ask', 'mid']);

function normalizePriceSource(value, fallback) {
  const source = String(value || '').trim().toLowerCase();
  return PRICE_SOURCES.has(source) ? source : fallback;
}

function quoteRequiredReason(source) {
  if (source === 'ask') return 'Ask quote required';
  if (source === 'mid') return 'Bid/Ask quote required';
  return 'Bid quote required';
}

function resolveQuotePrice({ bid, ask, source }) {
  const bidPrice = finiteNumber(bid);
  const askPrice = finiteNumber(ask);
  if (source === 'ask') {
    return Number.isFinite(askPrice) && askPrice > 0
      ? { ok: true, price: askPrice, bid: bidPrice, ask: askPrice }
      : { ok: false, reason: quoteRequiredReason(source) };
  }
  if (source === 'mid') {
    return Number.isFinite(bidPrice) && bidPrice > 0 && Number.isFinite(askPrice) && askPrice > 0
      ? { ok: true, price: (bidPrice + askPrice) / 2, bid: bidPrice, ask: askPrice }
      : { ok: false, reason: quoteRequiredReason(source) };
  }
  return Number.isFinite(bidPrice) && bidPrice > 0
    ? { ok: true, price: bidPrice, bid: bidPrice, ask: askPrice }
    : { ok: false, reason: quoteRequiredReason(source) };
}

function roundQtyToStep(value, minLot = 1) {
  const qty = Number(value);
  const step = normalizeMinLot(minLot);
  if (!Number.isFinite(qty) || qty <= 0) return 0;
  const units = Math.floor((qty + step * 1e-9) / step);
  return roundQty(units * step);
}

function resolveLevelOrderDefaults(config = {}, ticker) {
  const defaults = config.defaults || {};
  const normalizedTicker = String(ticker || '').trim().toUpperCase();
  const override = Array.isArray(config.symbols)
    ? config.symbols.find(item => String(item?.ticker || '').trim().toUpperCase() === normalizedTicker)
    : null;
  return {
    maxLot: finiteNumber(override?.maxLot, finiteNumber(defaults.maxLot, 0)),
    minLot: normalizeMinLot(finiteNumber(override?.minLot, finiteNumber(defaults.minLot, 1))),
    stopOffsetPts: finiteNumber(override?.stopOffsetPts, finiteNumber(defaults.stopOffsetPts, null)),
    stopMode: override?.stopMode ?? defaults.stopMode ?? 'level',
    fillPriority: override?.fillPriority ?? defaults.fillPriority,
    takeProfitPts: finiteNumber(override?.takeProfitPts, finiteNumber(defaults.takeProfitPts, null)),
    // Older fixed-lot chunkVolume settings do not define percentage sizing.
    chunkPercent: Number(override?.chunkPercent ?? defaults.chunkPercent ?? 20),
    chunkMaxGap: finiteNumber(override?.chunkMaxGap, finiteNumber(defaults.chunkMaxGap, 2)),
    buyPriceSource: normalizePriceSource(override?.buyPriceSource, normalizePriceSource(defaults.buyPriceSource, 'bid')),
    sellPriceSource: normalizePriceSource(override?.sellPriceSource, normalizePriceSource(defaults.sellPriceSource, 'bid'))
  };
}

function calculateChunkSizing({ totalQty, chunkPercent, quantityStep, minQty = quantityStep } = {}) {
  const total = Number(totalQty), percent = Number(chunkPercent), step = Number(quantityStep), minimum = Number(minQty);
  if (!Number.isFinite(percent) || percent < 2 || percent > 100)
    return { ok: false, reason: 'Размер части: от 2 до 100%' };
  if (!Number.isFinite(total) || total <= 0 || !Number.isFinite(step) || step <= 0
    || !Number.isFinite(minimum) || minimum <= 0)
    return { ok: false, reason: 'Нет допустимого объёма или шага лота MT5' };
  const totalUnits = Math.round(total / step);
  if (!Number.isSafeInteger(totalUnits) || totalUnits <= 0 || Math.abs(total / step - totalUnits) > 1e-7)
    return { ok: false, reason: 'Объём не соответствует шагу лота MT5' };
  // Round down once before submission. VPS keeps cumulative follower rounding,
  // minimum-lot checks and the unchanged total-risk budget for every tranche.
  const chunkUnits = Math.floor(totalUnits * percent / 100 + 1e-9);
  const chunkVolume = roundQty(chunkUnits * step);
  if (chunkUnits < 1 || chunkVolume + step * 1e-9 < minimum)
    return { ok: false, reason: 'Часть меньше минимального лота MT5. Увеличь процент' };
  const chunkCount = Math.ceil(totalUnits / chunkUnits);
  if (chunkCount > 50)
    return { ok: false, reason: 'Слишком много частей: максимум 50. Увеличь процент' };
  return { ok: true, chunkPercent: percent, chunkVolume, chunkCount };
}

function splitQuantity(totalQty, maxLot, instrumentType, minLot = 1) {
  const total = roundQtyToStep(totalQty, minLot);
  const max = Number(maxLot);
  if (!Number.isFinite(total) || total <= 0) return [];
  if (!Number.isFinite(max) || max <= 0 || max >= total) return [total];

  const cap = roundQtyToStep(max, minLot);
  if (!Number.isFinite(cap) || cap <= 0) return [total];

  const parts = [];
  let remaining = total;
  while (remaining > cap) {
    parts.push(cap);
    remaining = roundQty(remaining - cap);
  }
  if (remaining > 0) parts.push(roundQtyToStep(remaining, minLot));
  return parts.filter(q => Number.isFinite(q) && q > 0);
}

function calculateLimitBidTradePlan({
  action,
  ticker,
  instrumentType,
  level,
  riskUsd,
  stopOffsetPts,
  stopMode = 'level',
  maxLot,
  minLot = 1,
  takeProfitPts,
  bid,
  ask,
  buyPriceSource,
  sellPriceSource,
  entryPrice,
  tickSize,
  lot = 1,
  orderCalculator
} = {}) {
  const sideAction = String(action || '').toUpperCase();
  if (!['LB','LS','BS','SS'].includes(sideAction)) {
    return { ok: false, reason: 'Unsupported level order action' };
  }

  const lvl = finiteNumber(level);
  const tick = finiteNumber(tickSize);
  const risk = finiteNumber(riskUsd);
  const offset = finiteNumber(stopOffsetPts);
  const qtyStep = normalizeMinLot(minLot);
  if (!['level', 'entry'].includes(stopMode)) return { ok: false, reason: 'Stop mode must be level or entry' };
  if (!Number.isFinite(lvl) || lvl <= 0) return { ok: false, reason: 'Level > 0 required' };
  if (!Number.isFinite(tick) || tick <= 0) return { ok: false, reason: 'Tick size required' };
  if (!Number.isFinite(risk) || risk <= 0) return { ok: false, reason: 'Risk $ > 0 required' };
  if (!Number.isFinite(offset) || offset <= 0) return { ok: false, reason: 'Stop offset pts > 0 required' };

  const isStop = sideAction === 'BS' || sideAction === 'SS';
  const isBuy = sideAction === 'LB' || sideAction === 'BS';
  if (isStop && (entryPrice === undefined || entryPrice === null)) return {ok:false,reason:'Укажи цену срабатывания'};
  const priceSource = isBuy
    ? normalizePriceSource(buyPriceSource, 'bid')
    : normalizePriceSource(sellPriceSource, 'bid');
  const quotePrice = resolveQuotePrice({ bid, ask, source: priceSource });
  if (!quotePrice.ok) return quotePrice;
  const explicitEntry = entryPrice !== undefined && entryPrice !== null;
  const referencePrice = explicitEntry ? finiteNumber(entryPrice) : quotePrice.price;
  if (!Number.isFinite(referencePrice) || referencePrice<=0) return {ok:false,reason:'Укажи положительную цену входа'};
  if (isBuy && referencePrice < lvl) return {ok:false,reason:explicitEntry ? 'вход ниже уровня LT' : `Cannot buy when ${priceSource} is below level`};
  if (!isBuy && referencePrice > lvl) return {ok:false,reason:explicitEntry ? 'вход выше уровня LT' : `Cannot sell when ${priceSource} is above level`};
  if (explicitEntry) {
    const units = referencePrice/tick;
    if (Math.abs(units-Math.round(units))>1e-6) return {ok:false,reason:'Цена входа не соответствует шагу цены'};
    const market = finiteNumber(isBuy ? ask : bid);
    if (!Number.isFinite(market) || market<=0) return {ok:false,reason:isBuy ? 'Ask quote required' : 'Bid quote required'};
    if (isStop ? (isBuy ? referencePrice<=market : referencePrice>=market) : (isBuy ? referencePrice>=market : referencePrice<=market)) {
      return {ok:false,reason:isStop ? (isBuy ? 'Buy Stop должен быть выше Ask' : 'Sell Stop должен быть ниже Bid') : (isBuy ? 'вход должен быть ниже Ask' : 'вход должен быть выше Bid')};
    }
  }

  const levelDistancePts = Math.abs(referencePrice - lvl) / tick;
  const stopPts = stopMode === 'entry' ? offset : levelDistancePts + offset;
  const stopBase = stopMode === 'entry' ? referencePrice : lvl;
  const stopPrice = isBuy ? stopBase - offset * tick : stopBase + offset * tick;
  const qty = orderCalculator.qty({
    riskUsd: risk,
    stopPts,
    tickSize: tick,
    lot,
    instrumentType,
    quantityStep: qtyStep
  });
  const childQtys = splitQuantity(qty, maxLot, instrumentType, qtyStep);
  if (!childQtys.length) return { ok: false, reason: 'Calculated quantity is 0' };

  const tp = finiteNumber(takeProfitPts);
  return {
    ok: true,
    ticker,
    action: sideAction,
    orderKind: isStop ? (isBuy ? 'BS' : 'SS') : (isBuy ? 'BL' : 'SL'),
    orderType: isStop ? 'stop' : 'limit',
    orderSide: isBuy ? 'buy' : 'sell',
    level: lvl,
    bid: quotePrice.bid,
    ask: quotePrice.ask,
    priceSource: explicitEntry ? 'manual' : priceSource,
    referencePrice,
    tickSize: tick,
    riskUsd: risk,
    stopOffsetPts: offset,
    stopMode,
    minLot: qtyStep,
    levelDistancePts,
    stopPts,
    stopPrice,
    takeProfitPts: Number.isFinite(tp) && tp > 0 ? tp : null,
    totalQty: qty,
    childQtys
  };
}

module.exports = {
  resolveLevelOrderDefaults,
  normalizePriceSource,
  resolveQuotePrice,
  quoteRequiredReason,
  splitQuantity,
  roundQtyToStep,
  calculateChunkSizing,
  calculateLimitBidTradePlan
};

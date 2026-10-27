const {roundQtyToStep} = require('./strategy');

// New entries buy within [LT, LT + play] and sell within [LT - play, LT].
// Both edges are inclusive. Legacy zone and boundary plans remain
// calculable for compatibility tests and recovery, but cannot be newly armed.
// Entry stops keep the ordinary limit-entry distance and quantity. A stop from
// LT is fixed, so only that mode sizes at the adverse edge of the zone.
function calculateMarketZoneTradePlan({action, ticker, instrumentType, level, entryPrice,
  maxOffset, zoneMultiplier = 1, entryMode = 'side-zone', maxGap = 2, riskUsd, stopOffsetPts, stopMode = 'entry', takeProfitPts,
  tickSize, minLot, minQty = minLot, maxQty, maxLot = 0, chunkPercent = 30, lot = 1, orderCalculator} = {}) {
  if (!['MB','MS'].includes(action)) return {ok:false, reason:'Выбери направление рынка'};
  const values = [level, entryPrice, maxOffset, riskUsd, stopOffsetPts, tickSize, minLot, minQty, maxQty, maxLot, lot];
  if (values.some(value => value == null || value === '' || !Number.isFinite(Number(value))))
    return {ok:false, reason:'Нет параметров входа или спецификации MT5'};
  const [lt, suppliedCenter, offset, risk, stop, tick, step, minimum, maximum, cap, pointValue] = values.map(Number);
  const multiplier = Number(zoneMultiplier);
  if (![1,2].includes(multiplier)) return {ok:false, reason:'Люфт рынка: только ×1 или ×2'};
  // The existing receiver also expects a relative guard. A whole band width
  // cannot reject movement inside that band; absolute bounds still apply.
  const gap = entryMode === 'side-zone' ? offset * multiplier : Number(maxGap);
  if (!Number.isFinite(gap)) return {ok:false, reason:'Проверь допустимое отклонение цены'};
  if (lt <= 0 || suppliedCenter <= 0 || offset < 0 || gap < 0 || risk <= 0 || stop <= 0 || tick <= 0 || step <= 0
    || minimum <= 0 || maximum < minimum || cap < 0 || pointValue <= 0)
    return {ok:false, reason:'Проверь уровень, люфт, риск, стоп и лот'};
  if (!['entry','level'].includes(stopMode)) return {ok:false, reason:'Выбери стоп от входа или уровня'};
  const isBuy = action === 'MB', direction = isBuy ? 1 : -1;
  if (!['side-zone','boundary','zone'].includes(entryMode)) return {ok:false,reason:'Неверный режим рынка'};
  const zoneRadius = offset * multiplier;
  const center = entryMode === 'side-zone' ? lt
    : entryMode === 'boundary' ? Number((lt + direction * zoneRadius).toFixed(10)) : suppliedCenter;
  const zoneLower = Number((entryMode === 'side-zone' ? center - (isBuy ? 0 : zoneRadius)
    : entryMode === 'boundary' ? center - (isBuy ? 0 : gap) : center - zoneRadius).toFixed(10));
  const zoneUpper = Number((entryMode === 'side-zone' ? center + (isBuy ? zoneRadius : 0)
    : entryMode === 'boundary' ? center + (isBuy ? gap : 0) : center + zoneRadius).toFixed(10));
  const stopPrice = Number(((stopMode === 'entry' ? center : lt) - direction * stop * tick).toFixed(10));
  const take = takeProfitPts == null || takeProfitPts === '' ? 0 : Number(takeProfitPts);
  const takePrice = take > 0 ? Number((center + direction * take * tick).toFixed(10)) : 0;
  const onTick = price => Math.abs(price / tick - Math.round(price / tick)) <= 1e-6;
  if (!Number.isFinite(zoneRadius) || !Number.isFinite(zoneUpper) || zoneLower <= 0
    || !onTick(center) || !onTick(stopPrice) || (takePrice && !onTick(takePrice)))
    return {ok:false, reason:'Цена входа и защиты должны соответствовать шагу цены MT5'};
  if (!Number.isFinite(take) || take < 0 || !Number.isFinite(takePrice) || takePrice < 0)
    return {ok:false, reason:'Проверь тейк'};
  const favorableEdge = isBuy ? zoneLower : zoneUpper;
  const worstPrice = isBuy ? zoneUpper : zoneLower;
  if (stopPrice <= 0 || (stopMode === 'entry' ? zoneLower - stop * tick <= 0
    : direction * (favorableEdge - stopPrice) <= 0))
    return {ok:false, reason:'Вся зона рынка должна находиться перед SL. Уменьши люфт или увеличь стоп'};
  if (takePrice && direction * (takePrice - worstPrice) <= 0)
    return {ok:false, reason:'Вся зона рынка должна находиться до TP'};
  const stopPts = stopMode === 'entry' ? stop : direction * (worstPrice - stopPrice) / tick;
  const totalQty = roundQtyToStep(orderCalculator.qty({riskUsd:risk, stopPts, tickSize:tick,
    lot:pointValue, instrumentType, quantityStep:step}), step);
  const percent = entryMode === 'side-zone' ? Number(chunkPercent) : 100;
  if (!Number.isFinite(percent) || percent <= 0 || percent > 100)
    return {ok:false, reason:'Размер рыночной части: больше 0 и не больше 100%'};
  // A tiny risk-sized target may permit just one minimum lot. Do not reject
  // it or round the total upwards; broker minimums can enlarge a tiny part.
  const percentageVolume = Math.max(minimum, roundQtyToStep(totalQty * percent / 100, step));
  const chunkVolume = roundQtyToStep(Math.min(totalQty, maximum, cap > 0 ? cap : maximum, percentageVolume), step);
  if (totalQty < minimum - 1e-8 || chunkVolume < minimum - 1e-8)
    return {ok:false, reason:'Расчётный объём меньше минимального лота MT5'};
  const maxChunks = entryMode === 'side-zone' ? 100 : 50;
  if (Math.ceil(totalQty / chunkVolume - 1e-9) > maxChunks)
    return {ok:false, reason:entryMode === 'side-zone'
      ? 'Расчётный объём требует больше 100 частей. Увеличь процент части или Max lot либо уменьши риск'
      : 'Объём требует больше 50 рыночных частей. Увеличь Max lot'};
  return {ok:true, ticker, action, entryMode, orderKind:isBuy ? 'B' : 'S', orderType:'market',
    ...(entryMode === 'side-zone' ? {executionPolicy:'hybrid-market-limit-v1'} : {}),
    orderSide:isBuy ? 'buy' : 'sell', level:lt, referencePrice:center, worstPrice,
    zoneRadius, zoneMultiplier:multiplier, zoneLower, zoneUpper, maxGap:entryMode === 'zone' ? zoneRadius : gap,
    riskUsd:risk, tickSize:tick, minLot:step, stopOffsetPts:stop, stopMode,
    stopPrice, stopPts, takeProfitPts:take || null, totalQty, chunkVolume,
    chunkPercent:percent, priceSource:'manual'};
}

function resolveMarketChunkPercent(config = {}, ticker) {
  const symbol = Array.isArray(config.symbols) ? config.symbols.find(row =>
    String(row?.ticker || '').trim().toUpperCase() === String(ticker || '').trim().toUpperCase()) : null;
  const percent = Number(symbol?.chunkPercent ?? config.defaults?.chunkPercent ?? 30);
  // The saved percentage is authoritative, including fractions and 100%.
  // Only absent or invalid values use the new-entry default.
  return Number.isFinite(percent) && percent > 0 && percent <= 100 ? percent : 30;
}

module.exports = {calculateMarketZoneTradePlan, resolveMarketChunkPercent};

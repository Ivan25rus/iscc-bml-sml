const {indexSymbol} = require('../mt5-vps/approachPreparation');

function validateTrackedEntry({groups = [], symbol, level, entryPrice}) {
  const selected = Number(level), entry = Number(entryPrice);
  if (!Number.isFinite(selected) || selected <= 0 || !Number.isFinite(entry) || entry <= 0) {
    return {ok:false,reason:'Укажи уровень LT и цену входа'};
  }
  const matching = groups.filter(group => group.enabled !== false && indexSymbol(group.ticker)===indexSymbol(symbol)
    && Array.isArray(group.levels) && group.levels.some(value => Math.abs(Number(value)-selected)<1e-8)
    && Number.isFinite(Number(group.maxOffset)) && Number(group.maxOffset)>=0);
  if (!matching.length) return {ok:false,reason:'Выбранный уровень уже отсутствует в активных группах LT'};
  const maxOffset = Math.min(...matching.map(group => Number(group.maxOffset)));
  if (Math.abs(entry-selected)>maxOffset+1e-8) return {ok:false,reason:`Цена входа вне люфта ${maxOffset} от уровня LT`};
  return {ok:true,level:selected,entryPrice:entry,maxOffset};
}

module.exports = {validateTrackedEntry};

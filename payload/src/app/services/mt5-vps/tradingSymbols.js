const TRADING_SYMBOLS = Object.freeze(['USTEC', 'SPX500', 'XAUUSD']);
const GOLD_ENTRY_CAPABILITY = 'entry-gold-v1';

function normalizeTradingSymbol(value) {
  const name = String(value || '').toUpperCase().split(':').pop();
  return name === 'SPX' ? 'SPX500' : name;
}

function isTradingSymbol(value) {
  return TRADING_SYMBOLS.includes(normalizeTradingSymbol(value));
}

module.exports = { TRADING_SYMBOLS, GOLD_ENTRY_CAPABILITY, normalizeTradingSymbol, isTradingSymbol };

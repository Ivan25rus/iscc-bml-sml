// Levels are normalized once by the service. Preserve original order for ties,
// including equal distances produced by floating-point rounding.
function createLevelIndex(levels) {
  const ordered = levels.map((level, ordinal) => ({ level, ordinal })).sort((a, b) => a.level - b.level || a.ordinal - b.ordinal);
  const unique = ordered.filter((item, index) => !index || item.level !== ordered[index - 1].level);
  return {
    get length() { return unique.length; },
    nearest(price) {
      let lo = 0, hi = unique.length;
      while (lo < hi) {
        const middle = lo + Math.floor((hi - lo) / 2);
        if (unique[middle].level < price) lo = middle + 1; else hi = middle;
      }
      let nearest = null;
      const consider = index => {
        const item = unique[index], distance = Math.abs(price - item.level);
        if (!nearest || distance < nearest.distance || (distance === nearest.distance && item.ordinal < nearest.ordinal)) {
          nearest = { level: item.level, ordinal: item.ordinal, distance };
        }
      };
      if (lo > 0) consider(lo - 1);
      if (lo < unique.length) consider(lo);
      if (!nearest) return null;
      for (let index = lo - 2; index >= 0 && Math.abs(price - unique[index].level) === nearest.distance; index--) consider(index);
      for (let index = lo + 1; index < unique.length && Math.abs(price - unique[index].level) === nearest.distance; index++) consider(index);
      return { level: nearest.level, distance: nearest.distance };
    }
  };
}

module.exports = { createLevelIndex };

(function(global){
  'use strict';
  const ids=['bml-sml','accounts','management','partial-close','price-close','time-exit','levels','levels-to-lt'];
  const requires={'partial-close':['management'],'price-close':['partial-close'],'levels-to-lt':['levels']};
  function resolve(features){
    if(!Array.isArray(features) || features.length>ids.length || new Set(features).size!==features.length)
      throw Error('Выберите функции без повторов');
    const chosen=new Set(),add=id=>{
      if(!ids.includes(id))throw Error('Неизвестная функция: '+id);
      (requires[id]||[]).forEach(add);chosen.add(id);
    };
    features.forEach(add);
    if(!chosen.size)throw Error('Выберите хотя бы одну функцию');
    return {features:ids.filter(id=>chosen.has(id)),automaticallyIncluded:ids.filter(id=>chosen.has(id)&&!features.includes(id)),
      mt5Core:ids.slice(0,6).some(id=>chosen.has(id))};
  }
  const api={ids,resolve};
  if(typeof module==='object' && module.exports)module.exports=api;else global.ISCCSelection=api;
})(typeof globalThis==='object'?globalThis:this);

function followers(config) {
  const rows = config.followers ?? (config.mirrorAccount ? [{account:config.mirrorAccount,multiplier:1,enabled:true}] : []);
  if (!Array.isArray(rows) || rows.length>16) throw Error('Допускается до 16 дополнительных счетов');
  const seen=new Set([String(config.account)]);
  return rows.map(row=>{
    const account=String(row.account), multiplier=Number(row.multiplier);
    if (!/^[1-9][0-9]{0,15}$/.test(account) || seen.has(account) || !Number.isFinite(multiplier) || multiplier<=0 || multiplier>100) throw Error('Неверный счёт или коэффициент лота');
    seen.add(account);
    return {account,multiplier,enabled:row.enabled!==false};
  });
}
function percentVolume(row, percent) {
  const fraction=Number(percent)/100, lots=Number(row.lots), step=Number(row.quantityStep), minimum=Number(row.minQty);
  if (!Number.isFinite(fraction) || fraction<=0 || fraction>1 || !(step>0 && minimum>0 && lots>0)) throw Error('Неверный процент закрытия');
  const amount=fraction===1 ? lots : Number((Math.floor((lots*fraction+step*1e-8)/step)*step).toFixed(8));
  const remaining=lots-amount;
  if (amount<minimum-1e-8 || (remaining>1e-8 && remaining<minimum-1e-8)) throw Error('Процент даёт объём меньше минимального лота');
  return amount;
}
function closeVolumes(row, account, linked, mode, value) {
  if (!row?.__isPosition) return [];
  const amount=mode==='percent' ? percentVolume(row,value) : Number(value);
  const fraction=mode==='percent' ? Number(value)/100 : amount/Number(row.lots);
  const targets=[{...row,account:String(account)}];
  if (linked && row.mirrorAccount) {
    for (const account of row.mirrorAccounts || [row.mirrorAccount]) {
      const target=row.linkedPositions?.find(item=>String(item.account)===String(account));
      targets.push(target || {account:String(account),error:'Нет свежих данных связанной позиции'});
    }
  }
  return targets.map((target,index)=>{
    try {
      if (target.error) throw Error(target.error);
      const lots=Number(target.lots), step=Number(target.quantityStep), minimum=Number(target.minQty);
      const volume=index===0 ? amount : percentVolume(target,fraction*100);
      const remaining=Number((lots-volume).toFixed(8));
      if (!Number.isFinite(volume) || !(step>0 && minimum>0 && volume>=minimum-1e-8 && volume<lots-1e-8)
        || Math.abs(volume/step-Math.round(volume/step))>1e-7 || remaining<minimum-1e-8
        || Math.abs(remaining/step-Math.round(remaining/step))>1e-7
        || volume>Number(target.maxQty ?? Infinity)+1e-8) throw Error('Недопустимый объём закрытия или остатка');
      return {account:String(target.account),volume,remaining,step};
    } catch(error) {return {account:String(target.account),error:error.message};}
  });
}
module.exports={followers,percentVolume,closeVolumes};

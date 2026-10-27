const zone='America/New_York';
const clock=new Intl.DateTimeFormat('en-CA',{timeZone:zone,year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hourCycle:'h23'});
function parts(at) {return Object.fromEntries(clock.formatToParts(new Date(at)).filter(p=>p.type!=='literal').map(p=>[p.type,p.value]));}
function timeExitConfig(config={}) {
  const value=config.timeExit || {enabled:true,time:'15:00'};
  if (typeof value.enabled!=='boolean' || !/^(?:[01]\d|2[0-3]):[0-5]\d$/.test(value.time)) throw Error('Укажите время закрытия Нью-Йорка в формате ЧЧ:ММ');
  return {enabled:value.enabled,time:value.time,timeZone:zone,cancelPending:true};
}
function deadlineForNewTrade(config,now=Date.now()) {
  const value=timeExitConfig(config);
  if (!value.enabled) return null;
  if (!Number.isFinite(now)) throw Error('Не удалось определить текущее время');
  const p=parts(now);
  // Select a new immutable deadline once, before submitting the opening.
  // Existing commands and armed VPS rules never roll forward on retries.
  for (let day=0;day<2;day++) {
    const date=new Date(Date.UTC(Number(p.year),Number(p.month)-1,Number(p.day)+day)).toISOString().slice(0,10);
    const wall=Date.parse(`${date}T${value.time}:00Z`);
    const candidates=[4,5].map(offset=>wall+offset*3600000).filter(at=>{
      const q=parts(at);return `${q.year}-${q.month}-${q.day}`===date && `${q.hour}:${q.minute}`===value.time;
    });
    if (candidates.length!==1) throw Error('Это время в Нью-Йорке пропущено или повторяется при переводе часов. Выберите другое время.');
    if (candidates[0]>now) return {closeAtUtc:candidates[0],closeTimeZone:zone,closeLocalDate:date,closeLocalTime:value.time,cancelPendingAtDeadline:true};
  }
  throw Error('Не удалось выбрать ближайшее время закрытия');
}
module.exports={timeExitConfig,deadlineForNewTrade};

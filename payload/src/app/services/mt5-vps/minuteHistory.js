// On-demand chart reads only. Existing UI refreshes drive bounded retries.
function createMinuteHistory({load,changed=()=>{},now=Date.now}) {
  const rows=new Map();let closed=false;
  function get(symbol,{enabled=true,force=false}={}) {
    if(!symbol || !enabled || closed)return {data:null,loading:false,error:enabled?'':'Минутные бары недоступны на этом приёмнике'};
    let row=rows.get(symbol);
    if(!row){row={data:null,minute:-1,attemptMinute:-1,attempts:0,lastAttempt:-Infinity,retryAt:0,error:'',pending:null};rows.set(symbol,row);}
    const time=now(),minute=Math.floor(time/60000);
    if(row.attemptMinute!==minute){row.attemptMinute=minute;row.attempts=0;row.retryAt=0;}
    if(!row.pending && (force || row.minute!==minute) && time-row.lastAttempt>=1000
      && (force || row.attempts<3 && time>=row.retryAt)) {
      row.attempts++;row.lastAttempt=time;row.error='';
      row.pending=Promise.resolve().then(()=>load(symbol)).then(reply=>{
        if(reply?.status!=='ok' || !reply.data || !Array.isArray(reply.data.bars) || !reply.data.bars.length)
          throw Error(reply?.reason || 'MT5 пока не вернул минутные бары');
        row.data=reply.data;row.minute=minute;
      }).catch(error=>{row.error=error.message;row.retryAt=now()+2000;})
        .finally(()=>{row.pending=null;if(!closed)changed();});
    }
    return {data:row.data,loading:!!row.pending,error:row.error};
  }
  return {get,retry:symbol=>get(symbol,{force:true}),close(){closed=true;}};
}
module.exports={createMinuteHistory};

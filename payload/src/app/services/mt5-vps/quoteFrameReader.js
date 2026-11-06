// Bound each burst's synchronous work so command/UI callbacks can run between
// batches. Every frame still reaches the normal identity/freshness validator.
function createQuoteFrameReader(onFrame,onError,{
  schedule=setImmediate,cancel=clearImmediate,now=()=>performance.now(),wall=()=>Date.now(),
  maxFrames=8,maxWorkMs=4,maxBuffered=131072
}={}) {
  let buffer='',pending=null,closed=false;
  const chunks=[];
  function close() {
    closed=true;buffer='';chunks.length=0;
    if(pending!==null)cancel(pending);
    pending=null;
  }
  function drain() {
    pending=null;
    if(closed)return;
    const started=now();let count=0,boundary;
    try {
      while((boundary=buffer.indexOf('\n'))>=0) {
        const consumed=boundary+1,line=buffer.slice(0,boundary);
        buffer=buffer.slice(consumed);
        let remaining=consumed,arrival;
        while(remaining && chunks.length) {
          const part=chunks[0],used=Math.min(remaining,part.length);
          arrival=part;
          part.length-=used;remaining-=used;
          if(!part.length)chunks.shift();
        }
        if(line)onFrame(JSON.parse(line),{
          receivedAt:arrival.receivedAt,arrivalPerf:arrival.arrivalPerf,
          processingDelayMs:Math.max(0,now()-arrival.arrivalPerf)
        });
        if(closed)return;
        if(++count>=maxFrames || now()-started>=maxWorkMs)break;
      }
      if(buffer.includes('\n'))pending=schedule(drain);
    } catch(error) {close();onError(error);}
  }
  return {
    push(chunk) {
      if(closed)return;
      try {
        buffer+=chunk;
        if(buffer.length>maxBuffered)throw Error('Quote stream frame too large');
        if(chunk.length)chunks.push({length:chunk.length,receivedAt:wall(),arrivalPerf:now()});
        if(pending===null)drain();
      } catch(error) {close();onError(error);}
    },
    close
  };
}
module.exports={createQuoteFrameReader};

// Timing only: never stores prices, credentials or trading instructions.
function createQuoteContinuity() {
  let previousCapture=null,previousArrival=null,previousStream=null;
  const counters={frames:0,expired:0,maxArrivalGapMs:0,maxCaptureGapMs:0,maxFrameAgeMs:0};
  return {
    observe(capturedAt,receivedAt,arrivalPerf,accepted,streamId=0,detail) {
      const sameStream=previousStream!==null && previousStream===streamId;
      const row={capturedAt,receivedAt,accepted,sameStream,
        arrivalGapMs:previousArrival===null ? 0 : Math.max(0,arrivalPerf-previousArrival),
        captureGapMs:previousCapture===null || !sameStream ? 0 : Math.max(0,capturedAt-previousCapture),
        frameAgeMs:Math.max(0,receivedAt-capturedAt)};
      if(detail) {
        const numeric=(key,value)=>{if(Number.isFinite(value) && value>=0)row[key]=Number(value.toFixed(3));};
        numeric('processingDelayMs',detail.processingDelayMs);numeric('frameWorkMs',detail.frameWorkMs);
        if(Number.isFinite(detail.publishedAt))numeric('captureToPublishMs',detail.publishedAt-capturedAt);
        if(Number.isFinite(detail.writeStartedAt)) {
          numeric('writeToArrivalMs',receivedAt-detail.writeStartedAt);
          if(Number.isFinite(detail.publishedAt))numeric('publishToWriteMs',detail.writeStartedAt-detail.publishedAt);
        }
        const stages={};
        for(const key of ['nativeMs','accountInfoMs','terminalInfoMs','rpcMs','lockWaitMs','sendMs','producerDelayMs']) {
          const value=detail.quoteTiming?.[key];if(Number.isFinite(value) && value>=0)stages[key]=value;
        }
        if(Object.keys(stages).length)row.sourceTiming=stages;
      }
      previousCapture=capturedAt;previousArrival=arrivalPerf;previousStream=streamId;
      counters.frames++;if (!accepted)counters.expired++;
      for (const [key,value] of [['maxArrivalGapMs',row.arrivalGapMs],['maxCaptureGapMs',row.captureGapMs],['maxFrameAgeMs',row.frameAgeMs]]) counters[key]=Math.max(counters[key],value);
      return row;
    },
    summary:()=>({...counters})
  };
}
module.exports={createQuoteContinuity};

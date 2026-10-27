// Timing only: never stores prices, credentials or trading instructions.
function createQuoteContinuity() {
  let previousCapture=null,previousArrival=null,previousStream=null;
  const counters={frames:0,expired:0,maxArrivalGapMs:0,maxCaptureGapMs:0,maxFrameAgeMs:0};
  return {
    observe(capturedAt,receivedAt,arrivalPerf,accepted,streamId=0) {
      const sameStream=previousStream!==null && previousStream===streamId;
      const row={capturedAt,receivedAt,accepted,sameStream,
        arrivalGapMs:previousArrival===null ? 0 : Math.max(0,arrivalPerf-previousArrival),
        captureGapMs:previousCapture===null || !sameStream ? 0 : Math.max(0,capturedAt-previousCapture),
        frameAgeMs:Math.max(0,receivedAt-capturedAt)};
      previousCapture=capturedAt;previousArrival=arrivalPerf;previousStream=streamId;
      counters.frames++;if (!accepted)counters.expired++;
      for (const [key,value] of [['maxArrivalGapMs',row.arrivalGapMs],['maxCaptureGapMs',row.captureGapMs],['maxFrameAgeMs',row.frameAgeMs]]) counters[key]=Math.max(counters[key],value);
      return row;
    },
    summary:()=>({...counters})
  };
}
module.exports={createQuoteContinuity};

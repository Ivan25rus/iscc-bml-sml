// Deadline conversion only. These timestamps never make a price usable.
const {performance}=require('node:perf_hooks');
function rememberServerClock(adapter,capturedAt,at=performance.now()) {
  if(Number.isFinite(capturedAt))adapter.entryIntentClock={capturedAt,at};
}
function intentExpiry(adapter,ttl,wall=Date.now(),at=performance.now()) {
  const clock=adapter.entryIntentClock,age=at-clock?.at;
  // An authenticated capture is a lower bound for the VPS wall clock.
  // Advance with monotonic time, so a Mac clock correction cannot extend it.
  if(clock && Number.isFinite(age) && age>=0 && age<=60000)return clock.capturedAt+age+ttl;
  // A cold/disconnected Mac still sends immediately. Reserve headroom for
  // small clock skew instead of increasing the receiver's maximum deadline.
  return wall+ttl-1500;
}
module.exports={rememberServerClock,intentExpiry};

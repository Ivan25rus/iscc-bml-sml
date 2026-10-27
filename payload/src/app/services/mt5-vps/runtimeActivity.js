// Keep live account monitoring running without depending on an RDP window.
// This assertion allows display sleep and is released with the app.
function createRuntimeActivity(blocker) {
  let id=null;
  return {
    start(enabled) {
      if (enabled && id===null && blocker?.start) id=blocker.start('prevent-app-suspension');
    },
    stop() {
      if (id!==null) {blocker.stop(id);id=null;}
    }
  };
}
module.exports={createRuntimeActivity};

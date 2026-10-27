let cached;
function readBuildInfo() {
  if(!cached) {
    try {cached=JSON.parse(require('fs').readFileSync(require('path').join(__dirname,'../../build-info.json'),'utf8'));}
    catch {cached={buildId:'development',sourceSha256:null};}
  }
  return {...cached};
}
module.exports={readBuildInfo};

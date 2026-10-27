// Isolate the pure rule factory from the recipient's Electron configuration.
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {createRequire}=require('node:module');
const filename=path.resolve(__dirname,'../payload/src/app/services/tradeRules/index.js');
const localRequire=createRequire(filename),context={module:{exports:{}},
  require(name){return name==='../../config/load' ? ()=>({}) : localRequire(name);}};
vm.runInNewContext(fs.readFileSync(filename,'utf8'),context,{filename});
module.exports=context.module.exports;

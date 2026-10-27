const {gunzip}=require('zlib');
// Bound both compressed input and decompressed output for read-only replies.
const MAX_REPLY_BYTES=4*1024*1024;

function decodeReadGzip(bytes) {
  if(!Buffer.isBuffer(bytes) || bytes.length>MAX_REPLY_BYTES)return Promise.reject(Error('Слишком большой ответ MT5'));
  return new Promise((resolve,reject)=>gunzip(bytes,{maxOutputLength:MAX_REPLY_BYTES},(error,decoded)=>{
    if(error)reject(Error('Повреждённый или слишком большой сжатый ответ MT5'));
    else resolve(decoded.toString('utf8'));
  }));
}
module.exports={decodeReadGzip,MAX_REPLY_BYTES};

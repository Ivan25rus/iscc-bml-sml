const MAX_NAME_LENGTH=32;

function normalizeAccountNames(value={}) {
  if(!value || typeof value!=='object' || Array.isArray(value))throw Error('Неверный список имён счетов');
  return Object.fromEntries(Object.entries(value).map(([id,raw])=>{
    if(!/^[1-9][0-9]{0,15}$/.test(id) || typeof raw!=='string')throw Error('Неверный номер или имя счёта');
    const name=raw.trim();
    if(name.length>MAX_NAME_LENGTH || /[\p{Cc}\p{Cf}\p{Zl}\p{Zp}]/u.test(name))
      throw Error('Имя счёта — до '+MAX_NAME_LENGTH+' символов в одну строку');
    return [id,name];
  }).filter(([,name])=>name));
}

function accountName(names,account) {
  const id=String(account);
  return Object.hasOwn(names,id) ? names[id] : id;
}

module.exports={MAX_NAME_LENGTH,normalizeAccountNames,accountName};

const fs = require('fs');
const net = require('net');
const tls = require('tls');

// endpoint remains the authenticated server identity. Only the TCP destination
// changes for a private tunnel; trust, mTLS and hostname checks stay mandatory.
function connectionURL(config) {
  const url = new URL(config.endpoint);
  if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash) throw Error('MT5 VPS требует HTTPS');
  if (config.connectAddress !== undefined) {
    const address = config.connectAddress;
    const octets = typeof address === 'string' ? address.split('.').map(Number) : [];
    if (net.isIP(address || '') !== 4 || !(octets[0] === 10 || octets[0] === 127
        || (octets[0] === 172 && octets[1] >= 16 && octets[1] <= 31)
        || (octets[0] === 192 && octets[1] === 168))) throw Error('Адрес туннеля MT5 должен быть частным IPv4');
    if (!config.clientCertFile || !config.clientKey) throw Error('Туннель MT5 требует клиентский сертификат');
    url.hostname = address;
  }
  return url;
}

function resolveSecureConfig(config, storage) {
  if (!config.secureAuthFile) return {...config};
  storage ||= require('electron').safeStorage;
  if (!storage?.isEncryptionAvailable()) throw Error('Недоступна Связка ключей macOS; подключение MT5 остановлено');
  let secrets;
  try { secrets = JSON.parse(storage.decryptString(fs.readFileSync(config.secureAuthFile))); }
  catch { throw Error('Не удалось открыть защищённые ключи MT5'); }
  if (typeof secrets.token !== 'string' || secrets.token.length < 32 || !secrets.clientKey || !config.clientCertFile) {
    throw Error('Защищённые ключи MT5 неполные');
  }
  const resolved = {...config};
  Object.defineProperties(resolved, {
    token:{value:secrets.token,enumerable:false},
    clientKey:{value:secrets.clientKey,enumerable:false}
  });
  return resolved;
}

function tlsOptions(config) {
  const options = {ca:fs.readFileSync(config.caFile), rejectUnauthorized:true};
  if (config.connectAddress !== undefined) {
    connectionURL(config); // Validate before creating any socket.
    const identity = new URL(config.endpoint).hostname;
    options.servername = net.isIP(identity) ? '' : identity;
    options.checkServerIdentity = (_connectionAddress, certificate) => tls.checkServerIdentity(identity, certificate);
  }
  if (config.clientCertFile) {
    if (!config.clientKey) throw Error('Отсутствует защищённый ключ клиентского сертификата');
    options.cert = fs.readFileSync(config.clientCertFile);
    options.key = config.clientKey;
  }
  return options;
}

function prepareSavedConfig(config, storage) {
  if (!config.secureAuthFile) return config;
  storage ||= require('electron').safeStorage;
  const current=resolveSecureConfig(config,storage);
  if (config.token && config.token!==current.token) {
    const encrypted=storage.encryptString(JSON.stringify({token:config.token,clientKey:current.clientKey}));
    const temporary=config.secureAuthFile+'.incoming';
    fs.writeFileSync(temporary,encrypted,{mode:0o600});
    fs.renameSync(temporary,config.secureAuthFile);
  }
  const saved={...config}; delete saved.token; delete saved.clientKey;
  return saved;
}

module.exports = {resolveSecureConfig,tlsOptions,prepareSavedConfig,connectionURL};

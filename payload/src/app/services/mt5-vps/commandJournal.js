const fs = require('fs');
const path = require('path');
const { createHash } = require('crypto');
function uncertain(result) {
  if (result.status !== 'unknown') return false;
  const primary = result.accountResults?.[result.primaryAccount];
  return !primary || primary.status === 'unknown';
}

// Write intent before transmission. Unknown commands block their ticket across restarts.
class CommandJournal {
  constructor(file, {asyncWrites=false}={}) {
    this.file = file;
    this.asyncWrites = asyncWrites;
    if(asyncWrites)require('../journalWriter').warm();
    this.records = fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, 'utf8')) : [];
    if (!Array.isArray(this.records)) throw Error('Повреждён журнал команд MT5');
    if(asyncWrites)this.prepared=require('../journalWriter').prepareJournal(file).catch(()=>{});
  }
  prepareEntryPlans() {
    if(this.asyncWrites)return require('../journalWriter').prepareJournal(this.file+'.entry-plans.json').catch(()=>{});
  }
  save(changes) {
    if (this.asyncWrites) return require('../journalWriter').writeJournal(this.file,changes,changes ? undefined : this.records);
    fs.mkdirSync(path.dirname(this.file), {recursive:true});
    const temporary = this.file + '.tmp';
    const fd = fs.openSync(temporary, 'w', 0o600);
    try { fs.writeFileSync(fd, JSON.stringify(this.records)); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
    fs.renameSync(temporary, this.file);
    const directory = fs.openSync(path.dirname(this.file), 'r');
    try { fs.fsyncSync(directory); } finally { fs.closeSync(directory); }
  }
  async run(operation, payload, send) {
    const {expiresAt, ...core} = payload;
    const digest = createHash('sha256').update(JSON.stringify({operation,...core})).digest('hex');
    const previous = this.records.find(x => x.id === payload.id);
    if (previous) {
      if (previous.digest !== digest) return {status:'rejected',reason:'Номер команды уже использован с другими параметрами'};
      return previous.result;
    }
    if (this.records.some(x => x.ticket === payload.ticket && uncertain(x.result))) {
      return {status:'unknown',reason:'Есть неподтверждённая команда для этой позиции. Проверь MT5 и журнал команд; повтор заблокирован.'};
    }
    const record = {id:payload.id, operation, ticket:payload.ticket, symbol:payload.symbol, payload:core,
      digest, at:Date.now(), result:{status:'unknown',reason:'Команда сохранена перед отправкой; требуется подтверждение MT5'}};
    this.records.push(record);
    let journalTiming;
    try { journalTiming=await this.save([record]); } catch (error) { this.records.splice(this.records.indexOf(record),1); throw error; }
    try { const result = await send(journalTiming); if(!record.revokedBy)record.result=result; }
    catch (error) { if(!record.revokedBy)record.result = {status:'unknown',reason:error.message}; }
    record.updatedAt = Date.now();
    try { await this.save([record]); } catch {
      record.result = {status:'unknown',reason:'Не удалось сохранить ответ. Проверь MT5; повтор заблокирован.'};
    }
    return record.result;
  }
  async reconcile(receipt, {ticket} = {}) {
    for (const record of this.records.filter(x => x.result.status === 'unknown' && (ticket===undefined || x.ticket===ticket))) {
      const result = await receipt(record.id);
      if (result && ['ok','rejected'].includes(result.status)) {
        record.result = result; record.updatedAt = Date.now(); await this.save([record]);
      }
    }
  }
}
module.exports = {CommandJournal};

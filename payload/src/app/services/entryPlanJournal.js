const fs = require('node:fs');
const path = require('node:path');

const isEntryPlan = file => file.endsWith('.entry-plans.json');
const changeFile = file => file + '.changes';

function loadEntryPlans(file) {
  const snapshot = fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, 'utf8')) : [];
  if (!Array.isArray(snapshot)) throw Error('Повреждён журнал команд MT5');
  const records = new Map(snapshot.map(row => [row.id, row]));
  let bytes;
  try { bytes = fs.readFileSync(changeFile(file)); }
  catch (error) {
    if (error.code !== 'ENOENT') throw error;
    return {records, validBytes:0, size:0};
  }
  // A complete line is one batch. A crash during append can only tear the final line.
  const validBytes = bytes.lastIndexOf(10) + 1;
  if (validBytes) {
    for (const line of bytes.subarray(0, validBytes - 1).toString('utf8').split('\n')) {
      const batch = JSON.parse(line);
      if (!Array.isArray(batch) || batch.some(row => typeof row?.id !== 'string' || !row.result))
        throw Error('Повреждён журнал команд MT5');
      for (const row of batch) records.set(row.id, row);
    }
  }
  return {records, validBytes, size:bytes.length};
}

function changeBatch(changes) {
  if (!Array.isArray(changes) || !changes.length) throw Error('Missing entry plan journal changes');
  return JSON.stringify(changes) + '\n';
}

function appendEntryPlanBatch(file, state, text) {
  const directory = path.dirname(file);
  if (!state.directorySynced) fs.mkdirSync(directory, {recursive:true, mode:0o700});
  const fd = fs.openSync(changeFile(file), 'a', 0o600);
  try {
    if (state.validBytes !== state.size) fs.ftruncateSync(fd, state.validBytes);
    fs.writeFileSync(fd, text);
    fs.fsyncSync(fd);
  } finally { fs.closeSync(fd); }
  if (!state.directorySynced) {
    const parent = fs.openSync(directory, 'r');
    try { fs.fsyncSync(parent); } finally { fs.closeSync(parent); }
    state.directorySynced = true;
  }
  state.size = state.validBytes += Buffer.byteLength(text);
}

function appendEntryPlansSync(file, changes) {
  appendEntryPlanBatch(file, loadEntryPlans(file), changeBatch(changes));
}

module.exports = {isEntryPlan, changeFile, loadEntryPlans, changeBatch, appendEntryPlanBatch, appendEntryPlansSync};

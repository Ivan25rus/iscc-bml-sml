const fs = require('fs').promises;
const path = require('path');

// One ordered queue per file. Capture the bytes before yielding so an intent
// cannot accidentally persist a later receipt instead of its own snapshot.
const queues = new Map();
function writeDurable(file, text) {
  const previous = queues.get(file) || Promise.resolve();
  const operation = previous.catch(() => {}).then(async () => {
    const directory = path.dirname(file), temporary = file + '.tmp';
    await fs.mkdir(directory, {recursive:true, mode:0o700});
    const handle = await fs.open(temporary, 'w', 0o600);
    try { await handle.writeFile(text); await handle.sync(); }
    finally { await handle.close(); }
    await fs.rename(temporary, file);
    const parent = await fs.open(directory, 'r');
    try { await parent.sync(); } finally { await parent.close(); }
  });
  queues.set(file, operation);
  operation.finally(() => { if (queues.get(file) === operation) queues.delete(file); }).catch(() => {});
  return operation;
}
module.exports = {writeDurable};

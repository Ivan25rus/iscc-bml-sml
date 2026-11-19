'use strict';
const path = require('path');
const { fileURLToPath } = require('url');

const READ_WARNINGS = 'candle-renderer:level-warnings';
const WARNINGS_CHANGED = 'candle-renderer:level-warnings-changed';

function isPanel(url) {
  try { return fileURLToPath(url) === path.resolve(__dirname, '../../index.html'); }
  catch { return false; }
}

function relayWarnings(ipcMain, getState) {
  const panels = new Map();
  ipcMain.handle(READ_WARNINGS, ({ sender, senderFrame }) => {
    if (senderFrame !== sender.mainFrame || !isPanel(senderFrame?.url)) return null;
    const state = getState();
    panels.set(sender, JSON.stringify(state));
    return state;
  });
  return {
    publish() {
      if (!panels.size) return;
      const state = getState(), signature = JSON.stringify(state);
      for (const [panel, previous] of panels) {
        if (panel.isDestroyed() || !isPanel(panel.getURL())) { panels.delete(panel); continue; }
        if (signature === previous) continue;
        panel.send(WARNINGS_CHANGED, state);
        panels.set(panel, signature);
      }
    },
    stop() { ipcMain.removeHandler(READ_WARNINGS); panels.clear(); }
  };
}

module.exports = { relayWarnings, READ_WARNINGS, WARNINGS_CHANGED };

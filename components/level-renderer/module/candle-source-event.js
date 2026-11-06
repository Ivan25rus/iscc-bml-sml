// Only an outgoing TV storage mutation can prove an explicit local deletion.
// chart_id is a pane identifier; layout_id is the selected working layout.
function removalScope(record) {
  if (record?.event !== 'http_request' || record.dir !== 'out' || record.method !== 'PUT'
    || typeof record.host !== 'string' || !/^(?:[a-z0-9-]+\.)*tradingview\.(?:com|ru)$/i.test(record.host)
    || typeof record.path !== 'string' || !record.path.startsWith('/')) return null;
  try {
    const url = new URL(record.path, 'https://' + record.host);
    if (url.host !== record.host || !/^\/charts-storage\/user\/sources(?:\/|$)/.test(url.pathname)) return null;
    const ids = url.searchParams.getAll('layout_id');
    return ids.length === 1 && /^[A-Za-z0-9_-]{1,128}$/.test(ids[0]) ? ids[0] : null;
  } catch { return null; }
}
module.exports = { removalScope };

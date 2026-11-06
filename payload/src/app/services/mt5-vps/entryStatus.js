// Classification changes display only; uncertainty never releases an entry.
function readErrorKind(error) {
  const reason=String(error?.message || error || '');
  if(/Точный журнал|не подтверждён номер|не подтверждены номера|не подтвердил объёмы/i.test(reason))return 'journal';
  if(/ECONN|EPIPE|ENET|EHOST|ETIMEDOUT|TLS|certificate|socket|timeout|timed out|время ожидания истекло|ответ соединения прерван|Нет свежего статуса VPS|Нет связи с VPS|терминал недоступен/i.test(reason))return 'connection';
  return 'checking';
}
function readErrorLabel(entry) {
  if(entry?._lastConfirmedControl?.status==='ok' && entry._lastConfirmedControl.entryId===entry.id
    && entry._lastConfirmedControl.operation==='entry-protection' && entry.phase!=='unknown'
    && !entry.protectionPending && !entry.closurePending)
    return 'Защита подтверждена · обновляем состояние VPS';
  const kind=readErrorKind(entry?._readError);
  return {journal:'Ошибка сверки журнала',connection:'Нет связи с VPS',checking:'Проверяем исполнение и закрытие'}[kind]
    || 'Проверяем исполнение и закрытие';
}
module.exports={readErrorKind,readErrorLabel};

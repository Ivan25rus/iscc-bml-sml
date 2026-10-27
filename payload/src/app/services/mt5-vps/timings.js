function ms(value) { return Number.isFinite(value) && value>=0 ? `${Math.round(value)} мс` : '—'; }
function accountTimingLines(result) {
  if (!result) return [];
  if (!result.accountResults) return (result.raw?.results || []).flatMap(child=>accountTimingLines(child.result));
  const accounts = result.accountResults || {};
  return Object.entries(accounts).sort(([a],[b])=>a===String(result.primaryAccount) ? -1 : b===String(result.primaryAccount) ? 1 : 0).flatMap(([account,value])=>{
    const timing=value?.timings;
    if (!timing) return [];
    const lines=[`Счёт ${account}: ответ MT5 ${ms(timing.server?.mt5Ms)} · на VPS ${ms(timing.vpsFromReceiveMs ?? timing.server?.totalMs)} · очередь ${ms(timing.server?.queueMs)}`];
    if (Number.isFinite(timing.server?.accountInfoMs)) {
      const s=timing.server;
      lines.push(`Подготовка: счёт ${ms(s.accountInfoMs)} · терминал ${ms(s.terminalInfoMs)} · чтение журнала ${ms(s.journalLookupMs)} · сборка команды ${ms(s.requestBuildMs)} · прочее ${ms(s.prepareOtherMs)}`);
      lines.push(`Запись перед отправкой ${ms(s.ledgerBeforeMs)} · запись ответа ${ms(s.ledgerAfterMs)} · проверка ответа ${ms(s.replyValidationMs)}${timing.workerRpc ? ' · обмен со вторым процессом '+ms(timing.workerRpc.totalMs)+' · ожидание доступа '+ms(timing.workerRpc.lockWaitMs) : ''}`);
      if (Number.isFinite(s.targetReadMs)) lines.push(`Позиция ${ms(s.targetReadMs)} · параметры инструмента ${ms(s.symbolInfoMs)} · котировка ${ms(s.quoteReadMs)}`);
    }
    return lines;
  });
}
function timingLines(result) {
  if (!result) return [];
  const lines=accountTimingLines(result);
  if (!lines.length && result.timings?.server) lines.push(`Ответ MT5: ${ms(result.timings.server.mt5Ms)} · обработка на VPS: ${ms(result.timings.server.totalMs)}`);
  if (result.timings && Number.isFinite(result.timings.macClickToResultMs)) lines.push(`От нажатия до ответа: ${ms(result.timings.macClickToResultMs)} · подготовка на Mac ${ms(result.timings.macBeforeSendMs)} · обмен с VPS ${ms(result.timings.transport?.totalMs)}`);
  if (Number.isFinite(result.timings?.targetLookupMs)) {
    const t=result.timings;
    lines.push(`На Mac: список позиций ${ms(t.managementReadMs)} · выбор позиции ${ms(t.targetLookupMs)} · статус ${ms(t.healthReadMs)} · квитанции ${ms(t.receiptReadMs)} · журнал перед отправкой ${ms(t.journalBeforeSendMs)}`);
  }
  return lines;
}
module.exports={timingLines,accountTimingLines};

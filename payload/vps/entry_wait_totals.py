"""Account-local bounded stage totals; no threads, I/O or MT5 calls."""
import math


def _finite(value):
    try:
        return not isinstance(value, bool) and isinstance(value, (int, float)) and math.isfinite(value)
    except (OverflowError, ValueError, TypeError):
        return False


def record_common_stop_timing(timing, result, elapsed_ms, receipt_only=False):
    """Copy already returned timings, never position or protection payloads."""
    if not isinstance(result, dict) or not _finite(elapsed_ms) or elapsed_ms < 0:
        return
    previous = timing.get('commonStop', {})
    observed = dict(commandCalls=previous.get('commandCalls', 0)+(not receipt_only),
                    receiptChecks=previous.get('receiptChecks', 0)+receipt_only)
    for key in ('lastCommand', 'lastReceipt'):
        if key in previous:
            observed[key] = previous[key]
    sample = dict(coordinatorElapsedMs=round(elapsed_ms, 3),
                  status=str(result.get('status', 'unknown'))[:16])
    if _finite(result.get('elapsedMs')) and result['elapsedMs'] >= 0:
        sample['nativeElapsedMs'] = result['elapsedMs']
    fields = result.get('protectionTiming', {})
    if isinstance(fields, dict):
        sample['stages'] = {key:round(fields[key], 3) for key in (
            'accountCheckMs', 'positionReadMs', 'validationMs', 'riskCalculationMs',
            'intentCommitMs', 'modifyMs', 'finalReadMs')
            if _finite(fields.get(key)) and fields[key] >= 0}
    observed['lastReceipt' if receipt_only else 'lastCommand'] = sample
    timing['commonStop'] = observed


class EntryWaitTotals:
    MAX_STAGES = 16
    MAX_REASONS = 8
    TERMINAL = frozenset(('completed', 'stopped', 'rejected'))

    def __init__(self):
        self.data = dict(schema='entry-wait-totals-v1', observedMs=0., stages={}, waits=[])
        self.current = None
        self.at = None
        self.wait_rows = {}

    def _stage(self, phase):
        rows = self.data['stages']
        key = phase if phase in rows or len(rows) < self.MAX_STAGES-1 else 'other'
        return rows.setdefault(key, dict(durationMs=0., segments=0))

    def _wait(self, phase, reason):
        if not (phase.startswith('waiting-') or phase in ('limit', 'paused', 'reconciling')):
            return None
        key = (phase, reason)
        if key not in self.wait_rows:
            if len(self.wait_rows) >= self.MAX_REASONS-1:
                key = ('other', 'Additional waiting reasons')
            if key not in self.wait_rows:
                row = dict(phase=key[0], reason=key[1], durationMs=0., segments=0)
                self.wait_rows[key] = row
                self.data['waits'].append(row)
        return self.wait_rows[key]

    def observe(self, phase, reason, now_ms, quote_age_ms=None):
        """Account actor owns this object. Publish data with its normal state.

        Completed/stopped idle time is excluded. Repeated observations accrue
        elapsed time without growing a ring or creating a log event each tick.
        Reasons must come from the caller's existing sanitized diagnostics.
        """
        if (not isinstance(phase, str) or not phase or not isinstance(reason, str)
                or not _finite(now_ms)):
            return
        phase, reason = phase[:48], reason[:160]
        if self.at is not None and now_ms < self.at:
            return
        if self.current is not None:
            previous_phase, previous_reason = self.current
            elapsed = 0. if previous_phase in self.TERMINAL else now_ms-self.at
            self.data['observedMs'] = round(self.data['observedMs']+elapsed, 3)
            stage = self._stage(previous_phase)
            stage['durationMs'] = round(stage['durationMs']+elapsed, 3)
            waiting = self._wait(previous_phase, previous_reason)
            if waiting is not None:
                waiting['durationMs'] = round(waiting['durationMs']+elapsed, 3)
        if self.current != (phase, reason):
            self._stage(phase)['segments'] += 1
            waiting = self._wait(phase, reason)
            if waiting is not None:
                waiting['segments'] += 1
        else:
            waiting = self._wait(phase, reason)
        if waiting is not None and _finite(quote_age_ms):
            waiting['quoteSamples'] = waiting.get('quoteSamples', 0)+1
            waiting['minQuoteAgeMs'] = round(min(waiting.get('minQuoteAgeMs', quote_age_ms), quote_age_ms), 3)
            waiting['maxQuoteAgeMs'] = round(max(waiting.get('maxQuoteAgeMs', quote_age_ms), quote_age_ms), 3)
        self.current, self.at = (phase, reason), now_ms

"""Opening-only limits for the New York trading day."""
import json
import math
from datetime import datetime, timedelta, timezone


MAX_SESSION_ENTRIES = 3
DAILY_LOSS_FRACTION = 0.018
DAILY_LOSS_FRACTION_BY_ACCOUNT = {}
SESSION_OPEN = (9, 30)
CLOSE_HOUR = 16
MAX_ENTRY_REASON = 'Нью-Йорк 09:30–16:00: исчерпан лимит трёх входов'


def local_time(stamp, zone):
    return datetime.fromtimestamp(stamp, timezone.utc).astimezone(zone)


def session_bounds(stamp, zone):
    local = local_time(stamp, zone)
    start = local.replace(hour=SESSION_OPEN[0], minute=SESSION_OPEN[1], second=0, microsecond=0).timestamp()
    end = local.replace(hour=CLOSE_HOUR, minute=0, second=0, microsecond=0).timestamp()
    return (start * 1000, end * 1000) if local.weekday() < 5 and start <= stamp < end else None


def risk_day_start(stamp, zone):
    local = local_time(stamp, zone)
    close = local.replace(hour=CLOSE_HOUR, minute=0, second=0, microsecond=0)
    return (close - timedelta(days=1) if local < close else close).timestamp()


def limits_policy():
    close = f'{CLOSE_HOUR:02d}:00'
    daily_percent = round(DAILY_LOSS_FRACTION * 100, 2)
    return dict(timeZone='America/New_York', sessionStart=f'{SESSION_OPEN[0]:02d}:{SESSION_OPEN[1]:02d}',
                sessionEnd=close, maxSessionEntries=MAX_SESSION_ENTRIES,
                dailyRiskPercent=daily_percent, dailyRiskReset=close, offSessionRiskPercent=daily_percent,
                dailyRiskPercentByAccount={account: fraction * 100
                                           for account, fraction in DAILY_LOSS_FRACTION_BY_ACCOUNT.items()})


def entry_consumes_slot(state):
    if state['phase'] != 'stopped' or any(a.get('filledVolume', 0) > 0 for a in state['accounts'].values()):
        return True
    for child in state['children']:
        if child['state'] not in ('rejected', 'cancelled') or child.get('positions'):
            return True
        receipt = child.get('reconciledResult', child.get('result', {}))
        results = receipt.get('accountResults', {'entry': receipt})
        if any(result.get('filledVolume', 0) > 0 or result.get('status') == 'unknown' for result in results.values()):
            return True
    return False


class SessionEntries:
    def __init__(self, storage, zone, projection):
        self.storage, self.zone, self.projection = storage, zone, projection
        with storage.lock, storage.db:
            storage.db.execute('CREATE TABLE IF NOT EXISTS session_entry_starts (id TEXT PRIMARY KEY NOT NULL, created REAL NOT NULL)')
            storage.db.execute('CREATE INDEX IF NOT EXISTS session_entries_started ON session_entry_starts(created)')
            # The state JSON changes during acquisition. Keep the immutable start
            # time apart so a part does not update any session limit index.
            storage.db.execute("""INSERT OR IGNORE INTO session_entry_starts
                SELECT id,json_extract(state,'$.createdAt') FROM chunk_entries
                WHERE id NOT IN (SELECT id FROM session_entry_starts) AND json_type(state,'$.createdAt') IN ('integer','real')""")
            storage.db.execute('CREATE TABLE IF NOT EXISTS direct_opening_slots (id TEXT PRIMARY KEY, created REAL NOT NULL)')
            storage.db.execute('CREATE INDEX IF NOT EXISTS direct_opening_slots_started ON direct_opening_slots(created)')

    def record(self, identifier, stamp):
        # The caller commits this with the admission, under the same lock.
        self.storage.db.execute('INSERT INTO session_entry_starts VALUES (?,?)', (identifier, stamp * 1000))

    def check(self, stamp):
        bounds = session_bounds(stamp, self.zone)
        if bounds is None:
            return False
        with self.storage.lock:
            consumed = self.storage.db.execute('SELECT count(*) FROM direct_opening_slots WHERE created>=? AND created<?', bounds).fetchone()[0]
            if consumed >= MAX_SESSION_ENTRIES:
                raise ValueError(MAX_ENTRY_REASON)
            rows = self.storage.db.execute('SELECT entry.id,entry.state FROM session_entry_starts AS started '
                'JOIN chunk_entries AS entry ON entry.id=started.id WHERE started.created>=? AND started.created<?', bounds).fetchall()
        for identifier, raw in rows:
            consumed += entry_consumes_slot(self.projection(identifier) or json.loads(raw))
            if consumed >= MAX_SESSION_ENTRIES:
                raise ValueError(MAX_ENTRY_REASON)
        return True

    def reserve_direct(self, identifier, stamp):
        with self.storage.lock:
            existing = self.storage.db.execute('SELECT 1 FROM direct_opening_slots WHERE id=?', (identifier,)).fetchone()
        if existing:
            return False
        self.check(stamp)
        with self.storage.lock, self.storage.db:
            self.storage.db.execute('INSERT INTO direct_opening_slots VALUES (?,?)', (identifier, stamp * 1000))
        return True

    def release_direct(self, identifier):
        with self.storage.lock, self.storage.db:
            self.storage.db.execute('DELETE FROM direct_opening_slots WHERE id=?', (identifier,))


def daily_pnl(mt5, deals, balance):
    movements, trading = [], []
    credit = getattr(mt5, 'DEAL_TYPE_CREDIT', 3)
    cash_types = {getattr(mt5, 'DEAL_TYPE_' + name, default) for name, default in (
        ('BALANCE', 2), ('CORRECTION', 5), ('BONUS', 6))}
    for deal in deals:
        if deal.type == credit:
            continue  # MT5 reports credit independently from the balance.
        net = math.fsum(getattr(deal, field, 0.) for field in ('profit', 'commission', 'swap', 'fee'))
        if not math.isfinite(net):
            raise ValueError('Дневной риск: некорректная сумма в истории MT5')
        movements.append(net)
        if deal.type not in cash_types:
            trading.append(net)
    start_balance = balance - math.fsum(movements)
    if not math.isfinite(start_balance) or start_balance <= 0:
        raise ValueError('Дневной риск: не подтверждён баланс на начало дня')
    return start_balance, math.fsum(trading)


def check_daily_budget(start_balance, realized, exposure, requested, *, account=None):
    if not (math.isfinite(exposure) and exposure >= 0 and math.isfinite(requested) and requested > 0):
        raise ValueError('Дневной риск: не подтверждён риск нового входа или открытых позиций')
    fraction = DAILY_LOSS_FRACTION_BY_ACCOUNT.get(str(account), DAILY_LOSS_FRACTION)
    available = start_balance * fraction + realized - exposure
    if requested > available + 1e-8:
        percent = f'{fraction:.1%}'.replace('.', ',')
        raise ValueError(f'Дневной риск {percent}: доступно {max(0., available):.2f}, вход требует {requested:.2f} в валюте счёта')

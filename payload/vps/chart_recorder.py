"""Select the chart recorder and publish completed images to the Trade Hub inbox."""
import json
import hashlib
import os
from pathlib import Path
import re
import shutil
import sqlite3
from contextlib import closing
import threading
import time
import uuid
from datetime import datetime


CAPABILITY = 'chart-recorder-v1'
ROUTE = 'route.tsv'
ROLE = 'ISCC_CHART_RECORDER'
ACCOUNT = re.compile(r'[1-9][0-9]{0,19}')
POSITION = ACCOUNT
REVISION = re.compile(r'[a-f0-9]{32}')
SHOT = re.compile(r'([1-9][0-9]{0,19})_\d+(?:_exit_\d+)?')
WORKERS = ('levels', 'USTEC', 'SPX500')
CAPTURE_SYMBOLS = (*WORKERS[1:], 'XAUUSD')
TRADEHUB_ROLES_CAPABILITY = 'tradehub-roles-v1'
TRADEHUB_ROLES_FILE = 'tradehub-roles.json'


def read_route(root):
    raw = (root / ROUTE).read_bytes()
    if len(raw) > 256:
        raise ValueError('Invalid chart recorder route')
    header, revision, account, server = raw.decode('ascii').strip('\r\n').split('\t')
    if header != ROLE or not REVISION.fullmatch(revision) or not ACCOUNT.fullmatch(account) or not re.fullmatch(r'[ -~]{1,128}', server):
        raise ValueError('Invalid chart recorder route')
    return revision, account, server


def write_route(root, account, server):
    if not ACCOUNT.fullmatch(account) or not re.fullmatch(r'[ -~]{1,128}', server):
        raise ValueError('Invalid chart recorder account')
    revision = uuid.uuid4().hex
    temporary = root / (ROUTE + '.' + revision + '.tmp')
    try:
        with temporary.open('xb') as file:
            file.write(f'{ROLE}\t{revision}\t{account}\t{server}\n'.encode('ascii'))
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, root / ROUTE)
    finally:
        temporary.unlink(missing_ok=True)


def capture_plan(state, account):
    details = state.get('accounts', {}).get(account)
    identifier = state.get('id')
    if details is None or not isinstance(identifier, str) or not identifier:
        return None
    phase = state.get('lanePhases', {}).get(account, state.get('phase'))
    ready = phase in ('completed', 'stopped')
    ready &= not any(state.get(key) or details.get(key) for key in (
        'missingNativeChild', 'admissionWaiting', 'pendingVolume'))
    ready &= (state.get('laneReconciliation', {}).get(account, {}).get('status') != 'checking'
              and state.get('reconciliation', {}).get('status') != 'checking')
    members = set()
    for child in state.get('children', []):
        position = child.get('positions', {}).get(account)
        closed = child.get('closedOpenings', {}).get(account)
        if account in child.get('volumes', {}) and child.get('state') in ('pending', 'unknown', 'sending'):
            result = child.get('result', {}).get('accountResults', {}).get(account, {})
            if not child.get('settledAccounts', {}).get(account) and result.get('status') != 'rejected':
                ready = False
        ticket = str(closed.get('positionId') if closed else position.get('ticket') if position else '')
        if POSITION.fullmatch(ticket):
            members.add(ticket)
    if not 0 < len(members) <= 200:
        return None
    return hashlib.sha256(identifier.encode()).hexdigest()[:24], ready, tuple(sorted(members, key=int))


def read_capture_plans(path, account, requested, coordinator=False):
    """Read recent plans for one ledger with an independent, bounded budget."""
    if not requested or not path.is_file():
        return ()

    deadline = time.monotonic() + .350
    plans = []
    with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=0)) as db:
        db.set_progress_handler(lambda: int(time.monotonic() > deadline), 2000)
        # SQLite filters before copying large plan records into Python.
        # capture_plan verifies exact position identities after this prefilter.
        tickets = tuple(sorted(requested))
        filters = ' OR '.join('instr(state, ?) > 0' for _ in tickets)
        records = db.execute(
            'SELECT state FROM (SELECT state FROM chunk_entries ORDER BY rowid DESC LIMIT 256) '
            f'WHERE {filters}', tickets)
        for record, in records:
            state = json.loads(record)
            if coordinator and state.get('accountExecution') == 'independent-v5':
                continue
            plan = capture_plan(state, account)
            if plan is not None:
                plans.append(plan)
    return plans


class ChartRecorder:
    def __init__(self, root, inbox, accounts, server, clock=time.time, plan_directory=None):
        self.root, self.inbox = Path(root), Path(inbox)
        self.accounts, self.server, self.clock = accounts, server, clock
        self.plan_directory = Path(plan_directory) if plan_directory is not None else None
        self.lock = threading.Lock()
        self.stop = threading.Event()

    def _heartbeat(self, account, worker):
        file = self.root / f'{account}-{worker}.tsv'
        try:
            stat = file.stat()
            if not 0 <= self.clock() - stat.st_mtime <= 8 or stat.st_size > 384:
                return None
            schema, server, revision, active = file.read_text(encoding='utf-8').strip('\r\n').split('\t')
            if schema != ROLE or server != self.server or not REVISION.fullmatch(revision) or active not in ('0', '1'):
                return None
            return revision, active == '1'
        except (OSError, ValueError):
            return None

    def _state(self):
        revision, selected, server = read_route(self.root)
        if server != self.server:
            raise ValueError('Chart recorder server mismatch')
        rows = []
        applied = True
        for account in self.accounts():
            heartbeats = {worker: self._heartbeat(account, worker) for worker in WORKERS}
            missing = [worker for worker, value in heartbeats.items() if value is None or value[0] != revision]
            ready = not missing
            if account == selected:
                applied &= ready and all(value == (revision, True) for value in heartbeats.values())
            else:
                applied &= all(value is None or not value[1] for value in heartbeats.values())
            rows.append(dict(account=account, ready=ready, missing=missing))
        return dict(account=selected, revision=revision, applied=bool(applied and any(row['account']==selected for row in rows)), accounts=rows)

    def state(self, payload=None):
        with self.lock:
            previous = self._state()
            if payload is None:
                return previous
            if (not isinstance(payload, dict) or set(payload) != {'account', 'revision'}
                    or not isinstance(payload['account'], str) or not ACCOUNT.fullmatch(payload['account'])):
                raise ValueError('Выбери счёт MT5 из списка')
            if payload['revision'] != previous['revision']:
                raise ValueError('Выбор терминала уже изменился. Обнови настройки.')
            if not any(row['account'] == payload['account'] and row['ready'] for row in previous['accounts']):
                raise ValueError('Терминал не подтвердил готовность уровней и снимков USTEC и SPX500')
            if payload['account'] == previous['account']:
                return previous
            write_route(self.root, payload['account'], self.server)
            return self._state()

    def requested_plan_rows(self):
        """Publish only plan identities needed by the screenshot timer, outside order processing."""
        if self.plan_directory is None:
            return
        revision, account, server = read_route(self.root)
        if server != self.server or account not in self.accounts():
            return
        requested = set()
        for symbol in CAPTURE_SYMBOLS:
            path = self.root / f'shot-wait-{account}-{symbol}.tsv'
            try:
                stat = path.stat()
                if not 0 <= self.clock() - stat.st_mtime <= 4 or not 0 < stat.st_size <= 16384:
                    continue
                fields = path.read_text(encoding='utf-8').split()
                if fields[:1] == [revision]:
                    requested.update(value for value in fields[1:] if POSITION.fullmatch(value))
            except OSError:
                continue
        if not requested:
            return
        # Keep the read transactions short. The entry engines use WAL and do not
        # use this reader, its files, or its locks on their execution path.
        rows = {}
        ambiguous = set()
        sources = [(self.plan_directory / 'entry-coordinator.sqlite3', True),
                   (self.plan_directory / f'entry-plans-{account}.sqlite3', False)]
        for path, head in sources:
            try:
                plans = read_capture_plans(path, account, requested, coordinator=head)
            except (OSError, ValueError, TypeError, sqlite3.Error):
                # A busy or slow coordinator must not delay the account ledger.
                continue
            for plan in plans:
                group, ready, members = plan
                for position in members:
                    if position not in requested:
                        continue
                    previous = rows.get(position)
                    if previous is not None and previous[0] != group:
                        ambiguous.add(position)
                    rows[position] = group, ready, members
        output = [f'ISCC_SHOT_PLANS\t{server}\t{revision}\t{int(self.clock())}']
        for position in sorted(requested - ambiguous):
            if position not in rows:
                continue
            group, ready, members = rows[position]
            output.append(f'{position}\t{group}\t{int(ready)}\t{",".join(members)}')
        target = self.root / f'shot-plans-{account}.tsv'
        temporary = target.with_suffix('.tmp')
        temporary.write_text('\n'.join(output)+'\n', encoding='utf-8')
        os.replace(temporary, target)

    def publish(self):
        outbox = self.root / 'outbox'
        if not outbox.is_dir():
            return
        self.inbox.mkdir(parents=True, exist_ok=True)
        pending = 0
        for receipt in outbox.glob('*.json'):
            if pending >= 24:
                break
            try:
                stem = receipt.stem
                match = SHOT.fullmatch(stem)
                if not match or not 0 < receipt.stat().st_size <= 256 * 1024:
                    continue
                metadata = json.loads(receipt.read_text(encoding='utf-8'))
                if (metadata.get('account_id') != f'mt5:{self.server}:{match[1]}' or metadata.get('symbol') not in CAPTURE_SYMBOLS
                        or metadata.get('image') != stem+'.png' or metadata.get('status') not in ('ready', 'context_only')):
                    continue
                image = receipt.with_suffix('.png')
                if not 0 < image.stat().st_size <= 25_000_000 or metadata.get('image_bytes') != image.stat().st_size:
                    continue
                pending += 1
                # Publish the manifest last. An existing archive always wins.
                for source in (image, receipt):
                    destination = self.inbox / source.name
                    if not destination.exists():
                        temporary = destination.with_name('.' + destination.name + '.incoming')
                        shutil.copyfile(source, temporary)
                        os.replace(temporary, destination)
                receipt.unlink()
                image.unlink()
            except (OSError, ValueError, AttributeError):
                continue

    def start(self):
        def run():
            next_publish = 0.
            while not self.stop.wait(1 if self.plan_directory is not None else 5):
                try:
                    self.requested_plan_rows()
                except (OSError, ValueError, TypeError, sqlite3.Error):
                    pass
                if time.monotonic() >= next_publish:
                    next_publish = time.monotonic() + 5
                    try:
                        self.publish()
                    except OSError:
                        pass
        threading.Thread(target=run, daemon=True, name='iscc-chart-screenshots').start()


def read_json(path, limit=64 * 1024):
    if path.is_symlink() or path.stat().st_size > limit:
        raise ValueError('Invalid Trade Hub settings file')
    value = json.loads(path.read_text(encoding='utf-8-sig'))
    if not isinstance(value, dict):
        raise ValueError('Invalid Trade Hub settings')
    return value


def write_json(path, value):
    temporary = path.with_name(f'.{path.name}.{uuid.uuid4().hex}.tmp')
    try:
        with temporary.open('x', encoding='utf-8') as file:
            json.dump(value, file, separators=(',', ':'), ensure_ascii=False, allow_nan=False)
            file.write('\n')
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def history_header(path):
    # The exporter writes header first. Reading an export in the settings
    # dialog should never parse the entire trading history.
    if path.is_symlink() or path.stat().st_size > 25_000_000:
        raise ValueError('Invalid history export')
    with path.open(encoding='utf-8-sig') as file:
        prefix = file.read(32_768)
    match = re.match(r'\s*\{\s*"header"\s*:\s*', prefix)
    if not match:
        raise ValueError('History export lacks its native header')
    header, _ = json.JSONDecoder().raw_decode(prefix, match.end())
    if not isinstance(header, dict):
        raise ValueError('Invalid history header')
    return header


class TradeHubRoles:
    """One authenticated settings view for the actual journal and chart routes."""

    @classmethod
    def from_config(cls, config, accounts, chart):
        if os.name != 'nt' or not config.get('candleLevelsFileDirectory'):
            return None
        shared = Path(config['candleLevelsFileDirectory']) / 'DWX' / 'StopAudit' / 'VPS'
        if not shared.is_dir():
            return None
        return cls(shared, accounts, config['server'], chart)

    def __init__(self, shared, accounts, server, chart, clock=time.time):
        self.shared, self.accounts, self.server = Path(shared), accounts, server
        self.chart, self.clock = chart, clock
        self.lock = threading.Lock()

    def _saved(self):
        try:
            value = read_json(self.shared / TRADEHUB_ROLES_FILE)
        except FileNotFoundError:
            return {}
        allowed = value.get('accounts')
        if (value.get('schema') != 1 or value.get('server') != self.server
                or not isinstance(allowed, list) or len(allowed) > 128
                or any(not isinstance(account, str) or not ACCOUNT.fullmatch(account) for account in allowed)
                or len(allowed) != len(set(allowed))):
            raise ValueError('Не удалось проверить список счетов Trade Hub')
        return value

    def _history(self, path, account=None):
        try:
            header = history_header(path)
            login = header.get('login')
            generated = datetime.fromisoformat(header['exported_at_utc'].replace('Z', '+00:00'))
            if (type(login) is not int or not ACCOUNT.fullmatch(str(login)) or (account and account != str(login))
                    or header.get('schema') != 1 or header.get('source') != 'vps_native_mt5_history'
                    or header.get('account') != f'mt5:{self.server}:{login}' or header.get('server') != self.server
                    or header.get('currency') != 'USD' or type(header.get('count')) is not int or header['count'] < 0
                    or generated.tzinfo is None or generated.timestamp() > self.clock() + 60):
                return None
            return str(login)
        except (OSError, ValueError, TypeError, AttributeError, KeyError):
            return None

    def _history_role(self, accounts, legacy):
        try:
            config = read_json(self.shared / 'active-history-account.json', 4096)
            login = config.get('login')
            if type(login) is not int or not ACCOUNT.fullmatch(str(login)):
                raise ValueError('Некорректный счёт истории Trade Hub')
            selected = str(login)
            revision = config.get('revision')
        except FileNotFoundError:
            selected, revision = legacy, None
        if not isinstance(revision, str) or not REVISION.fullmatch(revision):
            revision = hashlib.sha256(f'history:{self.server}:{selected}'.encode()).hexdigest()[:32]
        ready = {account for account in accounts if account == legacy or self._history(
            self.shared / 'Accounts' / f'{account}.json', account) == account}
        rows = [dict(account=account, ready=account in ready, reason=None if account in ready else
                     'Нужна подтверждённая выгрузка истории') for account in accounts]
        return dict(account=selected, revision=revision, applied=selected in ready, accounts=rows)

    def _ticks_role(self, accounts, saved):
        try:
            status = read_json(self.shared / 'status.json')
        except (OSError, ValueError):
            status = {}
        selected = str(status.get('account', ''))
        if not ACCOUNT.fullmatch(selected) or status.get('runs_on') != 'VPS' or status.get('read_only') is not True:
            selected = None
        route = saved.get('tickRoute')
        route = route if isinstance(route, dict) else {}
        revision = route.get('revision')
        if not isinstance(revision, str) or not REVISION.fullmatch(revision):
            revision = hashlib.sha256(f'ticks:{self.server}:{selected}'.encode()).hexdigest()[:32]
        try:
            proof = read_json(self.shared / 'tick-role-readiness.json')
            checked = datetime.fromisoformat(proof['checkedAt'].replace('Z', '+00:00'))
            if (proof.get('schema') != 1 or proof.get('server') != self.server or proof.get('managed') is not True
                    or checked.tzinfo is None or not -60 <= self.clock() - checked.timestamp() <= 180
                    or not isinstance(proof.get('accounts'), list)):
                proof = {}
        except (OSError, ValueError, TypeError, AttributeError, KeyError):
            proof = {}
        ready = set()
        for row in proof.get('accounts', ()):
            try:
                checked = datetime.fromisoformat(row['checkedAt'].replace('Z', '+00:00'))
                if (row.get('ready') is True and isinstance(row.get('account'), str) and checked.tzinfo is not None
                        and -60 <= self.clock() - checked.timestamp() <= 900):
                    ready.add(row['account'])
            except (ValueError, TypeError, AttributeError, KeyError):
                continue
        pending = route.get('account')
        applied = bool(selected and (not pending or (pending == selected and status.get('roleRevision') == revision)))
        rows = [dict(account=account, ready=account in ready,
                     reason=None if account in ready else 'Сборщик тиков не подтвердил готовность терминала')
                for account in accounts]
        reason = status.get('pendingReason')
        reason = reason[:240] if not applied and pending == status.get('pendingAccount') and isinstance(reason, str) else None
        return dict(account=selected, revision=revision, applied=applied,
                    pendingAccount=None if applied else pending, pendingReason=reason, accounts=rows)

    def _state(self, saved):
        accounts = tuple(dict.fromkeys(str(account) for account in self.accounts() if ACCOUNT.fullmatch(str(account))))
        legacy = self._history(self.shared / 'history.json')
        history = self._history_role(accounts, legacy)
        chart = self.chart.state() if self.chart is not None else None
        if chart:
            for row in chart['accounts']:
                row['reason'] = None if row['ready'] else 'Терминал не подтвердил уровни или снимки'
        ticks = self._ticks_role(accounts, saved)
        return dict(roles=dict(history=history, ticks=ticks, chart=chart), historyAccounts=list(accounts))

    def state(self, payload=None):
        with self.lock:
            saved = self._saved()
            view = self._state(saved)
            if payload is None:
                return view
            if (not isinstance(payload, dict) or set(payload) != {'role', 'account', 'revision'}
                    or not isinstance(payload['role'], str) or payload['role'] not in view['roles']
                    or not isinstance(payload['account'], str) or not ACCOUNT.fullmatch(payload['account'])):
                raise ValueError('Выбери роль и счёт MT5')
            role, account = payload['role'], payload['account']
            current = view['roles'][role]
            if current is None or payload['revision'] != current['revision']:
                raise ValueError('Настройки ролей изменились. Обнови список.')
            if not any(row['account'] == account and row['ready'] for row in current['accounts']):
                raise ValueError('Счёт не подтвердил готовность для этой роли')
            if role == 'history' and current['account'] != account:
                write_json(self.shared / 'active-history-account.json', dict(login=int(account), revision=uuid.uuid4().hex))
            elif role == 'chart':
                self.chart.state(dict(account=account, revision=current['revision']))
            elif role == 'ticks' and (current['account'] != account or current.get('pendingAccount')):
                saved['tickRoute'] = dict(account=account, revision=uuid.uuid4().hex)
            updated = self._state(saved)
            selected = updated['roles']
            legacy = self._history(self.shared / 'history.json')
            approved = set(saved.get('accounts', ())) | set(updated['historyAccounts'])
            approved.update(row['account'] for row in selected.values() if row and row['account'])
            if legacy:
                approved.add(legacy)
            if len(approved) > 128:
                raise ValueError('Список подтверждённых счетов Trade Hub слишком большой')
            saved.update(schema=1, server=self.server, accounts=sorted(approved), historyAccount=selected['history']['account'],
                         chartAccount=selected['chart']['account'] if selected['chart'] else None,
                         tickAccount=selected['ticks']['account'])
            write_json(self.shared / TRADEHUB_ROLES_FILE, saved)
            return updated

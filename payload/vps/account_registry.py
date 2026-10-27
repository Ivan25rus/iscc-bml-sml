"""Explicit, read-only terminal discovery and disabled-by-default account enrollment.

No credentials, command retries or trading calls belong in this module. MT4 is
deliberately a preparation-only platform until its execution bridge is validated.
"""
import hashlib
import json
import math
import os
import pathlib
import re
import subprocess
import sys
import threading
import time
import uuid

RECORD_FIELDS = ('platform', 'account', 'server', 'currency', 'terminalPath', 'specifications')
PROBE_FIELDS = (*RECORD_FIELDS, 'connected', 'idle', 'executionReady')


def path_key(value):
    return str(value).replace('/', '\\').rstrip('\\').casefold()


def identity(row):
    return tuple(str(row.get(key, '')) for key in ('platform', 'account', 'server', 'currency')) + (path_key(row['terminalPath']),)


def terminal_processes():
    if os.name != 'nt':
        raise ValueError('Terminal discovery requires the Windows VPS')
    script = "[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false); @(Get-CimInstance Win32_Process -Filter \"Name='terminal64.exe' OR Name='terminal.exe'\" | Select-Object ExecutablePath) | ConvertTo-Json -Compress"
    result = subprocess.run(['powershell.exe', '-NoProfile', '-NonInteractive', '-Command', script],
                            capture_output=True, timeout=5, check=True)
    rows = json.loads(result.stdout.decode('utf-8-sig').strip() or '[]')
    if isinstance(rows, dict): rows = [rows]
    paths = sorted({row['ExecutablePath'] for row in rows if isinstance(row, dict) and row.get('ExecutablePath')})
    if len(paths) > 24: raise ValueError('Too many terminals; close unused terminals before discovery')
    return paths


def mt4_data_directory(terminal):
    install = pathlib.Path(terminal).parent
    portable = install / 'MQL4'
    roots = [install] if portable.is_dir() else []
    parent = pathlib.Path(os.environ.get('APPDATA', '')) / 'MetaQuotes' / 'Terminal'
    if parent.is_dir():
        for child in parent.iterdir():
            origin = child / 'origin.txt'
            if not origin.is_file() or origin.stat().st_size > 8192: continue
            raw = origin.read_bytes()
            text = raw.decode('utf-16' if raw.startswith((b'\xff\xfe', b'\xfe\xff')) else 'utf-8-sig').strip()
            if path_key(text) == path_key(install) and (child / 'MQL4').is_dir(): roots.append(child)
    matches = list(dict.fromkeys(roots))
    # Never choose an arbitrary profile when /portable and a regular profile coexist.
    fresh = [root for root in matches if (root / 'MQL4/Files/ISCC/account.json').is_file()
             and 0 <= time.time()-(root / 'MQL4/Files/ISCC/account.json').stat().st_mtime <= 5]
    if len(fresh) == 1: return fresh[0]
    if len(matches) == 1: return matches[0]
    raise ValueError('MT4 data folder is missing or ambiguous')


def read_mt4(terminal, now=None):
    file = mt4_data_directory(terminal) / 'MQL4/Files/ISCC/account.json'
    if file.is_symlink() or not file.is_file() or file.stat().st_size > 32768:
        raise ValueError('Attach ISCC_AccountProbe to one MT4 chart')
    now = time.time() if now is None else now
    age = now - file.stat().st_mtime
    if not 0 <= age <= 5: raise ValueError('MT4 heartbeat is stale')
    row = json.loads(file.read_text(encoding='utf-8-sig'))
    if row.get('schemaVersion') != 1 or row.get('platform') != 'mt4' or path_key(row.get('terminalPath', '')) != path_key(terminal):
        raise ValueError('MT4 terminal identity mismatch')
    if row.get('connected') is not True: raise ValueError('MT4 is disconnected')
    row['terminalPath'] = str(terminal)
    row['executionReady'] = False
    return {key: row[key] for key in PROBE_FIELDS if key in row}


def probe_terminal(terminal, symbols):
    if pathlib.Path(terminal).name.lower() == 'terminal.exe':
        return read_mt4(terminal)
    # An isolated process must never switch the receiver's MT5 account. The
    # helper also checks that the selected process is already running.
    result = subprocess.run([sys.executable, '-B', str(pathlib.Path(__file__).resolve()), '--probe-mt5',
                             str(terminal), json.dumps(symbols)], capture_output=True, timeout=6)
    if result.returncode: raise ValueError('MT5 terminal is disconnected or failed identity/specification checks')
    if len(result.stdout) > 32768: raise ValueError('Invalid terminal response')
    return json.loads(result.stdout)


def probe_mt5(terminal, symbols):
    if path_key(terminal) not in {path_key(x) for x in terminal_processes()}:
        raise ValueError('Terminal must already be running')
    import MetaTrader5 as mt5
    try:
        if not mt5.initialize(path=terminal, timeout=3000): raise ValueError('MT5 unavailable')
        account, info = mt5.account_info(), mt5.terminal_info()
        if not account or not info or not info.connected or path_key(info.path) != path_key(pathlib.Path(terminal).parent):
            raise ValueError('MT5 identity unavailable')
        positions, orders = mt5.positions_get(), mt5.orders_get()
        if positions is None or orders is None: raise ValueError('Cannot verify terminal orders')
        specs = {}
        for symbol in symbols:
            spec = mt5.symbol_info(symbol)
            if not spec: raise ValueError('Broker symbol not found')
            specs[symbol] = dict(quantityStep=spec.volume_step, minQty=spec.volume_min,
                                 maxQty=spec.volume_max, tickSize=spec.trade_tick_size,
                                 contractSize=spec.trade_contract_size)
        return dict(platform='mt5', account=str(account.login), server=account.server, currency=account.currency,
                    terminalPath=terminal, connected=True, idle=not positions and not orders,
                    executionReady=True, specifications=specs)
    finally: mt5.shutdown()


class AccountRegistry:
    def __init__(self, bridge, file, worker_factory, enumerate_terminals=terminal_processes, probe=probe_terminal):
        self.bridge, self.file, self.worker_factory = bridge, pathlib.Path(file), worker_factory
        self.enumerate_terminals, self.probe = enumerate_terminals, probe
        self.lock = threading.RLock()
        self.scanning = False
        self.candidates, self.records = {}, []
        self.scan_error, self.scanned_at = None, None
        self.last_scan_started = None
        if self.file.exists():
            if self.file.is_symlink() or self.file.stat().st_size > 65536: raise ValueError('Invalid account registry file')
            stored = json.loads(self.file.read_text(encoding='utf-8'))
            if stored.get('schemaVersion') != 1 or not isinstance(stored.get('accounts'), list) or len(stored['accounts']) > 16:
                raise ValueError('Invalid account registry schema')
            seen = {str(bridge.config['account']), *bridge.linked}
            paths = {path_key(bridge.config['terminalPath']), *(path_key(x.config['terminalPath']) for x in bridge.linked.values())}
            for row in stored['accounts']:
                if not isinstance(row, dict) or set(row) != set(RECORD_FIELDS): raise ValueError('Invalid stored account fields')
                self.validate(row)
                if row['account'] in seen or path_key(row['terminalPath']) in paths: raise ValueError('Duplicate registered terminal/account')
                seen.add(row['account']); paths.add(path_key(row['terminalPath']))
                self.records.append(row)

    def validate(self, row):
        if not isinstance(row, dict) or row.get('platform') not in ('mt4', 'mt5'):
            raise ValueError('Unknown account platform')
        if not isinstance(row.get('account'), str) or not re.fullmatch(r'[1-9][0-9]{0,19}', row['account']):
            raise ValueError('Invalid account number')
        if not isinstance(row.get('server'), str) or not 1 <= len(row['server']) <= 128 or any(ord(c) < 32 for c in row['server']):
            raise ValueError('Invalid broker server')
        if row.get('currency') != 'USD': raise ValueError('Only USD accounts are supported by the current risk calculator')
        if not isinstance(row.get('terminalPath'), str) or not pathlib.Path(row['terminalPath']).is_absolute() or pathlib.Path(row['terminalPath']).name.lower() not in ('terminal.exe', 'terminal64.exe'):
            raise ValueError('Invalid terminal path')
        if row['platform'] == 'mt5' and row['server'] != self.bridge.config['server']:
            raise ValueError('Automatic MT5 enrollment currently requires the same broker server as the primary')
        specs = row.get('specifications', {})
        expected = set(self.bridge.config['symbols']) if row['platform']=='mt5' else {'SPX500','USTEC'}
        if not isinstance(specs, dict) or set(specs) != expected:
            raise ValueError('Both index symbols must match the configured broker symbols')
        for spec in specs.values():
            if not isinstance(spec, dict): raise ValueError('Invalid lot specifications')
            if set(spec)-{'quantityStep','minQty','maxQty','tickSize','contractSize','symbol'}:
                raise ValueError('Unknown instrument specification fields')
            if 'symbol' in spec and (not isinstance(spec['symbol'],str) or not re.fullmatch(r'[A-Za-z0-9_.#-]{1,64}',spec['symbol'])):
                raise ValueError('Invalid broker symbol name')
            for key in ('quantityStep', 'minQty', 'maxQty', 'tickSize', 'contractSize'):
                value = spec.get(key)
                if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                    raise ValueError('Invalid broker lot specifications')
            if spec['maxQty'] < spec['minQty']: raise ValueError('Invalid broker lot bounds')

    def known(self, row):
        accounts = {str(self.bridge.config['account']), *self.bridge.linked, *(x['account'] for x in self.records)}
        return str(row.get('account', '')) in accounts

    def scan(self):
        with self.lock:
            if self.scanning: return
            if self.last_scan_started is not None and time.monotonic() - self.last_scan_started < 10: return
            self.scanning, self.last_scan_started = True, time.monotonic()
        def run():
            candidates, error = {}, None
            try:
                for terminal in self.enumerate_terminals():
                    platform = 'mt4' if pathlib.Path(terminal).name.lower() == 'terminal.exe' else 'mt5'
                    row = dict(platform=platform, terminalPath=terminal, state='unavailable')
                    try:
                        probed=self.probe(terminal, self.bridge.config['symbols'])
                        row.update({key:probed[key] for key in PROBE_FIELDS if key in probed})
                        self.validate(row)
                        if row.get('connected') is not True: raise ValueError('Terminal is disconnected')
                        row['state'] = 'registered' if self.known(row) else 'available'
                    except (ValueError, OSError, subprocess.SubprocessError):
                        row = dict(platform=platform,terminalPath=terminal,state='unavailable',
                                   reason='MT4: нужен подключённый USD-счёт и ISCC_AccountProbe' if platform == 'mt4' else 'MT5: проверь связь, USD, сервер и названия индексов')
                    key = hashlib.sha256(json.dumps(identity(row)).encode()).hexdigest()
                    candidates[key] = dict(row, candidateId=key, checkedAt=time.time()*1000)
            except (ValueError, OSError, subprocess.SubprocessError): error = 'Не удалось прочитать терминалы VPS'
            finally:
                with self.lock:
                    self.candidates, self.scan_error = candidates, error
                    self.scanned_at, self.scanning = time.time()*1000, False
        threading.Thread(target=run, name='iscc-account-discovery', daemon=True).start()

    def catalog(self):
        with self.lock:
            return dict(scanning=self.scanning, scannedAt=self.scanned_at, error=self.scan_error,
                        candidates=[dict(row,state='registered') if row.get('state') in ('available','registered') and self.known(row) else dict(row)
                                    for row in self.candidates.values()],
                        registered=[dict(row, executionReady=row['platform'] == 'mt5', copyingEnabled=False) for row in self.records])

    def persist(self, records):
        content = json.dumps(dict(schemaVersion=1, accounts=records), ensure_ascii=False, allow_nan=False).encode()
        temporary = self.file.with_name(self.file.name + '.' + uuid.uuid4().hex + '.tmp')
        try:
            with open(temporary, 'xb') as stream:
                os.chmod(temporary, 0o600)
                stream.write(content); stream.flush(); os.fsync(stream.fileno())
            os.replace(temporary, self.file)
        finally:
            if temporary.exists(): temporary.unlink()

    def register(self, payload):
        if not isinstance(payload, dict) or set(payload) != {'candidateId'} or not isinstance(payload['candidateId'], str):
            raise ValueError('Select a discovered terminal; paths and credentials are not accepted')
        if not self.bridge.group_lock.acquire(blocking=False): raise ValueError('Wait for the current trading command to finish')
        try:
            with self.lock:
                row = self.candidates.get(payload['candidateId'])
                if not row: raise ValueError('Discover terminals first')
                for registered in self.records:
                    if identity(row) == identity(registered): return dict(registered=True, **registered, executionReady=registered['platform']=='mt5')
                if row.get('state') != 'available' or not 0 <= time.time()*1000-row['checkedAt'] <= 120000:
                    raise ValueError('Refresh discovery before registering this terminal')
                current = self.probe(row['terminalPath'], self.bridge.config['symbols'])
                self.validate(current)
                if current.get('connected') is not True: raise ValueError('Terminal disconnected after discovery')
                if identity(current) != identity(row): raise ValueError('Account changed after discovery; scan again')
                if self.known(current): raise ValueError('Account is already configured')
                occupied = [self.bridge.config['terminalPath'], *(x.config['terminalPath'] for x in self.bridge.linked.values()), *(x['terminalPath'] for x in self.records)]
                if path_key(current['terminalPath']) in {path_key(x) for x in occupied}: raise ValueError('Each account requires a separate terminal')
                if len(self.records)+len(self.bridge.config.get('linkedAccounts', [])) >= 16: raise ValueError('At most 16 additional accounts are supported')
                if current.get('idle') is not True: raise ValueError('New account must have no open orders or positions')
                record = {key: current[key] for key in RECORD_FIELDS}
                worker = None
                try:
                    if record['platform'] == 'mt5':
                        worker = self.worker_factory(record)
                        health = worker.call('health')
                        if str(health.get('account')) != record['account'] or health.get('server') != record['server']:
                            raise ValueError('New worker identity mismatch')
                    self.persist([*self.records, record])
                except Exception:
                    if worker: worker.close()
                    raise
                self.records.append(record)
                if worker: self.bridge.linked = {**self.bridge.linked, record['account']: worker}
                return dict(registered=True, **record, executionReady=record['platform']=='mt5')
        finally: self.bridge.group_lock.release()

    def restore(self):
        for row in self.records:
            if row['platform'] == 'mt5': self.bridge.linked[row['account']] = self.worker_factory(row, wait=False)


if __name__ == '__main__':
    if len(sys.argv) != 4 or sys.argv[1] != '--probe-mt5': raise SystemExit(2)
    try: print(json.dumps(probe_mt5(sys.argv[2], json.loads(sys.argv[3])), allow_nan=False))
    except Exception: raise SystemExit(1)

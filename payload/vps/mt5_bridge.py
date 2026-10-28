"""HTTPS receiver for the user's two MT5 indices; no order retries."""
import hashlib
import copy
import hmac
import json
import gzip
import math
import pathlib
import re
import sqlite3
import socket
import ssl
import threading
import time
import os
import uuid
import multiprocessing
import subprocess
import sys
from contextlib import contextmanager, nullcontext
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from functools import wraps, lru_cache
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING
from collections import deque
from types import SimpleNamespace
from broker_opening_evidence import opening_order_evidence

RULE_ACTIVE_INTERVAL = .05
RULE_IDLE_INTERVAL = .1
RECEIVER_RETRY_SECONDS = 1
BRIDGE_VERSION = 39
ENTRY_PROTOCOL_CAPABILITIES = ('hybrid-entry-batch-v1', 'entry-admission-cancel-proof-v1', 'hybrid-quote-wait-v1', 'entry-cancel-proof-v1', 'entry-fill-priority-v1')

class EntryPriceRejected(ValueError):
    def __init__(self, message, details):
        super().__init__(message)
        self.entry_diagnostics = details

@lru_cache(maxsize=1)
def runtime_build_identity():
    # Hash once at process startup. No file reads on the quote/trading path.
    directory = pathlib.Path(__file__).resolve().parent
    receiver = hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()
    entry = hashlib.sha256((directory/'chunk_entry.py').read_bytes()).hexdigest()
    return dict(buildId=receiver[:12]+'-'+entry[:12], receiverSha256=receiver,
                entryModuleSha256=entry, entryAdmissionContract='atomic-entry-admission-v1')

# Windows does not ship an IANA timezone database. Keep the same public
# New York TZif rules as the Mac, including the DST transition table and tail.
NEW_YORK_TZIF_GZIP = 'H4sIAAAAAAAC/+3Wf3TNdRzH8TtmGGukibBENcM2P1p+rVqhNdzmxxaW5qM2O0PtHTVTmgppzqnV4UzFmciPEebXZNT8bBydfjBEP+ZDLMn4sEJW9/Z5fqq/+sufndO9534fO5s/3dfznZyaNb6H51+voL8/vM7//fMtL9tH0YpwWbztZ/X+ax5Z8t7namnGKflg5k962YR1sjx2pVox4oBZGZavinuuMcW+5Xp1izfN6qo5+sNaMWvKs/TayuFmXdFgXbLpHrM+L0pvWDRMNmYOUpum95TNCV1V6Zgw2RJxk/qo31XZGmRUWZvjUlZ9SG0P/NVsr9isPj79tfmk5CtdvrfM7CjYqHcuW2h2Zc/Xu2e9ZPYk5ei96WPNpzGjdIV3uuwL7av3R6XJ/msj1YGQeDlwrI/67HUjhzr+IodUna58/rQ5HP+9PpJSYY6236G/ji02xzxL9PGwuea4nqm/8a2Ub3dm6O+q8uX7Va+qqvKJciI/Xekir5zMelidyouRHxI7qdNpzeXMyGhdnRBlfuzVTJ+NCDU/tbyszwVdMueuHNY/V1ea80e36JqKUnNhR6W6WHJIzOJSdalgs1yesUDVZhfKL2OnqV+TcuXKA4+rqzGpcq3TVP1baJxcbzRaX782ytSd7ad/P9bX/LG/nfZtbWv8K/3as8BnAuac1PWmalNffCpQ/SENhmoVFH9CGnbbrRq+0VoaN1umGq+db4LNa6qJni1Njy/VITsz5aayWTp01UzT7N3xunl+hrl52iO6RVaCuWV0Zx2WGGla9mmqb41sYlp1iFStG9WY2+o1UbddDpY2J2tU24Pnpd2uL1X4hi/k9iUbVPscj7njlXmqQ8op03FCib4zdq+5a9Db+u6wFSai8xQd4dsjkcEpOrJquXQ+10t3KZ8jXSuTVVRRlkRvulfF5A2WbvNaqe6ZmabH5OuqZ0KiuWf4dyo2oou5t0e56tUwxPRu843uXX3R9Kn7WPetOGj6fVuk40ouyH3bX9b3F3wlDyx8Usdnb5QHZ81QDyXNl/7p49SAmBwZOKC/GlhbbBKixuqEyrkmMeRBnbhpkhlU01EPXpQvQ47GK+/0ifLolg4qaYxXhhYGqmFxMTL8+Wo1om1zSU7Zr5LrauWx2NXqsTOhZlR4hR6195IZ7SvWqcsqzeNVc/WYWaXmifJJOi19gRlb9KhW3s0ybs5E9WRUoTyV6VXpIbmSkRCjMmpSJTOiucr8PE6ygmpV1tpwmVB9RE0s7GsmfXlJP53T1jxTUqmzU3xGCkr1s7HaTM5eoKeE7TbPJU3Tz/lOSE7vQpVTtUtyW+Wq3PIPZNq1VPVC0Wx58Vicmp6XKS9tDVd5aYky4x2Pqh9Q78begQ3qBfxH337/tiKPx+/ftzMg0O8vW+9p9M/T/qapezYb4k32DBxgPyPsZ6T9DE1m3wLs2z2Tb2AL/faVVt+8hWwisovINiL7iGwkspPIViJ7iWwmspvIdjrtfiIb6rQ7imwpsqfIpiK7imwrsq/IxiI7i2wtsrfI5iK7i2wvsr/IBjvtDiNb7LR7jGwyssvINiP7jGw0stPIViN7jWw2stvIdiP7jWy40+44suVOu+fIpiO77rTbjuw7svHIziNbj+w9svnI7jvt9iP7jzQA6QDSAqQHSBOQLiBtQPqANALpBNIKpBdIM5BuIO1A+uG0DUE6grQE6QnSFKQrSFuQviCNQTqDtAbpDdIcpDtIe5D+IA1y2g4hLUJ6hDQJXZfcf2TbJl62T+4LZhuFdAppFdIrpFnui2a7hbTLafuFNMxpO4a0DOkZ0jSka0jbkL4hjUM6h7QO6R3SPKR7SPuQ/iENRDqItNBpe4g0Eeki0kakj0gjkU4irUR6iTQT6SbSTqSfSEOdtqNIS522p0hTka4ibUX6ijQW6SzSWqS3SHOR7iLtRfqLNBjpMNJip+0x0mSky0ibkT4jjUY6jbQa6TXSbKTbSLuRfiMNd9qOIy132p4jTXfariNtR/qONB7pPNJ6pPdI85HuI+1H+o/cAE57ByC3gNPeA8hN4LR3AXIbIPcBciMgdwJyKyD3AnIzIHcDcjsg9wNyQyB3BHJLOO09gdwUTntXILeF094XyI2B3BnIrYHcG8jNgdwdyO2B3B/IDYLcIcgt4vT99X3lJnHauwS5TZz2PkFuFOROQW4V5F5BbhbkbsH/b5cbv12C7R9i7T/o6u0Z3SO6W1dv9+7R3aO7Bf8J8/qOveANAAA='

@lru_cache(maxsize=1)
def new_york_zone():
    from zoneinfo import ZoneInfo
    import base64, gzip, io
    return ZoneInfo.from_file(io.BytesIO(gzip.decompress(base64.b64decode(NEW_YORK_TZIF_GZIP))),key='America/New_York')


def measured(timing, name, fn, *args):
    if timing is None:
        return fn(*args)
    started = time.perf_counter()
    try:
        return fn(*args)
    finally:
        if timing is not None:
            timing[name] = round(timing.get(name, 0.)+(time.perf_counter()-started)*1000, 2)


def scaled_volume(volume, factor, spec, partial=False):
    decimal_factor=Decimal(str(factor))
    volume, factor = float(volume), float(factor)
    if not all(math.isfinite(x) and x>0 for x in (volume,factor)) or factor>100:
        raise ValueError('Invalid lot multiplier or close percentage')
    step, minimum, maximum = (float(spec[k]) for k in ('quantityStep','minQty','maxQty'))
    if not all(math.isfinite(x) and x>0 for x in (step,minimum,maximum)):
        raise ValueError('Invalid broker lot specifications')
    if partial and factor==1: amount=volume
    else:
        units=(Decimal(str(volume))*decimal_factor/Decimal(str(step))+Decimal('1e-12')).quantize(Decimal('1'),rounding=ROUND_FLOOR)
        amount=float(units*Decimal(str(step)))
    if amount<minimum-1e-8 or amount>maximum+1e-8 or (partial and volume-amount>1e-8 and volume-amount<minimum-1e-8):
        raise ValueError('Scaled volume or remainder outside broker lot limits')
    return amount


def serialized(method):
    @wraps(method)
    def call(self, *args, **kwargs):
        started=time.perf_counter()
        with self.lock:
            self.read_timings.record(method.__name__+'LockWait',(time.perf_counter()-started)*1000)
            return method(self, *args, **kwargs)
    return call


def format_management_snapshot(value, account, groups=()):
    """Pure UI formatting after releasing the native worker/RPC queue."""
    result=dict(value)
    for source,target in (('_closeRules','rules'),('_timeRules','timeRules')):
        if source in result:
            result[target]=[dict(id=i,**json.loads(p),state=s,result=json.loads(r)) for i,p,s,r in result.pop(source)]
    if result.get('orders'):
        parsed=[(i,json.loads(p),json.loads(r)) for i,p,r in groups]
        for row in result['orders']:
            identifiers={str(row['ticket']),str(row.get('identifier',row['ticket']))}
            for identifier,payload,opened in parsed:
                original=opened.get('accountResults',{}).get(str(account),{})
                if (payload.get('operation')=='place' and payload.get('payload',{}).get('symbol')==row['symbol']
                        and str(original.get('providerOrderId')) in identifiers):
                    peers=[a for a in payload['requests'] if a!=str(account)]
                    row.update(groupId=identifier,mirrorAccounts=peers,mirrorStatus=opened['status'])
                    if peers:row['mirrorAccount']=peers[0]
                    break
    return result


class ReadTimings:
    """Bounded in-memory timings; never stores arguments, prices or credentials."""
    def __init__(self):
        self.lock = threading.Lock()
        self.operations = {}
        self.recent = deque(maxlen=40)

    def record(self, name, elapsed):
        with self.lock:
            row = self.operations.setdefault(name, dict(calls=0, slow=0, maxMs=0.))
            row['calls'] += 1
            row['maxMs'] = max(row['maxMs'], round(elapsed, 3))
            if elapsed >= 250:
                row['slow'] += 1
                self.recent.append(dict(operation=name, elapsedMs=round(elapsed, 3), at=time.time()*1000))

    @contextmanager
    def span(self, name):
        started = time.perf_counter()
        try:
            yield
        finally:
            self.record(name, (time.perf_counter()-started)*1000)

    def call(self, name, fn, *args, **kwargs):
        with self.span(name):
            return fn(*args, **kwargs)

    def snapshot(self):
        with self.lock:
            return dict(schema=1, thresholdMs=250,
                        operations={key:dict(value) for key,value in self.operations.items()},
                        recent=list(self.recent))

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit


class Bridge:
    def __init__(self, mt5, config, database):
        self.runtime_build = runtime_build_identity()
        self.mt5, self.config = mt5, config
        offset = config.get('brokerUtcOffsetSeconds', 0)
        if type(offset) not in (int, float) or not math.isfinite(offset) or abs(offset) > 14*3600:
            raise ValueError('Invalid broker UTC offset')
        self.broker_utc_offset_seconds = offset
        # Every bridge operation is serialized by the HTTP handler, including
        # ledger access. Quote streams release the lock before network writes.
        self.lock = threading.RLock()
        self.native_command_waiting = threading.Event()
        self.native_command_revision = 0
        self.management_epoch = uuid.uuid4().hex
        self.management_index_accounts = set()
        self.entry_read_context = threading.local()
        self.read_timings = ReadTimings()
        self.quote_freshness = {}
        self.group_lock = threading.RLock()
        self.group_executor = None
        self.preparation_benchmark_file=pathlib.Path(database).resolve().parent/'preparation-benchmark.json'
        self.entry_progress_file=pathlib.Path(database).resolve().parent/('entry-progress-'+str(config['account'])+'.json')
        self.candle_database=pathlib.Path(database).resolve().parent/'candle-levels.sqlite3'
        self.candle_book=None
        # Only immutable timezone rules are cached. Clock freshness and the
        # supplied deadline are still validated for every command/account.
        new_york_zone()
        self.db = sqlite3.connect(database, check_same_thread=False)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('CREATE TABLE IF NOT EXISTS requests (id TEXT PRIMARY KEY, digest TEXT, result TEXT)')
        self.db.execute('CREATE TABLE IF NOT EXISTS close_rules (id TEXT PRIMARY KEY, payload TEXT, state TEXT, result TEXT)')
        self.db.execute('CREATE TABLE IF NOT EXISTS groups (id TEXT PRIMARY KEY, digest TEXT, payload TEXT, result TEXT)')
        for account in (str(config['account']), *(str(row['account']) for row in config.get('linkedAccounts', []))):
            self._ensure_management_indexes(account)
        self.db.execute('CREATE TABLE IF NOT EXISTS time_rules (id TEXT PRIMARY KEY, payload TEXT, state TEXT, result TEXT)')
        self.db.execute('CREATE TABLE IF NOT EXISTS request_targets (id TEXT PRIMARY KEY, ticket TEXT, symbol TEXT)')
        self.db.execute("CREATE INDEX IF NOT EXISTS request_unknown_ticket ON requests(CASE WHEN json_valid(result) THEN json_extract(result,'$.ticket') END) WHERE CASE WHEN json_valid(result) THEN json_extract(result,'$.status') ELSE 'invalid' END IN ('unknown','invalid')")
        self.db.execute("CREATE INDEX IF NOT EXISTS group_unknown_ticket ON groups(CASE WHEN json_valid(payload) THEN json_extract(payload,'$.payload.ticket') END) WHERE CASE WHEN json_valid(result) THEN json_extract(result,'$.status') ELSE 'invalid' END IN ('unknown','invalid')")
        self.db.execute('CREATE INDEX IF NOT EXISTS request_target_symbol ON request_targets(symbol)')
        self.db.execute('CREATE TABLE IF NOT EXISTS entry_common_stops (id TEXT PRIMARY KEY, digest TEXT, result TEXT)')
        self.db.execute('CREATE TABLE IF NOT EXISTS entry_common_stop_targets (command_id TEXT, symbol TEXT, ticket TEXT, PRIMARY KEY(command_id,symbol,ticket))')
        self.db.execute('CREATE INDEX IF NOT EXISTS entry_common_stop_target_lookup ON entry_common_stop_targets(symbol,ticket)')
        self.db.execute('CREATE TABLE IF NOT EXISTS entry_common_stop_migrations (id TEXT PRIMARY KEY)')
        if not self.db.execute('SELECT 1 FROM entry_common_stop_migrations WHERE id=?', ('targets-v1',)).fetchone():
            # Upgrade the old result-only ledger once, before entry processing
            # starts. New intents maintain this small unknown-target index in
            # the same transaction as their outcome; the hot path never parses
            # completed historical JSON.
            for command_id, text in self.db.execute('SELECT id,result FROM entry_common_stops'):
                result = json.loads(text)
                if result.get('status') == 'unknown':
                    self.db.executemany('INSERT OR IGNORE INTO entry_common_stop_targets VALUES (?,?,?)',
                        [(command_id, str(result.get('symbol', '')), str(row['ticket']))
                         for row in result.get('positions', []) if row.get('ticket') is not None])
            self.db.execute('INSERT INTO entry_common_stop_migrations VALUES (?)', ('targets-v1',))
        self.db.commit()
        self.linked = {}
        self.rule_stop = threading.Event()
        # A crash after claiming a rule never rearms it, even if submission had
        # not started. Receipt reconciliation can prove a known outcome later.
        for identifier, in ([] if config.get('_coordinatorOnly') else self.db.execute("SELECT id FROM close_rules WHERE state='firing'").fetchall()):
            row = self.db.execute('SELECT result FROM requests WHERE id=?', ('rule:'+identifier,)).fetchone()
            result = json.loads(row[0]) if row else {'status':'unknown', 'reason':'Receiver interrupted during trigger; inspect MT5'}
            self.set_rule_state(identifier, 'completed' if result['status']=='ok' else result['status'], result)
        self.started = time.perf_counter()
        self.frames = self.frame_bytes = 0
        self.quote_work = 0.
        self.resource_sample = (self.started, time.process_time())
        self.quote_fast_until = 0.
        # Refresh at most weekly in the normal quote path. Parameter refusals
        # invalidate only the affected instrument, never add a polling loop.
        self.instrument_metadata = {}
        self.spec_checked_at = {}
        self.spec_refusal_at = {}
        self.closed_history_cache = {}
        self.opening_link_cache = {}
        self.time_rule_check_at = 0.
        self.time_rule_reconcile_at = 0.
        self.time_rule_reconcile_cursor = 0
        self.terminal_probe_at = 0.
        self.terminal_reconnect_at = 0.
        for identifier, text in ([] if config.get('_coordinatorOnly') else self.db.execute("SELECT id,payload FROM time_rules WHERE state IN ('opening','firing')").fetchall()):
            payload=json.loads(text)
            request_id=payload.get('commandId',identifier)
            receipt=self.db.execute('SELECT result FROM requests WHERE id=?',(request_id,)).fetchone()
            result=json.loads(receipt[0]) if receipt else {'status':'unknown','reason':'Receiver interrupted; inspect MT5'}
            if result['status']=='ok':
                if request_id==identifier: payload['ticket']=str(result['providerOrderId'])
                else: payload.setdefault('actions',{})[request_id]=result
                self.write_time_rule(identifier,payload,'armed',result)
            else: self.write_time_rule(identifier,payload,'unknown',result)
        from chunk_entry import ChunkEntries, EntryDiagnostics
        role='coordinator' if config.get('_coordinatorOnly') else 'mt5-'+str(config['account'])
        self.entry_diagnostics=EntryDiagnostics(pathlib.Path(database).resolve().parent/'logs'/'entry-diagnostics'/(role+'.jsonl'),config['account'])
        self.chunk_entries = ChunkEntries(self,restore=not config.get('_nativeWorker') and not config.get('_coordinatorOnly'))

    def quote_interval(self, now=None):
        return .05 if (time.perf_counter() if now is None else now) < self.quote_fast_until else .1

    def set_quote_mode(self, payload, now=None):
        if not isinstance(payload, dict) or set(payload) != {'intervalMs'} or type(payload['intervalMs']) is not int or payload['intervalMs'] not in (50, 100):
            raise ValueError('Quote interval must be 50 or 100 milliseconds')
        # The Mac owns LT proximity. A short lease restores normal cadence if
        # the app stops refreshing it; this never submits a trading request.
        response = self.envelope(intervalMs=payload['intervalMs'], leaseSeconds=15)
        self.quote_fast_until = ((time.perf_counter() if now is None else now)+15) if payload['intervalMs'] == 50 else 0.
        return response

    def diagnostics(self):
        now, cpu = time.perf_counter(), time.process_time()
        previous, previous_cpu = self.resource_sample
        self.resource_sample = (now, cpu)
        elapsed = max(now - previous, .001)
        uptime = max(now - self.started, .001)
        resources = {'receiverCpuPercentOneCore': round((cpu-previous_cpu)/elapsed*100, 2),
                     'sampleSeconds': round(elapsed, 2), 'receiverMemoryMB': None}
        if os.name == 'nt':
            import ctypes
            from ctypes import wintypes
            class Counters(ctypes.Structure):
                _fields_ = [('cb', wintypes.DWORD), ('PageFaultCount', wintypes.DWORD)] + [
                    (name, ctypes.c_size_t) for name in ('PeakWorkingSetSize', 'WorkingSetSize',
                    'QuotaPeakPagedPoolUsage', 'QuotaPagedPoolUsage', 'QuotaPeakNonPagedPoolUsage',
                    'QuotaNonPagedPoolUsage', 'PagefileUsage', 'PeakPagefileUsage')]
            counters = Counters()
            counters.cb = ctypes.sizeof(counters)
            kernel = ctypes.WinDLL('kernel32', use_last_error=True)
            kernel.GetCurrentProcess.restype = wintypes.HANDLE
            psapi = ctypes.WinDLL('psapi', use_last_error=True)
            psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD]
            if psapi.GetProcessMemoryInfo(kernel.GetCurrentProcess(), ctypes.byref(counters), counters.cb):
                resources['receiverMemoryMB'] = round(counters.WorkingSetSize/1048576, 2)
        terminal = self.read_timings.call('terminalInfo', self.mt5.terminal_info)
        benchmark=None
        try:
            if self.preparation_benchmark_file.stat().st_size<=65536:
                benchmark=json.loads(self.preparation_benchmark_file.read_text(encoding='utf-8'))
        except (OSError,ValueError): pass
        publisher=getattr(self,'quote_publisher',None)
        return self.envelope(build=self.runtime_build, resources=resources, preparationBenchmark=benchmark, readTimings=self.read_timings.snapshot(),
            entryDiagnosticLog=self.entry_diagnostics.snapshot(),
            linkedWorkerRPC={account:worker.rpc_snapshot() for account,worker in self.linked.items() if callable(getattr(worker,'rpc_snapshot',None))},
            quoteFreshness=copy.deepcopy(self.quote_freshness), quotePublisher=publisher.snapshot() if publisher else None, quoteStream={
            'frames': self.frames, 'averageBytesPerFrame': round(self.frame_bytes/max(self.frames, 1)),
            'averageWorkMsPerFrame': round(self.quote_work/max(self.frames, 1)*1000, 2),
            'averageBytesPerSecond': round(self.frame_bytes/uptime),
            'intervalMs': round(self.quote_interval()*1000)},
            brokerPingMicroseconds=getattr(terminal, 'ping_last', None))

    def account(self, timing=None):
        account = measured(timing, 'accountInfoMs', self.read_timings.call, 'accountInfo', self.mt5.account_info)
        terminal = measured(timing, 'terminalInfoMs', self.read_timings.call, 'terminalInfo', self.mt5.terminal_info)
        if not account or not terminal or not terminal.connected:
            raise ValueError('MT5 is disconnected')
        if account.login != int(self.config['account']) or account.server != self.config['server']:
            raise ValueError('MT5 account or server mismatch')
        if account.currency != 'USD':
            raise ValueError('The Mac risk calculator requires a USD account')
        return account

    def envelope(self, **values):
        account = self.account()
        return {'account': account.login, 'server': account.server, **values}

    def snapshot(self, symbol):
        return self._snapshot(symbol, self.account())

    @serialized
    def entry_snapshot(self, symbol):
        """One fresh account/quote batch for an internal entry round only."""
        return self._entry_snapshot_for(symbol,self._entry_read_account())

    def _entry_read_account(self):
        account=getattr(self.entry_read_context,'account',None)
        return account if account is not None else self.account()

    def _entry_snapshot_for(self, symbol, account):
        if getattr(account, 'margin_mode', None) != 2:
            raise ValueError('Chunk entry requires a hedging account on every participating terminal')
        if getattr(account, 'trade_allowed', True) is False or getattr(account, 'trade_expert', True) is False:
            raise ValueError('Account does not permit algorithmic trading')
        return self._snapshot(symbol, account)

    @serialized
    def entry_context(self, symbol, watch=()):
        # One account mutex/RPC. Collect proof first and quote last, so a slow
        # position read cannot age the quote before the coordinator receives it.
        started = time.perf_counter()
        account = self.account()
        previous=getattr(self.entry_read_context,'account',None)
        self.entry_read_context.account=account
        try:
            proof = self._entry_positions_for(watch,account) if watch else None
            snapshot = self.entry_snapshot(symbol)
        finally:self.entry_read_context.account=previous
        return dict(snapshot=snapshot, proof=proof,
            contextMs=round((time.perf_counter()-started)*1000, 3))

    @serialized
    def entry_admission(self, payload):
        """One account-local read/preflight batch; no intent or broker send."""
        account=self.account()
        if (not self.config.get('tradingEnabled',False) or getattr(account,'margin_mode',None)!=2
                or getattr(account,'trade_allowed',True) is False or getattr(account,'trade_expert',True) is False):
            raise ValueError('Account does not permit independent hedged entry')
        snapshot=self._entry_snapshot_for(payload['symbol'],account)
        spec={key:snapshot['metadata'][key] for key in ('quantityStep','minQty','maxQty')}
        from chunk_entry import ChunkEntries
        state=dict(accounts={str(account.login):dict(parts=[payload['chunkVolume']])})
        request=ChunkEntries._child(SimpleNamespace(primary=str(account.login)),payload,state,0,payload['trigger'])
        self.open_request(request)
        self.validate_deadline(request)
        return dict(identity=worker_result('account',account),snapshot=snapshot,spec=spec,checkedRequest=request)

    def tick_utc_seconds(self, tick):
        # Convert the configured broker wall clock to UTC.
        # is UTC regardless of the Windows/Mac display timezone. Convert only
        # the configured broker clock; never infer an offset from a stale tick.
        stamp = getattr(tick, 'time_msc', 0)/1000 or getattr(tick, 'time', 0)
        return stamp - self.broker_utc_offset_seconds

    def observe_quote(self, symbol, tick, source, captured_at, read_ms):
        """Bounded evidence from existing native reads; never refresh a tick."""
        try:
            stamp = self.tick_utc_seconds(tick) * 1000
            age = captured_at - stamp
            if not math.isfinite(stamp) or not math.isfinite(age):
                stamp = age = None
        except (TypeError, ValueError, OverflowError):
            stamp = age = None
        sample = dict(nativeTickUtcMs=stamp, nativeCapturedAtMs=captured_at,
                      nativeAgeMs=age,
                      readMs=round(read_ms, 3), source=source)
        state = self.quote_freshness.setdefault(symbol, dict(samples=0, staleSamples=0))
        state['samples'] += 1
        state['latest'] = sample
        if age is None or not -250 <= age <= 1000:
            state['staleSamples'] += 1
            state['lastStale'] = sample
        return sample

    def _expire_specification(self, symbol, force=False):
        now=time.monotonic();checked=self.spec_checked_at.get(symbol)
        if force or checked is not None and now-checked>=7*24*3600:
            self.instrument_metadata.pop(symbol,None)
            getattr(self,'volume_specs',{}).pop(symbol,None)
            self.spec_checked_at.pop(symbol,None)

    def _specification_refused(self, symbol, retcode):
        if symbol in self.config['symbols'] and retcode in (10014,10015,10016):
            now=time.monotonic()
            if now-self.spec_refusal_at.get(symbol,float('-inf'))>=1:
                self.spec_refusal_at[symbol]=now
                self._expire_specification(symbol,force=True)

    def lot_specs(self):
        for symbol in self.config['symbols']:self._expire_specification(symbol)
        if not hasattr(self,'volume_specs'):
            self.volume_specs={}
        for symbol in self.config['symbols']:
            if symbol not in self.volume_specs:
                metadata=self.instrument_metadata.get(symbol)
                if metadata: self.volume_specs[symbol]={key:metadata[key] for key in ('quantityStep','minQty','maxQty')}
                else:
                    if not self.mt5.symbol_select(symbol,True): raise ValueError('Broker symbol not found')
                    info=self.mt5.symbol_info(symbol)
                    if not info: raise ValueError('Cannot read broker lot specifications')
                    self.volume_specs[symbol]=dict(quantityStep=info.volume_step,minQty=info.volume_min,maxQty=info.volume_max)
                    self.spec_checked_at.setdefault(symbol,time.monotonic())
        return self.volume_specs

    def quote_frame(self):
        account = self.account()
        snapshots = {symbol: self._snapshot(symbol, account) for symbol in self.config['symbols']}
        return {'account': account.login, 'server': account.server, 'snapshots': snapshots,
                'capturedAt': time.time() * 1000, 'intervalMs': round(self.quote_interval() * 1000)}

    def _snapshot(self, symbol, account):
        if symbol not in self.config['symbols']:
            raise ValueError('Only SPX500 and USTEC are allowed')
        self._expire_specification(symbol)
        metadata = self.instrument_metadata.get(symbol)
        if metadata is None and not self.mt5.symbol_select(symbol, True):
            raise ValueError('Broker symbol not found')
        read_started = time.perf_counter()
        tick = self.read_timings.call('quoteTick', self.mt5.symbol_info_tick, symbol)
        captured_at = time.time() * 1000
        if not tick or not math.isfinite(tick.bid) or not math.isfinite(tick.ask) or tick.bid <= 0 or tick.ask < tick.bid:
            raise ValueError('No valid broker quote')
        observation = self.observe_quote(symbol, tick, 'snapshot', captured_at, (time.perf_counter()-read_started)*1000)
        if metadata is None:
            info = self.mt5.symbol_info(symbol)
            if not info:
                raise ValueError('Cannot obtain broker instrument specifications')
            value = self.mt5.order_calc_profit(self.mt5.ORDER_TYPE_BUY, symbol, 1., tick.ask, tick.ask + 1.)
            if value is None or not math.isfinite(value) or value <= 0:
                raise ValueError('Cannot obtain USD value of one index point')
            metadata = {'tickSize': info.trade_tick_size, 'quantityStep': info.volume_step,
                        'minQty': info.volume_min, 'maxQty': info.volume_max, 'contractSize': value}
            if any(not math.isfinite(number) or number <= 0 for number in metadata.values()) or metadata['maxQty'] < metadata['minQty']:
                raise ValueError('Invalid broker instrument specifications')
            self.instrument_metadata[symbol] = metadata
            self.spec_checked_at[symbol]=time.monotonic()
            if hasattr(self,'volume_specs'):
                self.volume_specs[symbol]={key:metadata[key] for key in ('quantityStep','minQty','maxQty')}
        return {'account': account.login, 'server': account.server,
                'quote': {'bid': tick.bid, 'ask': tick.ask, 'price': (tick.bid + tick.ask) / 2, 'tickSize': metadata['tickSize'],
                          'time': tick.time, 'timeMsc': getattr(tick, 'time_msc', 0),
                          'utcTime': self.tick_utc_seconds(tick), 'brokerUtcOffsetSeconds': self.broker_utc_offset_seconds,
                          'nativeCapturedAt': captured_at, 'nativeAgeMs': observation['nativeAgeMs']},
                'metadata': dict(metadata)}

    def place(self, payload):
        return self.execute('place', payload)

    @serialized
    def check_opening(self, payload):
        # Internal worker RPC, never an HTTP trading endpoint. Both terminals
        # must pass this read-only barrier before either submission can begin.
        account=self.account()
        if not self.config.get('tradingEnabled',False): raise ValueError('MT5 receiver is in read-only mode')
        self.open_request(payload)
        self.validate_deadline(payload)
        if 'closeAtUtc' in payload and getattr(account,'margin_mode',2)!=2:
            raise ValueError('Timed exit requires a hedging account with a separate position')
        return True

    @serialized
    def execute(self, operation, payload, received=None):
        started = time.perf_counter()
        timing = {'receivedAt': received[0] if received else time.time()*1000,
                  'queueMs': round((started-received[1])*1000, 2) if received else 0.}
        def timed(status):
            timing['totalMs'] = round((time.perf_counter()-started)*1000 + timing['queueMs'], 2)
            reply_started=time.perf_counter()
            response=self.envelope(result=status)
            if (operation=='place' and payload.get('type')=='market' and payload.get('positionProofRequested') is True and 'entryGuard' in payload
                    and status.get('status')=='ok' and status.get('filledVolume')==request['volume']
                    and re.fullmatch(r'[1-9][0-9]{0,19}',str(status.get('providerOrderId','')))):
                proof_started=time.perf_counter()
                try:
                    # The envelope has just validated the real native identity.
                    # Evidence stays in this response, never in the durable
                    # opening receipt or a replay after restart.
                    evidence=self._entry_positions_for([dict(ticket=status['providerOrderId'],symbol=payload['symbol'])],
                        SimpleNamespace(login=response['account'],server=response['server']))
                    response['positionProof']=dict(evidence,schema='entry-opening-proof-v1',commandId=identifier)
                except Exception:pass  # A missing proof takes the existing fresh-read path.
                timing['positionProofMs']=round((time.perf_counter()-proof_started)*1000,3)
            timing['replyValidationMs']=round((time.perf_counter()-reply_started)*1000,2)
            timing['totalMs']=round((time.perf_counter()-started)*1000+timing['queueMs'],2)
            response['serverTiming']=dict(timing)
            return response
        normalize_started=time.perf_counter()
        identifier = str(payload.get('id', ''))
        if not re.fullmatch(r'[A-Za-z0-9_.:-]{1,160}', identifier):
            raise ValueError('Missing or invalid request ID')
        core = {key: value for key, value in payload.items() if key != 'expiresAt'}
        # Keep existing opening digests compatible with the installed ledger.
        if operation != 'place':
            core['operation'] = operation
        digest = hashlib.sha256(json.dumps(core, sort_keys=True, allow_nan=False).encode()).hexdigest()
        timing['normalizeMs']=round((time.perf_counter()-normalize_started)*1000,2)
        previous = measured(timing,'journalLookupMs',lambda:self.db.execute('SELECT digest,result FROM requests WHERE id=?', (identifier,)).fetchone())
        if previous:
            if previous[0] != digest:
                raise ValueError('Request ID was already used with different parameters')
            return self.envelope(result=json.loads(previous[1]))
        rejected = lambda message: timed({'status': 'rejected', 'reason': message})
        account=measured(timing,'accountCheckMs',self.account,timing)
        if not self.config.get('tradingEnabled', False):
            return rejected('MT5 receiver is in read-only mode')
        expires = float(payload.get('expiresAt', 0))
        remaining = expires - time.time() * 1000
        if not math.isfinite(remaining) or remaining <= 0 or remaining > 3000:
            return rejected('Command expired or client clock is incorrect')
        if operation in ('schedule-close', 'cancel-close'):
            try:
                # Both records are committed atomically: retrying a lost reply
                # returns the arm/cancel receipt without creating another rule.
                status = self.rule_command(operation, payload)
                self.db.execute('INSERT INTO requests VALUES (?,?,?)', (identifier, digest, json.dumps(status)))
                self.db.commit()
                return timed(status)
            except (ValueError, TypeError, KeyError) as error:
                self.db.rollback()
                return rejected(str(error))
        try:
            if operation != 'place':
                self.check_rule_unknown(payload)
            request = measured(timing,'requestBuildMs',self.open_request,payload) if operation == 'place' else measured(timing,'requestBuildMs',self.control_request,operation,payload,timing)
            if operation=='place':
                self.validate_deadline(payload)
                if 'closeAtUtc' in payload and getattr(account,'margin_mode',2)!=2:
                    raise ValueError('Timed exit requires a hedging account with a separate position')
        except (ValueError, TypeError, KeyError) as error:
            details = getattr(error, 'entry_diagnostics', None)
            if details is not None:
                return timed(dict(status='rejected', reason=str(error), entryDiagnostics=details))
            return rejected(str(error))
        if time.time() * 1000 >= expires:
            return rejected('Command expired before submission')
        unknown = {'status': 'unknown', 'reason': 'Submission recorded; verify receipt with MT5'}
        if operation != 'place':
            # A pending-order removal cannot close or change an already filled
            # position. Retain the operation even when its transport reply is lost.
            unknown.update(operation=operation, ticket=str(payload['ticket']))
        ledger_started = time.perf_counter()
        timing['prepareMs'] = round((ledger_started-started)*1000, 2)
        timing['prepareOtherMs']=round(max(0,timing['prepareMs']-sum(timing.get(key,0.) for key in ('normalizeMs','journalLookupMs','accountCheckMs','requestBuildMs'))),2)
        self.db.execute('INSERT INTO requests VALUES (?,?,?)', (identifier, digest, json.dumps(unknown)))
        if operation!='place': self.db.execute('INSERT INTO request_targets VALUES (?,?,?)',(identifier,str(payload['ticket']),payload['symbol']))
        if operation=='place' and 'closeAtUtc' in payload:
            deadline={key:payload[key] for key in ('symbol','closeAtUtc','closeTimeZone','closeLocalDate','closeLocalTime','cancelPendingAtDeadline')}
            deadline.update(openingId=identifier,createdAt=time.time()*1000,actions={})
            self.db.execute('INSERT INTO time_rules VALUES (?,?,?,?)',(identifier,json.dumps(deadline),'opening',json.dumps(unknown)))
        self.db.commit()
        timing['ledgerBeforeMs'] = round((time.perf_counter()-ledger_started)*1000, 2)
        if time.time()*1000 >= expires:
            status={'status':'rejected','reason':'Command expired before broker submission'}
            self.db.execute('UPDATE requests SET result=? WHERE id=?',(json.dumps(status),identifier))
            if operation=='place' and 'closeAtUtc' in payload:
                self.db.execute('UPDATE time_rules SET state=?,result=? WHERE id=?',('rejected',json.dumps(status),identifier))
            self.db.commit()
            return timed(status)
        if operation=='place' and 'entryGuard' in payload:
            try:
                # Broker-terminal data is read again after the durable intent,
                # immediately before sending. Never reuse a Mac quote here.
                self.guard_chunk_request(payload,request)
                self.account()
                now=time.time()*1000
                if now>=expires or ('closeAtUtc' in payload and now>=float(payload['closeAtUtc'])):
                    raise ValueError('Chunk command expired during final validation')
            except (ValueError,TypeError,KeyError) as error:
                status={'status':'rejected','reason':str(error)}
                details = getattr(error, 'entry_diagnostics', None)
                if details is not None:
                    status['entryDiagnostics'] = details
                self.db.execute('UPDATE requests SET result=? WHERE id=?',(json.dumps(status),identifier))
                if 'closeAtUtc' in payload:
                    self.db.execute('UPDATE time_rules SET state=?,result=? WHERE id=?',('rejected',json.dumps(status),identifier))
                self.db.commit()
                return timed(status)
        try:
            if operation == 'place' and 'entryGuard' in payload:
                unknown.update(submittedPrice=request['price'], sl=request['sl'], tp=request['tp'])
                if payload['entryGuard'].get('supportReprice') is True:
                    unknown['supportRepriced'] = True
            timing['mt5StartedAt'] = time.time()*1000
            mt5_started = time.perf_counter()
            diagnostic=self.entry_diagnostics.begin('order-send:'+operation+(':'+str(payload.get('type','market')) if operation=='place' else ''),
                entry_id=re.sub(r':h[0-9]+$','',identifier) if 'entryGuard' in payload else identifier,
                symbol=payload.get('symbol'),reason='Waiting for native MT5 order_send result',announce=True)
            try:
                result = self.mt5.order_send(request)
            except Exception as error:
                self.entry_diagnostics.finish(diagnostic,error=error)
                raise
            else:
                self.entry_diagnostics.finish(diagnostic,retcode=getattr(result,'retcode',None))
            timing['mt5ReturnedAt'] = time.time()*1000
            timing['mt5Ms'] = round((time.perf_counter()-mt5_started)*1000, 2)
            if result is None:
                self.db.execute('UPDATE requests SET result=? WHERE id=?', (json.dumps(unknown), identifier))
                self.db.commit()
                return timed(unknown)
            if result.retcode in (self.mt5.TRADE_RETCODE_DONE, self.mt5.TRADE_RETCODE_PLACED,
                                   getattr(self.mt5, 'TRADE_RETCODE_NO_CHANGES', 10025)):
                status = {'status': 'ok', 'providerOrderId': str(result.order), 'filledVolume': result.volume, 'fillPrice': result.price, 'retcode': result.retcode}
                if operation == 'place' and 'stopDistance' in payload.get('entryGuard', {}):
                    status.update(submittedPrice=request['price'], sl=request['sl'])
                if operation == 'place' and payload.get('entryGuard', {}).get('supportReprice') is True:
                    status.update(submittedPrice=request['price'], sl=request['sl'], tp=request['tp'],
                                  supportRepriced=True)
            elif result.retcode in (self.mt5.TRADE_RETCODE_TIMEOUT,
                                    getattr(self.mt5, 'TRADE_RETCODE_CONNECTION', 10031),
                                    getattr(self.mt5, 'TRADE_RETCODE_DONE_PARTIAL', 10010)):
                status = {**unknown, 'retcode': result.retcode, 'providerOrderId': str(result.order)}
            else:
                status = {'status': 'rejected', 'reason': str(result.comment), 'retcode': result.retcode}
                self._specification_refused(payload.get('symbol'),result.retcode)
            if operation != 'place':
                status.update(operation=operation, ticket=str(payload['ticket']))
            elif 'closeAtUtc' in payload:
                status.update(closeAtUtc=payload['closeAtUtc'],closeLocalDate=payload['closeLocalDate'],closeLocalTime=payload['closeLocalTime'])
            timing['totalMs'] = round((time.perf_counter()-started)*1000+timing['queueMs'], 2)
            status['timings'] = {'server': dict(timing)}
            ledger_started = time.perf_counter()
            self.db.execute('UPDATE requests SET result=? WHERE id=?', (json.dumps(status), identifier))
            if operation=='place' and 'closeAtUtc' in payload:
                deadline['ticket']=str(status.get('providerOrderId',''))
                self.db.execute('UPDATE time_rules SET payload=?,state=?,result=? WHERE id=?',
                    (json.dumps(deadline),'armed' if status['status']=='ok' else status['status'],json.dumps(status),identifier))
            self.db.commit()
            timing['ledgerAfterMs'] = round((time.perf_counter()-ledger_started)*1000, 2)
            return timed(status)
        except Exception:
            if 'mt5_started' in locals() and 'mt5Ms' not in timing:
                timing['mt5Ms'] = round((time.perf_counter()-mt5_started)*1000, 2)
            try:
                self.db.execute('UPDATE requests SET result=? WHERE id=?', (json.dumps(unknown), identifier))
                self.db.commit()
            except Exception:
                self.db.rollback()
            return timed(unknown)

    def check_rule_unknown(self, payload):
        for identifier,text in self.db.execute('SELECT r.id,r.result FROM requests r JOIN request_targets t ON t.id=r.id WHERE t.ticket=? AND t.symbol=?',
                                              (str(payload.get('ticket')),payload.get('symbol'))):
            pending_result = json.loads(text)
            if (identifier!=payload.get('id') and pending_result.get('status')=='unknown'
                    and pending_result.get('operation') != 'cancel'):
                raise ValueError('Unconfirmed management command; inspect MT5 before further management')
        for identifier, text in self.db.execute("SELECT id,payload FROM close_rules WHERE state IN ('unknown','firing')"):
            if str(json.loads(text)['ticket']) == str(payload.get('ticket')) and payload.get('id') != 'rule:'+identifier:
                raise ValueError('Unconfirmed price-triggered close; inspect MT5 before further management')
        for identifier,text in self.db.execute("SELECT id,payload FROM time_rules WHERE state IN ('unknown','firing','opening')"):
            rule=json.loads(text)
            if (str(rule.get('ticket'))==str(payload.get('ticket')) and payload.get('id')!=rule.get('commandId')
                    and not self._unconfirmed_opening_rule(identifier, rule)):
                raise ValueError('Unconfirmed timed exit; inspect MT5 before further management')

    def _unconfirmed_opening_rule(self, identifier, rule):
        # An unclaimed deadline accompanying an opening is not an unknown
        # close. Its exact opening must still be independently proved before
        # the coordinator can confirm volume or change protection.
        if rule.get('openingId') != identifier or rule.get('actions') or rule.get('commandId'):
            return False
        row = self.db.execute('SELECT result FROM requests WHERE id=?', (identifier,)).fetchone()
        receipt = json.loads(row[0]) if row else {}
        return (receipt.get('status') == 'unknown' and not receipt.get('operation')
                and re.fullmatch(r'[1-9][0-9]{0,19}', str(receipt.get('providerOrderId', ''))) is not None
                and str(receipt['providerOrderId']) == str(rule.get('ticket')))

    @staticmethod
    def validate_deadline(payload):
        if 'closeAtUtc' not in payload: return
        from datetime import datetime, timezone
        due=payload['closeAtUtc']
        if type(due) not in (int,float) or not math.isfinite(due) or not time.time()*1000<due<=time.time()*1000+26*3600000:
            raise ValueError('Timed exit must be in the future, within 26 hours')
        local=datetime.fromtimestamp(due/1000,timezone.utc).astimezone(new_york_zone())
        if (payload.get('closeTimeZone')!='America/New_York' or payload.get('cancelPendingAtDeadline') is not True
            or local.strftime('%Y-%m-%d')!=payload.get('closeLocalDate') or local.strftime('%H:%M')!=payload.get('closeLocalTime')
            or local.second or local.microsecond):
            raise ValueError('Timed exit date does not match New York clock')

    def write_time_rule(self, identifier, payload, state, result):
        self.db.execute('UPDATE time_rules SET payload=?,state=?,result=? WHERE id=?',
                        (json.dumps(payload),state,json.dumps(result),identifier))
        self.db.commit()

    def time_rules(self):
        return [dict(id=i,**json.loads(p),state=s,result=json.loads(r)) for i,p,s,r in
                self.db.execute('SELECT * FROM time_rules ORDER BY rowid DESC LIMIT 100')]

    def opening_rows(self, ticket, symbol, rows, evidence=None):
        matched=[x for x in rows if x['symbol']==symbol and str(ticket) in (str(x['ticket']),str(x.get('identifier')))]
        if matched and all(x['__isPosition'] for x in matched): return matched
        # A partial pending order can still be absent from order history. Its
        # live position_id links the remainder to the hedging position now.
        live_ids = {str(row.get('positionId', '0')) for row in matched if not row['__isPosition']}
        live_ids.discard('0')
        live_ids.discard('None')
        linked = [row for row in rows if row['symbol']==symbol and row['__isPosition']
                  and live_ids.intersection((str(row['ticket']), str(row.get('identifier'))))]
        if linked:
            for row in linked:
                if row not in matched:
                    matched.append(row)
            return matched
        # A final broker-confirmed result is immutable, but always match the
        # current position identifier first. Never cache a live/unknown state.
        closed=self.closed_history_cache.get((str(ticket),symbol))
        if closed is not None:
            position_id=closed.get('positionId')
            for row in rows:
                if row['symbol']==symbol and row['__isPosition'] and position_id and str(position_id) in (str(row['ticket']),str(row.get('identifier'))) and row not in matched:
                    matched.append(row)
            return matched
        # A pending remainder can coexist with a position after a partial fill.
        # Its history is needed only when such a position could be present.
        if matched and not any(x['symbol']==symbol and x['__isPosition'] for x in rows): return matched
        key=(str(self.config['account']),self.config['server'],symbol,str(ticket))
        position_id=self.opening_link_cache.get(key)
        if position_id:
            linked=[row for row in rows if row['symbol']==symbol and row['__isPosition']
                    and position_id in (str(row['ticket']),str(row.get('identifier')))]
            if linked:return matched+[row for row in linked if row not in matched]
        history=self.read_timings.call('orderHistory', self.mt5.history_orders_get, ticket=int(ticket))
        if evidence is not None: evidence['history']=history
        position_id=self._remember_opening_link(ticket,symbol,history)
        for row in rows:
            if row['symbol']==symbol and row['__isPosition'] and position_id!='0' and position_id in (str(row['ticket']),str(row.get('identifier'))) and row not in matched:
                matched.append(row)
        return matched

    def _remember_opening_link(self, ticket, symbol, history):
        matches=[row for row in history or () if str(row.ticket)==str(ticket) and row.symbol==symbol]
        position_id=str(getattr(matches[0],'position_id',0)) if len(matches)==1 else '0'
        if re.fullmatch(r'[1-9][0-9]{0,19}',position_id):
            key=(str(self.config['account']),self.config['server'],symbol,str(ticket))
            previous=self.opening_link_cache.get(key)
            if previous is not None and previous!=position_id:
                raise ValueError('Exact broker opening-to-position link changed')
            self.opening_link_cache[key]=position_id
            if len(self.opening_link_cache)>4096:self.opening_link_cache.pop(next(iter(self.opening_link_cache)))
        return position_id

    def opening_state(self, ticket, symbol, rows, evidence=None):
        # Absence alone never proves a close. Link the opening order to its
        # position identifier and require a balanced deal history or cancellation.
        if not re.fullmatch(r'[1-9][0-9]{0,19}',str(ticket)) or symbol not in self.config['symbols']:
            raise ValueError('Invalid opening ticket or symbol')
        evidence={} if evidence is None else evidence
        matches=evidence['matches'] if 'matches' in evidence else self.opening_rows(ticket,symbol,rows,evidence)
        if matches:
            row=matches[0]
            return {'phase':'opened' if row['__isPosition'] else 'waiting','ticket':row['ticket']}
        for text, in self.db.execute("SELECT r.result FROM requests r INDEXED BY request_unknown_ticket JOIN request_targets t ON r.id=t.id WHERE t.symbol=? AND CASE WHEN json_valid(r.result) THEN json_extract(r.result,'$.status') ELSE 'invalid' END IN ('unknown','invalid')",(symbol,)):
            result=json.loads(text)
            if result.get('status')=='unknown' and result.get('operation') != 'cancel':
                return {'phase':'unknown','reason':'Unconfirmed management command'}
        for text, in self.db.execute("SELECT result FROM requests WHERE CASE WHEN json_valid(result) THEN json_extract(result,'$.status') ELSE 'invalid' END IN ('unknown','invalid') AND (CASE WHEN json_valid(result) THEN json_extract(result,'$.ticket') END=? OR NOT json_valid(result))",(str(ticket),)):
            result=json.loads(text)
            if result.get('status')=='unknown' and result.get('operation') != 'cancel' and str(result.get('ticket'))==str(ticket): return {'phase':'unknown','reason':'Unconfirmed management command'}
        for text,result in self.db.execute("SELECT payload,result FROM groups WHERE CASE WHEN json_valid(result) THEN json_extract(result,'$.status') ELSE 'invalid' END IN ('unknown','invalid') AND (CASE WHEN json_valid(payload) THEN json_extract(payload,'$.payload.ticket') END=? OR NOT json_valid(payload) OR NOT json_valid(result))",(str(ticket),)):
            group=json.loads(text)
            if json.loads(result).get('status')=='unknown' and str(group['payload'].get('ticket'))==str(ticket):
                return {'phase':'unknown','reason':'Unconfirmed linked command'}
        for table in ('close_rules','time_rules'):
            for identifier,text in self.db.execute("SELECT id,payload FROM "+table+" WHERE state IN ('unknown','firing','opening')"):
                if table == 'time_rules' and self._unconfirmed_opening_rule(identifier, json.loads(text)):
                    continue
                if str(json.loads(text).get('ticket'))==str(ticket): return {'phase':'unknown','reason':'Unconfirmed exit condition'}
        key=(str(ticket),symbol)
        if key in self.closed_history_cache: return self.closed_history_cache[key]
        # Reuse this read's evidence, not a cached active order: previously an
        # absent opening performed the same terminal history RPC twice.
        history=evidence['history'] if 'history' in evidence else self.read_timings.call('orderHistory', self.mt5.history_orders_get, ticket=int(ticket))
        if history is None: return {'phase':'unknown','reason':'Cannot read broker order history'}
        order=next((x for x in history if str(x.ticket)==str(ticket) and x.symbol==symbol),None)
        if not order: return {'phase':'unknown','reason':'Opening not found in broker history'}
        position_id=getattr(order,'position_id',0)
        if position_id:
            # An identifier can differ from the current hedging position ticket.
            if any(row['symbol']==symbol and str(row.get('identifier'))==str(position_id) for row in rows):
                return {'phase':'unknown','reason':'Opening position still present with another ticket'}
            deals=self.read_timings.call('dealHistory', self.mt5.history_deals_get, position=int(position_id))
            if deals is None: return {'phase':'unknown','reason':'Cannot read broker deal history'}
            deals=[x for x in deals if x.symbol==symbol and int(x.position_id)==int(position_id)]
            incoming=sum(x.volume for x in deals if x.entry==0)
            outgoing=sum(x.volume for x in deals if x.entry in (1,3))
            if incoming>0 and abs(incoming-outgoing)<1e-8 and all(x.entry in (0,1,3) for x in deals):
                result={'phase':'closed','positionId':str(position_id),'reason':'Broker confirms full exit',
                        'exitReasons':list({getattr(x,'reason',None) for x in deals if x.entry in (1,3)})}
                proof = self._closed_opening_proof(ticket, symbol, position_id, deals, order)
                if proof is not None:
                    result['openingProof'] = proof
                broker_evidence = opening_order_evidence(order, ticket, symbol,
                    fill_price=proof.get('fillPrice') if proof else None)
                if broker_evidence:
                    result['brokerOrderEvidence'] = broker_evidence
            else: return {'phase':'unknown','reason':'No balanced full exit in broker history'}
        elif getattr(order,'state',None) in (getattr(self.mt5,'ORDER_STATE_CANCELED',2),getattr(self.mt5,'ORDER_STATE_EXPIRED',6),getattr(self.mt5,'ORDER_STATE_REJECTED',5)):
            result={'phase':'cancelled','reason':'Broker confirms pending order ended'}
        else: return {'phase':'unknown','reason':'Opening outcome unconfirmed'}
        self.closed_history_cache[key]=result
        return result

    @staticmethod
    def _closed_opening_proof(ticket, symbol, position_id, deals, order):
        """Exact entry VWAP for a fully closed hedging opening, never absence."""
        try:
            incoming = [row for row in deals if row.entry == 0]
            outgoing = [row for row in deals if row.entry in (1, 3)]
            if not incoming or not outgoing or len(deals) > 512 or any(row.entry not in (0,1,3) for row in deals):
                return None
            ids = [int(row.ticket) for row in deals]
            if len(set(ids)) != len(ids) or any(identifier <= 0 for identifier in ids):
                return None
            side = int(incoming[0].type)
            if side not in (0, 1) or any(int(row.type) != side or str(row.order) != str(ticket) for row in incoming):
                return None
            if any(int(row.type) != 1-side for row in outgoing):
                return None
            if any(row.symbol != symbol or str(row.position_id) != str(position_id)
                   or not math.isfinite(float(row.volume)) or float(row.volume) <= 0
                   or not math.isfinite(float(row.price)) or float(row.price) <= 0 for row in deals):
                return None
            volume = sum(float(row.volume) for row in incoming)
            if abs(volume-sum(float(row.volume) for row in outgoing)) > 1e-8:
                return None
            if (str(order.ticket) != str(ticket) or order.symbol != symbol
                    or int(order.type) not in ((0, 2, 4) if side == 0 else (1, 3, 5))
                    or not math.isfinite(float(order.volume_initial)) or volume > float(order.volume_initial)+1e-8
                    or not math.isfinite(float(order.sl)) or float(order.sl) <= 0
                    or not math.isfinite(float(order.tp)) or float(order.tp) < 0):
                return None
            return dict(kind='exact-balanced-opening-v1', openingTicket=str(ticket),
                positionId=str(position_id), symbol=symbol, side='buy' if side == 0 else 'sell',
                volume=volume, fillPrice=sum(float(row.volume)*float(row.price) for row in incoming)/volume,
                sl=float(order.sl), tp=float(order.tp),
                incomingDeals=len(incoming), outgoingDeals=len(outgoing))
        except (AttributeError, ValueError, TypeError, OverflowError):
            return None

    def reconcile_time_rule_history(self):
        """Background history yields between exact tickets to waiting entry RPCs."""
        if self.native_command_waiting.is_set() or time.monotonic() < self.time_rule_reconcile_at:
            return
        with self.lock:
            if self.native_command_waiting.is_set():
                return
            rules = self.db.execute("SELECT id,payload FROM time_rules WHERE state='armed'").fetchall()
            if not rules:
                return
            self.time_rule_reconcile_at = time.monotonic()+2.
            start = self.time_rule_reconcile_cursor % len(rules)
            selected = (rules[start:]+rules[:start])[:4]
        rows, captured, revision = None, 0., -1
        for index, (identifier, text) in enumerate(selected):
            if self.rule_stop.is_set() or self.native_command_waiting.is_set():
                break
            with self.lock:
                if self.native_command_waiting.is_set():
                    break
                current = self.db.execute('SELECT payload,state FROM time_rules WHERE id=?', (identifier,)).fetchone()
                self.time_rule_reconcile_cursor = (start+index+1) % len(rules)
                if current != (text, 'armed'):
                    continue
                payload = json.loads(text)
                if payload['closeAtUtc'] <= time.time()*1000:
                    break  # The next protective pass handles due exits first.
                if (rows is None or revision != self.native_command_revision
                        or time.monotonic()-captured > .25):
                    self.account()
                    rows = self.orders()['orders']
                    captured, revision = time.monotonic(), self.native_command_revision
                state = self.opening_state(payload['ticket'], payload['symbol'], rows)
                if state['phase'] in ('closed', 'cancelled'):
                    self.write_time_rule(identifier, payload, 'completed', {'status':'ok', **state})

    def evaluate_time_rules(self, reconcile_history=True):
        if time.monotonic()<self.time_rule_check_at: return
        self.time_rule_check_at=time.monotonic()+.1
        rules=self.db.execute("SELECT id,payload FROM time_rules WHERE state='armed'").fetchall()
        due=[(i,json.loads(p)) for i,p in rules if json.loads(p)['closeAtUtc']<=time.time()*1000]
        # A closed opening no longer needs its deadline. Reconcile before that
        # deadline too, with the same broker-history and unknown-command checks.
        # Bound history work independently of the quote publisher's cadence.
        if reconcile_history and rules:
            self.reconcile_time_rule_history()
        if not due or not self.config.get('tradingEnabled',False): return
        account=self.account()  # Disconnection before claiming leaves every rule armed.
        if getattr(account,'margin_mode',2)!=2: raise ValueError('Timed exit requires a hedging account')
        rows=self.orders()['orders']
        for identifier,payload in due:
            ticket=payload['ticket'];symbol=payload['symbol']
            matches=self.opening_rows(ticket,symbol,rows)
            if not matches:
                state=self.opening_state(ticket,symbol,rows)
                if state['phase'] in ('closed','cancelled'):
                    self.write_time_rule(identifier,payload,'completed',{'status':'ok',**state})
                continue
            # Cancel a pending remainder first; the next read resolves any fill.
            row=next((x for x in matches if not x['__isPosition']),matches[0])
            operation='close' if row['__isPosition'] else 'cancel'
            command_id='deadline:'+hashlib.sha256((identifier+':'+operation+':'+str(row['ticket'])).encode()).hexdigest()
            if command_id in payload['actions']:
                previous=payload['actions'][command_id]
                if previous['status']=='rejected': self.write_time_rule(identifier,payload,'rejected',previous)
                continue  # Never resend a claimed action.
            request=dict(id=command_id,ticket=str(row['ticket']),symbol=symbol,expiresAt=time.time()*1000+2000)
            if operation=='close': request.update(volume=row['lots'],expectedVolume=row['lots'])
            try:
                self.check_rule_unknown(request)
                self.control_request(operation,request)  # Recheck account target and actual volume before claim.
            except (ValueError,TypeError,KeyError): continue
            payload['commandId']=command_id
            self.write_time_rule(identifier,payload,'firing',{'status':'unknown','reason':'Timed exit claimed before submission'})
            try: result=self.execute(operation,request)['result']
            except Exception: result={'status':'unknown','reason':'Timed exit interrupted; inspect MT5'}
            payload['actions'][command_id]=result
            # A definite cancellation refusal may mean the limit filled during
            # order_send. Read again before deciding; never retry its cancellation.
            self.write_time_rule(identifier,payload,'armed' if result['status']=='ok' or operation=='cancel' and result['status']=='rejected' else result['status'],result)

    @staticmethod
    def position_identity(target):
        return [target.ticket, getattr(target, 'identifier', target.ticket), getattr(target, 'time_msc', 0), target.type, target.price_open]

    def rule_command(self, operation, payload):
        self.check_rule_unknown(payload)
        if operation == 'cancel-close':
            identifier = str(payload.get('ruleId', ''))
            row = self.db.execute('SELECT payload,state FROM close_rules WHERE id=?', (identifier,)).fetchone()
            if not row or str(json.loads(row[0])['ticket']) != str(payload.get('ticket')) or json.loads(row[0])['symbol'] != payload.get('symbol'):
                raise ValueError('Close condition does not belong to the selected position')
            if row[1] != 'armed':
                raise ValueError('Close condition is no longer armed; refresh its status')
            result = {'status':'ok', 'operation':operation, 'ticket':str(payload['ticket']), 'ruleId':identifier}
            self.db.execute('UPDATE close_rules SET state=?,result=? WHERE id=?', ('cancelled', json.dumps(result), identifier))
            return result
        target = self.target(payload, True)
        self.control_request('close', payload)  # broker lot limits and expected volume
        if float(payload['volume']) >= target.volume-1e-8:
            raise ValueError('Price condition must close only part of the position')
        price = float(payload.get('targetPrice', 0))
        step = self.mt5.symbol_info(target.symbol).trade_tick_size
        if not math.isfinite(price) or price <= 0 or not math.isfinite(step) or step <= 0 or abs(price/step-round(price/step)) > 1e-6 or payload.get('comparison') not in ('gte','lte'):
            raise ValueError('Invalid trigger price or comparison')
        for text, in self.db.execute("SELECT payload FROM close_rules WHERE state='armed'"):
            if str(json.loads(text)['ticket']) == str(target.ticket):
                raise ValueError('An active close condition already exists for this position')
        identifier = str(payload['id'])
        stored = {key:payload[key] for key in ('ticket','symbol','volume','expectedVolume','targetPrice','comparison')}
        stored.update(identity=self.position_identity(target), createdAt=time.time()*1000)
        if 'closePercent' in payload: stored['closePercent']=payload['closePercent']
        result = {'status':'ok', 'operation':operation, 'ticket':str(target.ticket), 'ruleId':identifier}
        self.db.execute('INSERT INTO close_rules VALUES (?,?,?,?)', (identifier, json.dumps(stored), 'armed', json.dumps(result)))
        return result

    def set_rule_state(self, identifier, state, result):
        self.db.execute('UPDATE close_rules SET state=?,result=? WHERE id=?', (state, json.dumps(result), identifier))
        self.db.commit()

    def close_rules(self):
        return self.envelope(rules=[dict(id=i, **json.loads(p), state=s, result=json.loads(r))
            for i,p,s,r in self.db.execute('SELECT * FROM close_rules ORDER BY rowid DESC LIMIT 100')])

    def evaluate_rules(self):
        rows = self.db.execute("SELECT id,payload FROM close_rules WHERE state='armed'").fetchall()
        if not rows or not self.config.get('tradingEnabled', False):
            return
        self.account()
        for identifier, text in rows:
            payload = json.loads(text)
            positions = self.mt5.positions_get(ticket=int(payload['ticket']))
            if positions is None:
                continue
            target = next((x for x in positions if str(x.ticket)==str(payload['ticket'])), None)
            if not target or target.symbol != payload['symbol'] or self.position_identity(target) != payload['identity'] or abs(target.volume-float(payload['expectedVolume']))>1e-8:
                self.set_rule_state(identifier, 'cancelled', {'status':'rejected','reason':'Position closed, replaced or volume changed'})
                continue
            tick = self.mt5.symbol_info_tick(target.symbol)
            stamp = self.tick_utc_seconds(tick)
            if not tick or not all(math.isfinite(x) for x in (tick.bid,tick.ask,stamp)) or tick.bid<=0 or tick.ask<tick.bid or not -1 <= time.time()-stamp <= 5:
                continue
            price = tick.bid if target.type == self.mt5.ORDER_TYPE_BUY else tick.ask
            reached = price >= float(payload['targetPrice']) if payload['comparison']=='gte' else price <= float(payload['targetPrice'])
            if not reached:
                continue
            self.set_rule_state(identifier, 'firing', {'status':'unknown','reason':'Trigger claimed before submission'})
            try:
                result = self.execute('close', {**payload, 'id':'rule:'+identifier, 'expiresAt':time.time()*1000+2000})['result']
            except Exception:
                result = {'status':'unknown','reason':'Trigger interrupted; inspect MT5'}
            self.set_rule_state(identifier, 'completed' if result['status']=='ok' else result['status'], result)

    def rule_interval(self):
        active = self.config.get('tradingEnabled', False) and self.db.execute(
            "SELECT 1 FROM close_rules WHERE state='armed' LIMIT 1").fetchone()
        return RULE_ACTIVE_INTERVAL if active else RULE_IDLE_INTERVAL

    @serialized
    def maintain_terminal_connection(self, now=None):
        # The rule thread runs even under continuous HTTP/RPC traffic. Keep
        # recovery outside commands: never retry an order after reconnecting.
        now = time.monotonic() if now is None else now
        if now < self.terminal_probe_at:
            return
        self.terminal_probe_at = now + .5
        try:
            self.account()
            return
        except ValueError as error:
            if str(error) != 'MT5 is disconnected':
                return  # Wrong account/server/USD must remain blocked.
        if now < self.terminal_reconnect_at:
            return
        self.terminal_reconnect_at = now + 5
        self.mt5.shutdown()
        if self.mt5.initialize(path=self.config['terminalPath'], timeout=3000):
            self.account()  # Validate the exact identity before accepting it.
            # A reconnect alone keeps the weekly schedule; a rejected broker
            # specification or the weekly deadline refreshes both caches.
            for symbol in self.config['symbols']:self._expire_specification(symbol)

    def start_rules(self):
        self.entry_diagnostics.start()
        from chunk_entry import EntryProgressObserver
        def progress_plans():
            # Published dictionaries are immutable; never read the trade DB or
            # hold a shared lock in a background-priority thread. A bounded
            # dictionary copy retains the current immutable published values.
            records = self.chunk_entries.public_records.copy()
            return [row[1] for row in records.values()]
        if self.chunk_entries.progress_observer is None and not (self.config.get('_nativeWorker') and self.config.get('_primaryWorker')):
            self.chunk_entries.progress_observer = EntryProgressObserver(
                self.config['account'], self.config['server'], self.entry_progress_file, progress_plans)
            self.chunk_entries.progress_observer.start()
        def run():
            interval = RULE_IDLE_INTERVAL
            while not self.rule_stop.is_set():
                started = time.perf_counter()
                diagnostic=self.entry_diagnostics.begin('rules-lock',reason='Waiting for native account lock')
                try:
                    with self.lock:
                        self.entry_diagnostics.step(diagnostic,'terminal-recovery')
                        self.read_timings.call('terminalRecovery', self.maintain_terminal_connection)
                        interval = self.rule_interval()
                        self.entry_diagnostics.step(diagnostic,'price-rules')
                        self.read_timings.call('priceRules', self.evaluate_rules)
                        self.entry_diagnostics.step(diagnostic,'time-rules')
                        self.read_timings.call('timeRules', self.evaluate_time_rules, False)
                        interval = self.rule_interval()
                    # Only historical reconciliation releases the account
                    # mutex between tickets. Due exits and live protective
                    # rules above retain their original serialized behavior.
                    self.read_timings.call('timeRuleHistory', self.reconcile_time_rule_history)
                except Exception as error:
                    self.entry_diagnostics.finish(diagnostic,error=error)
                    diagnostic=None  # Leave unclaimed conditions armed.
                finally:
                    self.entry_diagnostics.finish(diagnostic)
                if self.rule_stop.wait(max(0, interval-(time.perf_counter()-started))):
                    break
        self.rule_thread = threading.Thread(target=run, daemon=True)
        self.rule_thread.start()

    def health(self):
        return self._health(self.account())

    def candle_levels(self, payload=None):
        self.account()
        if self.candle_book is None:
            from candle_levels import CandleLevels
            self.candle_book=CandleLevels(self.mt5,self.config,self.candle_database,broker_zone_data=NEW_YORK_TZIF_GZIP)
        state=self.candle_book.state() if payload is None else self.candle_book.apply(payload)
        return self.envelope(candleLevels=state)

    def candle_symbols(self, encoded_names):
        from candle_levels import available_symbol
        self.account()
        names = json.loads(encoded_names)
        if not isinstance(names, list) or len(names) > 32:
            raise ValueError('At most 32 exact candle symbols can be checked')
        rows = []
        for name in names:
            info = available_symbol(self.mt5, name)
            rows.append(dict(name=info.name, description=str(getattr(info, 'description', ''))[:160]))
        return self.envelope(symbols=rows)

    def candle_snapshot(self, symbol):
        from candle_levels import available_symbol
        account = self.account()
        info = available_symbol(self.mt5, symbol, select=True)
        tick = self.mt5.symbol_info_tick(symbol)
        size = info.trade_tick_size
        if (not tick or not math.isfinite(tick.bid) or not math.isfinite(tick.ask) or tick.bid <= 0 or tick.ask < tick.bid
                or not math.isfinite(size) or size <= 0 or not -1 <= time.time() - self.tick_utc_seconds(tick) <= 5):
            raise ValueError('No fresh quote for the MT5 candle symbol')
        return dict(account=account.login, server=account.server, symbol=symbol, capturedAt=int(time.time()*1000),
            quote=dict(bid=tick.bid, ask=tick.ask, price=(tick.bid+tick.ask)/2, tickSize=size,
                       utcTime=self.tick_utc_seconds(tick)), metadata=dict(tickSize=size))

    def _capabilities(self):
        # Entry receipts also carry this full list: clients replace their
        # connection capability cache when an authenticated reply includes it.
        return ['place','cancel','close','partial-close','modify','schedule-close','cancel-close','quote-stream','adaptive-quotes','timings','diagnostics','mirror-accounts','management-snapshot','linked-worker-recovery','scaled-accounts','percentage-close','time-exit','opening-history','group-admission','prepared-group-workers','latest-quote-publisher','batched-management-reads','chunked-breakout','market-zone-entry','market-zone-entry-stop','market-offset-entry','market-side-zone-entry','candle-levels','candle-levels-color','candle-levels-window','candle-levels-mappings',
                'market-limit-side-zone-entry','hybrid-market-limit-v1','hybrid-position-protection-v1','hybrid-entry-speed-v1',
                'hybrid-native-reconcile-v1','hybrid-support-reprice-v1','atomic-entry-admission-v1','entry-progress-snapshots-v1','independent-account-entry-v2',
                'parallel-account-entry-v3','entry-progress-fast-read-v1',
                'independent-account-lanes-v4','entry-events-v1','entry-account-continue-v1','entry-stop-priority-v1',
                'entry-control-recovery-v1','entry-intent-delivery-v1','entry-status-order-v1','entry-exact-parent-close-v1',
                *(['independent-account-engines-v5','entry-initial-sizing-only-v1'] if getattr(self,'isolated_entries',False) else []),
                *ENTRY_PROTOCOL_CAPABILITIES] + (['lt-level-sync-v1'] if getattr(self,'lt_level_sync',None) else []) + (['account-enrollment','mt4-preparation'] if getattr(self,'account_registry',None) else [])

    def _health(self, account):
        account_info = {key:float(getattr(account, attr)) for key,attr in
            (('balance','balance'),('equity','equity'),('margin','margin'),('freeMargin','margin_free'))
            if type(getattr(account,attr,None)) in (int,float) and math.isfinite(getattr(account,attr))}
        return dict(account=account.login,server=account.server,tradingEnabled=self.config.get('tradingEnabled', False), version=BRIDGE_VERSION, idleTimeoutSeconds=30,
            accountInfo=account_info,
            brokerUtcOffsetSeconds=self.broker_utc_offset_seconds,
            rulesActiveIntervalMs=int(RULE_ACTIVE_INTERVAL*1000), rulesIdleIntervalMs=int(RULE_IDLE_INTERVAL*1000),
            receiverRestartBaseSeconds=RECEIVER_RETRY_SECONDS, capabilities=self._capabilities(),
            linkedAccounts=list(self.linked), chunkEntryAvailable=True, candleLevelsAvailable=True,
            marketZoneEntryStopAvailable=True, build=getattr(self,'runtime_build',{}),
            entryProgress=(self.chunk_entries.progress_observer.snapshot() if getattr(getattr(self,'chunk_entries',None),'progress_observer',None) else None))

    def group_result(self, results):
        primary = results.get(str(self.config['account']), next(iter(results.values())))
        states = [x['status'] for x in results.values()]
        status = 'ok' if all(x=='ok' for x in states) else 'rejected' if all(x=='rejected' for x in states) else 'unknown'
        return {**primary, 'status':status, 'primaryAccount':str(self.config['account']), 'accountResults':results,
                **({'reason':'; '.join(str(a)+': '+r['status']+(' — '+r['reason'] if r.get('reason') else '') for a,r in results.items())} if status!='ok' else {})}

    def opening_group(self, ticket, symbol, rows=None):
        if rows is None:
            target = self.target({'ticket':ticket,'symbol':symbol}, True) if any(str(x.ticket)==str(ticket) for x in self.mt5.positions_get() or ()) else self.target({'ticket':ticket,'symbol':symbol}, False)
            identifiers = {str(target.ticket), str(getattr(target, 'identifier', target.ticket))}
        else:
            # These rows were read in this same locked management snapshot.
            # Display decoration must not read the terminal again per row.
            row=next((x for x in rows if str(x['ticket'])==str(ticket) and x['symbol']==symbol),None)
            if row is None: raise ValueError('Position/order no longer exists')
            identifiers={str(row['ticket']),str(row.get('identifier',row['ticket']))}
        for identifier,payload,result in self.db.execute('SELECT id,payload,result FROM groups ORDER BY rowid DESC'):
            data, result = json.loads(payload), json.loads(result)
            primary = result.get('accountResults', {}).get(str(self.config['account']), {})
            if data['operation']=='place' and data['payload']['symbol']==symbol and primary.get('providerOrderId') in identifiers:
                return identifier,data,result
        raise ValueError('Selected position has no linked opening from ISCC')

    def group_execute(self, operation, payload, received=None, *, volumes=None, guards=None, independent=False):
        # Group coordination never owns the primary MT5/SQLite lock while
        # waiting for the other process. Same-ID groups remain serialized.
        received = received or (time.time()*1000,time.perf_counter())
        with self.group_lock:
            return self._group_execute(operation, payload, received, volumes=volumes, guards=guards, independent=independent)

    def warm_group_executor(self):
        # The executor lives until receiver shutdown. An already running
        # group command must not block independent entry merely obtaining it.
        executor = self.group_executor
        if executor is not None:
            return executor
        with self.group_lock:
            if self.group_executor is not None: return self.group_executor
            executor=ThreadPoolExecutor(max_workers=17,thread_name_prefix='iscc-group')
            # Force one worker per configured account to exist before the first
            # click. A barrier starts them without introducing a sleep or trade.
            count=min(17,1+len(self.linked))
            barrier=threading.Barrier(count+1,timeout=5)
            try:
                ready=[executor.submit(barrier.wait) for _ in range(count)]
                barrier.wait()
                for future in ready: future.result()
            except Exception:
                executor.shutdown(wait=True)
                raise
            self.group_executor=executor
            return executor

    def close_group_executor(self):
        if self.group_executor is not None:
            self.group_executor.shutdown(wait=True)
            self.group_executor=None
        self.entry_diagnostics.close()

    def requested_followers(self, payload):
        rows=payload.get('mirrorAccounts')
        if rows is None: rows=[dict(account=payload.get('mirrorAccount'),multiplier=1)]
        if not isinstance(rows,list) or not 1<=len(rows)<=16: raise ValueError('Invalid follower account list')
        seen={str(self.config['account'])}; result={}
        for row in rows:
            account=str(row['account']); multiplier=float(row.get('multiplier',1))
            if account in seen or account not in self.linked or not math.isfinite(multiplier) or not 0<multiplier<=100:
                raise ValueError('Invalid follower account or multiplier')
            seen.add(account); result[account]=multiplier
        return result

    def expected_group_volumes(self, opening, opened, states, groups, hybrid_cache=None):
        expected={}
        hybrid = self.hybrid_opened_volumes(opening, opened, states, hybrid_cache)
        for account,request in opening['requests'].items():
            expected[account]=hybrid.get(account, float(request['volume']))
            provider=str(opened.get('accountResults',{}).get(account,{}).get('providerOrderId',''))
            rows=states.get(account,{}).get('orders',[])
            row=next((x for x in rows if provider in (str(x['ticket']),str(x.get('identifier')))),None)
            tickets={provider}
            if row: tickets.add(str(row['ticket']))
            # Confirmed partial closes and fired conditions describe the expected
            # remainder. A manual MT5 change has no record and is still detected.
            for _,command,result in groups:
                request=command.get('requests',{}).get(account,{})
                outcome=result.get('accountResults',{}).get(account,{})
                if command['operation']=='close' and str(request.get('ticket')) in tickets and request.get('symbol')==opening['payload']['symbol'] and outcome.get('status')=='ok':
                    expected[account]=min(expected[account],float(request['expectedVolume'])-float(request['volume']))
            for rule in states.get(account,{}).get('rules',[]):
                if rule['state']=='completed' and str(rule['ticket']) in tickets and rule['symbol']==opening['payload']['symbol']:
                    expected[account]=min(expected[account],float(rule['expectedVolume'])-float(rule['volume']))
        return expected

    @staticmethod
    def _management_ticket_expression(account, target=False):
        if not re.fullmatch(r'[0-9]{1,20}', str(account)):
            raise ValueError('Invalid management index account')
        source = 'payload' if target else 'result'
        field = 'requests' if target else 'accountResults'
        ticket = 'ticket' if target else 'providerOrderId'
        return ("CASE WHEN json_valid("+source+") THEN CAST(json_extract("+source+
                ",'$."+field+'."'+str(account)+'".'+ticket+"') AS TEXT) END")

    def _ensure_management_indexes(self, account):
        account = str(account)
        if account in self.management_index_accounts:
            return
        with self.lock:
            for target in (False, True):
                name = 'group_live_'+('target_' if target else 'opening_')+account
                expression = self._management_ticket_expression(account, target)
                self.db.execute('CREATE INDEX IF NOT EXISTS '+name+' ON groups('+expression+')')
            self.management_index_accounts.add(account)

    def management_groups(self, states, reverse=False):
        """Read indexed groups for live tickets, retaining complete peer receipts."""
        selected = {}
        for account, state in states.items():
            identifiers = sorted({str(value) for row in state.get('orders', [])
                for value in (row.get('ticket'), row.get('identifier')) if value is not None})
            if not identifiers:
                continue
            self._ensure_management_indexes(account)
            for target in (False, True):
                expression = self._management_ticket_expression(account, target)
                index = 'group_live_'+('target_' if target else 'opening_')+str(account)
                for offset in range(0, len(identifiers), 100):
                    batch = identifiers[offset:offset+100]
                    with self.lock:
                        rows = self.db.execute('SELECT rowid,id,payload,result FROM groups INDEXED BY '+index+
                            ' WHERE '+expression+' IN ('+','.join('?' for _ in batch)+')', batch).fetchall()
                    for row in rows:
                        selected[row[1]] = row
        return [(identifier, payload, result) for _, identifier, payload, result in
                sorted(selected.values(), key=lambda row: row[0], reverse=reverse)]

    def hybrid_opened_volumes(self, opening, opened, states, cache=None):
        """Use only a settled hybrid opening's durable, exact owned volume.

        A cancelled remainder can leave a smaller hedging position than the
        originally accepted pending request. Live volume alone is no proof of
        that expected partial fill: require the coordinator's confirmed ledger,
        original group receipt and exact opening/position identities instead.
        """
        command = opening.get('payload', {})
        match = re.fullmatch(r'([A-Za-z0-9_.:-]{1,100}):h[1-9][0-9]*', str(command.get('id', '')))
        if opening.get('operation') != 'place' or command.get('type') not in ('limit','market') or not match:
            return {}
        receipts = opened.get('accountResults', {})
        if not any(row.get('__isPosition') is True and row.get('symbol') == command.get('symbol')
                   and str(receipt.get('providerOrderId', '')) in (str(row.get('ticket')),str(row.get('identifier')))
                   for account,receipt in receipts.items() for row in states.get(account, {}).get('orders', [])):
            return {}
        if cache is not None and match[1] in cache:
            saved = cache[match[1]]
        else:
            with self.lock:
                saved = self.db.execute('SELECT payload,state FROM chunk_entries WHERE id=?', (match[1],)).fetchone()
            saved = tuple(json.loads(text) for text in saved) if saved else None
            if cache is not None:
                cache[match[1]] = saved
        if saved is None:
            return {}
        plan, parent = saved
        if (plan.get('executionPolicy') != 'hybrid-market-limit-v1' or plan.get('id') != match[1]
                or parent.get('id') != match[1] or parent.get('executionPolicy') != 'hybrid-market-limit-v1'
                or any(plan.get(key) != command.get(key) or parent.get(key) != command.get(key) for key in ('symbol','side'))):
            return {}
        children = [child for child in parent.get('children', []) if child.get('id') == command['id']]
        if len(children) != 1:
            return {}
        child = children[0]
        if child.get('type') != command.get('type') or child.get('state') not in ('confirmed','cancelled'):
            return {}
        requests = opening.get('requests', {})
        captured_receipts = child.get('result', {}).get('accountResults', {})
        if (opened.get('status') != 'ok' or child.get('result', {}).get('status') != 'ok'
                or set(requests) != set(parent.get('accounts', {})) or set(receipts) != set(requests)
                or set(captured_receipts) != set(requests)):
            return {}
        result = {}
        for account, request in requests.items():
            captured = child.get('positions', {}).get(account)
            if captured is None:
                continue
            receipt, original = captured_receipts[account], receipts[account]
            ticket = str(original.get('providerOrderId', ''))
            volume, reserved = float(captured.get('volume', 0)), float(child.get('volumes', {}).get(account, 0))
            acquired = float(captured.get('acquiredVolume', volume))
            current = next((row for row in states.get(account, {}).get('orders', [])
                            if str(row.get('ticket', '')) == str(captured.get('ticket', ''))
                            and row.get('__isPosition') is True and row.get('symbol') == command['symbol']
                            and row.get('type') == command['side']), None)
            if (receipt.get('status') != 'ok' or original.get('status') != 'ok'
                    or not re.fullmatch(r'[1-9][0-9]{0,19}', ticket)
                    or str(receipt.get('providerOrderId', '')) != ticket
                    or str(captured.get('openingTicket', '')) != ticket
                    or not re.fullmatch(r'[1-9][0-9]{0,19}', str(captured.get('ticket', '')))
                    or current is None or abs(float(current.get('open_price', 0))-float(captured.get('fillPrice', 0))) > 1e-8
                    or request.get('symbol') != command['symbol'] or request.get('side') != command['side']
                    or not all(math.isfinite(value) and value > 0 for value in (volume,reserved))
                    or abs(reserved-float(request.get('volume', 0))) > 1e-8 or volume > reserved+1e-8
                    or acquired < reserved-1e-8 and child.get('cancellations', {}).get(account, {}).get('status') not in ('ok','rejected')):
                continue
            result[account] = volume
        return result

    def _group_execute(self, operation, payload, received, *, volumes=None, guards=None, independent=False):
        if type(independent) is not bool or independent and (operation != 'place' or volumes is None or guards is None):
            raise ValueError('Independent batches require captured internal account guards')
        timing = {'receivedAt':received[0], 'queueMs':round((time.perf_counter()-received[1])*1000,2)}
        started = time.perf_counter()
        identifier=str(payload.get('id',''))
        if not re.fullmatch(r'[A-Za-z0-9_.:-]{1,130}',identifier): raise ValueError('Invalid group request ID')
        digest_started=time.perf_counter()
        core={key:value for key,value in payload.items() if key!='expiresAt'}
        # Only the local persistent chunk coordinator supplies these keyword
        # arguments. HTTP clients cannot override per-account volume or guard.
        if volumes is not None: core['chunkVolumes']=volumes
        if guards is not None: core['chunkGuards']=guards
        if independent: core['independentAccounts']=True
        digest=hashlib.sha256(json.dumps({'operation':operation,**core},sort_keys=True,allow_nan=False).encode()).hexdigest()
        timing['digestMs']=round((time.perf_counter()-digest_started)*1000,2)
        primary=str(self.config['account'])
        lock_started=time.perf_counter()
        with self.lock:
            timing['journalLockWaitMs']=round((time.perf_counter()-lock_started)*1000,2)
            previous=measured(timing,'groupJournalLookupMs',lambda:self.db.execute('SELECT digest,result FROM groups WHERE id=?',(identifier,)).fetchone())
            if previous:
                if previous[0]!=digest: raise ValueError('Group request ID reused with different parameters')
                return self.envelope(result=json.loads(previous[1]))
        followers=measured(timing,'followerValidationMs',self.requested_followers,payload)
        if operation=='place': measured(timing,'deadlineValidationMs',self.validate_deadline,payload)
        sizing_started=time.perf_counter()
        requests={primary:dict(payload)}
        states={}
        if operation!='place':
            with self.lock:
                groups=[(i,json.loads(p),json.loads(r)) for i,p,r in self.db.execute('SELECT id,payload,result FROM groups')]
                for _,old,result in groups:
                    if str(old['payload'].get('ticket'))==str(payload.get('ticket')) and result['status']=='unknown':
                        raise ValueError('Unconfirmed group command; inspect linked accounts')
                _,opening,opened=self.opening_group(payload['ticket'],payload['symbol'])
                if set(opening['requests'])!={primary,*followers} or opened['status']!='ok':
                    raise ValueError('Linked opening is not confirmed on every selected account')
            states[primary]=self.local_management()
        for account,multiplier in followers.items():
            if operation=='place' and isinstance(volumes,dict) and account not in volumes:
                continue
            worker=self.linked[account]
            if hasattr(worker,'available') and not worker.available(): raise ValueError('Linked MT5 is reconnecting; no group commands submitted')
            request=dict(payload,id=identifier+':'+account)
            if operation=='place':
                specs=getattr(worker,'volume_specs',None)
                if specs is None: specs=worker.call('lot_specs')
                request['volume']=volumes[account] if volumes is not None else scaled_volume(payload['volume'],multiplier,specs[payload['symbol']])
            else:
                states[account]=worker.call('local_management')
                provider=str(opened['accountResults'][account]['providerOrderId'])
                position=operation in ('close','schedule-close','cancel-close') or payload.get('target')=='position'
                matches=[x for x in states[account]['orders'] if x['symbol']==payload['symbol'] and x['__isPosition']==position and provider in (str(x['ticket']),str(x.get('identifier')))]
                if len(matches)!=1: raise ValueError('Linked position/order no longer exists; manage accounts separately')
                request['ticket']=matches[0]['ticket']
                if operation=='cancel-close': request['ruleId']=str(payload['ruleId'])+':'+account
            requests[account]=request
        if operation!='place':
            expected=self.expected_group_volumes(opening,opened,states,groups)
            for account,request in requests.items():
                rows=states[account]['orders']
                row=next((x for x in rows if str(x['ticket'])==str(request['ticket'])),None)
                expected_position=operation in ('close','schedule-close','cancel-close') or payload.get('target')=='position'
                if not row or row['__isPosition']!=expected_position:
                    raise ValueError('Linked position/order kind changed; refresh before managing')
                if abs(float(row['lots'])-expected[account])>1e-8:
                    raise ValueError('Linked position volume differs from confirmed management; refresh and manage separately')
                if operation in ('close','schedule-close'):
                    if account==primary and abs(float(payload['expectedVolume'])-row['lots'])>1e-8: raise ValueError('Position volume changed')
                    fraction=Decimal(str(payload['closePercent']))/100 if 'closePercent' in payload else Decimal(str(payload['volume']))/Decimal(str(payload['expectedVolume']))
                    if not math.isfinite(fraction) or not 0<fraction<=1 or (operation=='schedule-close' and fraction>=1):
                        raise ValueError('Invalid partial close percentage')
                    spec=dict(quantityStep=row['quantityStep'],minQty=row['minQty'],maxQty=row.get('maxQty',1000000))
                    amount=scaled_volume(row['lots'],fraction,spec,partial=True)
                    if account==primary and abs(amount-float(payload['volume']))>1e-8: raise ValueError('Close volume does not match selected percentage')
                    request.update(volume=amount,expectedVolume=row['lots'])
        for request in requests.values():
            request.pop('mirrorAccount',None); request.pop('mirrorAccounts',None)
        if volumes is not None or guards is not None:
            if (operation!='place' or not isinstance(volumes,dict) or not isinstance(guards,dict)
                    or not volumes or not set(volumes).issubset(requests) or set(guards)!=set(volumes)):
                raise ValueError('Invalid internal chunk account allocation')
            # Only the durable coordinator supplies this internal subset.
            # A completed account never receives a zero-volume broker order.
            requests = {account:request for account,request in requests.items() if account in volumes}
            for account,request in requests.items():
                request['volume']=volumes[account]
                request['entryGuard']=guards[account]
        timing['routingAndSizingMs']=round((time.perf_counter()-sizing_started)*1000,2)
        executor=measured(timing,'executorReadyMs',self.warm_group_executor)
        preflight_error=None
        preflight_diagnostics={}
        account_refusals={}
        if operation=='place':
            # Verify both real terminal identities in parallel, not just that
            # worker processes exist. A failed preflight sends no orders.
            check_started=time.perf_counter()
            checks={account:executor.submit(self.linked[account].call,'check_opening',request)
                    for account,request in requests.items() if account!=primary}
            # The coordinator already has a thread. Start follower checks first
            # and perform the primary check here, removing a scheduling hop.
            # Every check still finishes before either submission is allowed.
            try:
                if primary in requests: self.check_opening(requests[primary])
                else: self.account()
            except (ValueError,OSError,RuntimeError) as error:
                if independent and primary in requests: account_refusals[primary]=str(error)
                else: preflight_error=str(error)
                detail=getattr(error,'entry_diagnostics',None)
                if isinstance(detail,dict): preflight_diagnostics[detail.get('account')]=detail
            for account,check in checks.items():
                try: check.result()
                except (ValueError,OSError,RuntimeError) as error:
                    if independent: account_refusals[account]=str(error)
                    else: preflight_error=str(error)
                    detail=getattr(error,'entry_diagnostics',None)
                    if isinstance(detail,dict): preflight_diagnostics[detail.get('account')]=detail
            timing['accountPreflightMs']=round((time.perf_counter()-check_started)*1000,2)
        stored={'operation':operation,'payload':payload,'requests':requests}
        if independent: stored['independentAccounts']=True
        result=self.group_result({account:{'status':'unknown','reason':'Group intent saved before transmission'} for account in requests})
        encoding_started=time.perf_counter()
        stored_text,result_text=json.dumps(stored),json.dumps(result)
        timing['intentEncodingMs']=round((time.perf_counter()-encoding_started)*1000,2)
        timing['prepareMs'] = round((time.perf_counter()-started)*1000,2)
        ledger_started = time.perf_counter()
        lock_started=time.perf_counter()
        with self.lock:
            timing['intentLockWaitMs']=round((time.perf_counter()-lock_started)*1000,2)
            self.db.execute('INSERT INTO groups VALUES (?,?,?,?)',(identifier,digest,stored_text,result_text)); self.db.commit()
        timing['ledgerBeforeMs'] = round((time.perf_counter()-ledger_started)*1000,2)
        if operation=='place':
            remaining=float(payload.get('expiresAt',0))-time.time()*1000
            timing['submissionBudgetMs']=round(remaining,2) if math.isfinite(remaining) else None
            # Leave a shared one-second headroom for final checks and durable
            # per-account intents. Never renew a late command on just one side.
            if preflight_error or not math.isfinite(remaining) or remaining<1000 or remaining>3000:
                reason=('Account preflight failed; no account submitted: '+preflight_error if preflight_error else
                        'Insufficient shared submission time; no account submitted')
                result=self.group_result({account:{'status':'rejected',
                    'reason':reason,
                    **({'entryDiagnostics':preflight_diagnostics[account]} if account in preflight_diagnostics else {}),
                    'requestedVolume':request['volume']} for account,request in requests.items()})
                with self.lock:
                    self.db.execute('UPDATE groups SET result=? WHERE id=?',(json.dumps(result),identifier));self.db.commit()
                timing['totalMs']=round((time.perf_counter()-received[1])*1000,2)
                result['timings']={'server':dict(timing)}
                return self.envelope(result=result,serverTiming=dict(timing))
        refused_results={account:dict(status='rejected',reason=reason,requestedVolume=requests[account]['volume'],
            **({'entryDiagnostics':preflight_diagnostics[account]} if account in preflight_diagnostics else {}))
            for account,reason in account_refusals.items()}
        if independent:
            requests={account:request for account,request in requests.items() if account not in account_refusals}
        handoffs = {}
        abort_dispatch = threading.Event()
        handoff_deadline = time.monotonic()+.25
        if operation=='place':
            handoffs = {account:dict(ready=threading.Event(), sent=False) for account in requests if account!=primary
                        and callable(getattr(self.linked[account], 'call_dispatched', None))}
        def send(account,request):
            dispatched = (time.time()*1000,time.perf_counter())
            handoff = handoffs.get(account)
            def sent():
                handoff['sent'] = True
                handoff['at'] = time.perf_counter()
                handoff['onTime'] = time.monotonic()<=handoff_deadline and not abort_dispatch.is_set()
                handoff['ready'].set()
            try:
                if handoff is not None:
                    response = self.linked[account].call_dispatched(sent, abort_dispatch, handoff_deadline,
                        'execute',operation,request,dispatched)
                else:
                    response = self.execute(operation,request,dispatched) if account==primary else self.linked[account].call('execute',operation,request,dispatched)
                status = response['result']
                account_timing = {**status.get('timings',{})}
                if response.get('serverTiming'): account_timing['server']=response['serverTiming']
                if response.get('rpcTiming'): account_timing['workerRpc']=response['rpcTiming']
            except WorkerDispatchNotSent as error:
                status = {'status':'rejected','reason':str(error),'dispatchNotSent':True,
                    'workerContention':getattr(error,'rpc_diagnostics',{})}
                account_timing = {}
            except Exception:
                status = {'status':'unknown','reason':'Account response unconfirmed; inspect receipt'}
                account_timing = {}
            finally:
                if handoff is not None:
                    handoff['ready'].set()
            if handoff is not None and handoff.get('sent'):
                account_timing['handoffAfterReceivedMs'] = round((handoff['at']-received[1])*1000,2)
            account_timing.update(dispatchAfterReceivedMs=round((dispatched[1]-received[1])*1000,2),
                                  vpsFromReceiveMs=round((time.perf_counter()-received[1])*1000,2))
            return {**status,**({'requestedVolume':request['volume']} if operation=='place' else {}),'timings':account_timing}
        parallel_started = time.perf_counter()
        futures={account:executor.submit(send,account,request) for account,request in requests.items() if account!=primary}
        dispatch_error = None
        for handoff in handoffs.values():
            if (not handoff['ready'].wait(max(0.,handoff_deadline-time.monotonic()))
                    or not handoff['sent'] or not handoff.get('onTime')):
                dispatch_error = 'Follower dispatch was not confirmed; primary account was not submitted'
                abort_dispatch.set()
                break
        if handoffs:
            timing['followerDispatchWaitMs'] = round((time.perf_counter()-parallel_started)*1000,2)
            timing['followerDispatchConfirmed'] = dispatch_error is None
        results=({primary:dict(status='rejected',reason=dispatch_error,requestedVolume=requests[primary]['volume'])
                 if dispatch_error else send(primary,requests[primary])} if primary in requests else {})
        results.update(refused_results)
        reply_deadline = time.monotonic()+2.25
        for account, future in futures.items():
            try:
                # Native workers already bound lock/response waits. A stalled
                # transport must still leave a durable unknown, never a false
                # rejection while its send could arrive later.
                results[account] = future.result(timeout=max(0.,reply_deadline-time.monotonic())) if account in handoffs else future.result()
            except FutureTimeoutError:
                abort_dispatch.set()
                results[account] = dict(status='unknown',reason='Follower dispatch or response remains unconfirmed; no automatic replay',
                                        requestedVolume=requests[account]['volume'])
        result=self.group_result(results)
        timing['parallelMs'] = round((time.perf_counter()-parallel_started)*1000,2)
        timing['totalMs'] = round((time.perf_counter()-received[1])*1000,2)
        result['timings'] = {'server':dict(timing)}
        ledger_started = time.perf_counter()
        result_text=json.dumps(result)
        with self.lock:
            self.db.execute('UPDATE groups SET result=? WHERE id=?',(result_text,identifier)); self.db.commit()
            timing['ledgerAfterMs'] = round((time.perf_counter()-ledger_started)*1000,2)
            timing['totalMs'] = round((time.perf_counter()-received[1])*1000,2)
            result['timings']['server'] = dict(timing)
            return self.envelope(result=result,serverTiming=dict(timing))

    def decorated_orders(self, account=None):
        result = self.orders() if account is None else self._orders(account)
        for row in result['orders']:
            try:
                group,data,opened = self.opening_group(row['ticket'],row['symbol'],result['orders'])
                accounts=[account for account in data['requests'] if account!=str(self.config['account'])]
                row.update(groupId=group, mirrorAccounts=accounts, mirrorStatus=opened['status'])
                if accounts:
                    row['mirrorAccount'] = accounts[0]
            except ValueError:
                pass
        return result

    def open_request(self, payload):
        symbol, side, kind = payload.get('symbol'), payload.get('side'), payload.get('type')
        if symbol not in self.config['symbols'] or side not in ('buy', 'sell') or kind not in ('market', 'limit', 'stop'):
            raise ValueError('Symbol, side or order type is not allowed')
        volume = float(payload.get('volume', 0))
        price = float(payload.get('price', 0))
        sl, tp = float(payload.get('sl', 0)), float(payload.get('tp', 0))
        if not all(math.isfinite(x) for x in (volume, price, sl, tp)):
            raise ValueError('Non-finite order parameter')
        # Mac owns entry, sizing and protection. MT5 returns broker validation
        # from order_send; no quote lookup, recalculation or order_check here.
        order_type = getattr(self.mt5, 'ORDER_TYPE_' + side.upper() + ('' if kind == 'market' else '_' + kind.upper()))
        request = {
            'action': self.mt5.TRADE_ACTION_DEAL if kind == 'market' else self.mt5.TRADE_ACTION_PENDING,
            'symbol': symbol, 'type': order_type, 'volume': volume, 'price': price,
            'sl': sl, 'tp': tp, 'deviation': int(self.config.get('deviationPoints', 20)),
            'magic': int(self.config.get('magic', 120009)), 'comment': ('cid:' + str(payload.get('cid', '')))[:31],
            'type_time': self.mt5.ORDER_TIME_GTC,
            'type_filling': self.mt5.ORDER_FILLING_FOK if kind == 'market' else self.mt5.ORDER_FILLING_RETURN}
        if 'entryGuard' in payload:
            self.guard_chunk_request(payload, request)
        return request

    def guard_chunk_request(self, payload, request):
        """Fresh terminal-side guard; a market fill itself can still slip."""
        guard=payload['entryGuard']
        required={'anchor','maxGap','maxRiskPrice','positions'}
        zone_fields = {'priceFloor','priceCeiling'}
        entry_stop_fields = {'stopDistance','riskBudget'}
        pending = payload.get('type') == 'limit'
        base_guard = {key:value for key,value in guard.items() if key not in ('limitPolicy','limitPrice','fixedProtection','supportReprice')} if isinstance(guard,dict) else guard
        if payload.get('type') not in ('market', 'limit') or not isinstance(guard,dict) or set(base_guard) not in (
                required, required|zone_fields, required|zone_fields|entry_stop_fields):
            raise ValueError('Invalid chunk price guard')
        if 'limitPolicy' in guard and (not pending or guard['limitPolicy'] != 'support'):
            raise ValueError('Invalid hybrid support limit policy')
        if 'limitPrice' in guard and guard.get('limitPolicy') != 'support':
            raise ValueError('A pinned support price requires the hybrid support policy')
        if 'supportReprice' in guard and (guard['supportReprice'] is not True or not pending or guard.get('limitPolicy') != 'support'):
            raise ValueError('Only a captured support limit may use native repricing')
        if pending and not zone_fields.issubset(guard):
            raise ValueError('A hybrid limit requires both captured price boundaries')
        anchor,gap,risk_price=(float(guard[key]) for key in ('anchor','maxGap','maxRiskPrice'))
        if not all(math.isfinite(x) for x in (anchor,gap,risk_price)) or anchor<=0 or risk_price<=0 or gap<0:
            raise ValueError('Invalid chunk price boundary')
        info=self.mt5.symbol_info(request['symbol'])
        tick=self.mt5.symbol_info_tick(request['symbol'])
        if not info or not tick: raise ValueError('No fresh chunk quote')
        age=time.time()-self.tick_utc_seconds(tick)
        if not math.isfinite(age) or age<-.25 or age>1:
            raise ValueError('Chunk tick is stale; no submission')
        bid,ask=float(tick.bid),float(tick.ask)
        if not all(math.isfinite(x) and x>0 for x in (bid,ask)) or ask<bid:
            raise ValueError('Invalid chunk Bid/Ask')
        buy=payload['side']=='buy'; quote=ask if buy else bid; direction=1 if buy else -1
        price = float(guard.get('limitPrice', payload['price'])) if pending else quote
        if guard.get('supportReprice') is True:
            lower, upper = float(guard['priceFloor']), float(guard['priceCeiling'])
            tick_size = float(info.trade_tick_size)
            minimum_distance = float(getattr(info, 'trade_stops_level', 0))*float(getattr(info, 'point', tick_size))
            if (not all(math.isfinite(value) for value in (lower, upper, tick_size, minimum_distance))
                    or tick_size <= 0 or minimum_distance < 0 or lower > upper):
                raise ValueError('Invalid support repricing bounds')
            # Reuse the tick already required by the final native guard. Never
            # chase outside LT/luft or borrow the reserved market part's risk.
            opposite = min(bid, ask-minimum_distance) if buy else max(ask, bid+minimum_distance)
            units = opposite/tick_size
            opposite = (math.floor(units+1e-9) if buy else math.ceil(units-1e-9))*tick_size
            lower_tick = math.ceil(lower/tick_size-1e-9)*tick_size
            upper_tick = math.floor(upper/tick_size+1e-9)*tick_size
            opposite = max(lower_tick, min(upper_tick, opposite))
            price = round(max(lower, min(upper, opposite)), 10)
        if not math.isfinite(price) or price <= 0:
            raise ValueError('Invalid captured chunk price')
        if direction*(price-anchor)>gap+1e-8 or direction*(price-risk_price)>1e-8:
            raise EntryPriceRejected('Chunk price moved outside entry or risk boundary',
                dict(account=str(self.config['account']), symbol=request['symbol'], side=payload['side'],
                    kind=payload['type'], bid=bid, ask=ask, tickUtcMs=self.tick_utc_seconds(tick)*1000,
                    checkedAtMs=time.time()*1000, rejectionStage='native-entry-price-guard', mutationStarted=False))
        if 'priceFloor' in guard:
            lower,upper=(float(guard[key]) for key in ('priceFloor','priceCeiling'))
            if not all(math.isfinite(value) and value>0 for value in (lower,upper)) or lower>upper:
                raise ValueError('Invalid market zone price boundary')
            if price<lower-1e-8 or price>upper+1e-8:
                raise EntryPriceRejected('Market price moved outside the entry zone',
                    dict(account=str(self.config['account']), symbol=request['symbol'], side=payload['side'],
                        kind=payload['type'], bid=bid, ask=ask, tickUtcMs=self.tick_utc_seconds(tick)*1000,
                        checkedAtMs=time.time()*1000, rejectionStage='native-entry-price-guard', mutationStarted=False))
            if pending:
                edge = upper if buy else lower
                minimum_distance = float(getattr(info, 'trade_stops_level', 0)) * float(getattr(info, 'point', info.trade_tick_size))
                tick_size = float(info.trade_tick_size)
                if guard.get('limitPolicy') == 'support':
                    # Coordinator captured each account's opposite quote and
                    # clamped it at LT. Preserve that price during a small
                    # quote drift; final validation still requires a genuine
                    # limit inside the captured zone and broker distance.
                    edge = price
                if (not math.isfinite(tick_size) or tick_size<=0 or not math.isfinite(minimum_distance) or minimum_distance<0
                        or not math.isfinite(price) or abs(price-edge)>1e-8 or direction*(quote-edge)<=1e-8
                        or abs(price/tick_size-round(price/tick_size))>1e-6
                        or direction*(quote-edge)<minimum_distance-1e-8):
                    raise EntryPriceRejected('Limit requires a fresh quote beyond its captured outer edge and broker distance',
                        dict(account=str(self.config['account']), symbol=request['symbol'], side=payload['side'],
                             kind=payload['type'], bid=bid, ask=ask, submittedPrice=price,
                             zoneLower=lower, zoneUpper=upper, minimumDistance=minimum_distance,
                             tickSize=tick_size, tickUtcMs=self.tick_utc_seconds(tick)*1000,
                             checkedAtMs=time.time()*1000, rejectionStage='final-native-price-guard', **{'mutationStarted': False}))
        if 'stopDistance' in guard:
            distance, budget = (float(guard[key]) for key in ('stopDistance','riskBudget'))
            tick_size = float(info.trade_tick_size)
            point_value = self.mt5.order_calc_profit(self.mt5.ORDER_TYPE_BUY, request['symbol'], 1., price, price + 1.)
            if point_value is None:
                raise ValueError('Cannot confirm USD value of the entry stop')
            point_value = float(point_value)
            if not all(math.isfinite(value) and value > 0 for value in (distance, budget, tick_size, point_value)):
                raise ValueError('Invalid entry stop or remaining risk')
            if abs(distance / tick_size - round(distance / tick_size)) > 1e-6:
                raise ValueError('Entry stop does not match the broker price step')
            if request['volume'] * distance * point_value > budget + 1e-8:
                raise ValueError('Entry stop exceeds the remaining risk')
            request['sl'] = round(price - direction * distance, 10)
        if 'fixedProtection' in guard:
            fixed = guard['fixedProtection']
            if not isinstance(fixed, dict) or set(fixed) not in ({'tp'}, {'sl','tp','riskBudget'}):
                raise ValueError('Invalid fixed hybrid protection')
            take = float(fixed['tp'])
            if not math.isfinite(take) or take < 0:
                raise ValueError('Invalid fixed hybrid take profit')
            request['tp'] = take
            if 'sl' in fixed:
                if 'stopDistance' in guard:
                    raise ValueError('Fixed and moving entry stops cannot be combined')
                stop, budget = float(fixed['sl']), float(fixed['riskBudget'])
                if not math.isfinite(stop) or stop <= 0 or not math.isfinite(budget) or budget <= 0:
                    raise ValueError('Invalid fixed hybrid stop or risk')
                profit = self.mt5.order_calc_profit(self.mt5.ORDER_TYPE_BUY if buy else self.mt5.ORDER_TYPE_SELL,
                    request['symbol'], request['volume'], price, stop)
                if profit is None or not math.isfinite(float(profit)) or max(0., -float(profit)) > budget+1e-8:
                    raise ValueError('Fixed hybrid stop exceeds remaining risk')
                request['sl'] = stop
        if request['sl']<=0 or direction*(price-request['sl'])<=0 or (request['tp'] and direction*(request['tp']-price)<=0):
            raise ValueError('Chunk price reached stop or take profit')
        reference = price if pending else bid if buy else ask
        minimum = float(getattr(info, 'trade_stops_level', 0))*float(getattr(info, 'point', info.trade_tick_size))
        if not math.isfinite(minimum) or minimum < 0:
            raise ValueError('Invalid native protection distance')
        if (direction*(reference-request['sl']) <= 1e-8
                or direction*(reference-request['sl']) < minimum-1e-8
                or request['tp'] and (direction*(request['tp']-reference) <= 1e-8
                    or direction*(request['tp']-reference) < minimum-1e-8)):
            raise EntryPriceRejected('Fresh broker quote does not permit the captured SL/TP',
                dict(account=str(self.config['account']), symbol=request['symbol'], side=payload['side'],
                    kind=payload['type'], bid=bid, ask=ask, sl=request['sl'], tp=request['tp'],
                    minimumDistance=minimum, tickSize=float(info.trade_tick_size),
                    tickUtcMs=self.tick_utc_seconds(tick)*1000, checkedAtMs=time.time()*1000,
                    rejectionStage='final-native-protection-guard', **{'mutationStarted':False}))
        previous=guard['positions']
        if not isinstance(previous,list) or len(previous)>200:
            raise ValueError('Invalid previous chunk positions')
        if previous:
            current=self.mt5.positions_get()
            if current is None: raise ValueError('Cannot confirm previous chunk positions')
            current={str(row.ticket):row for row in current}
            for expected in previous:
                if not isinstance(expected,dict) or set(expected)!={'ticket','volume','sl','tp'}:
                    raise ValueError('Invalid previous chunk position guard')
                row=current.get(str(expected['ticket']))
                expected_type = self.mt5.ORDER_TYPE_BUY if buy else self.mt5.ORDER_TYPE_SELL
                if not row or row.symbol!=request['symbol'] or row.type!=expected_type or any(
                    not math.isfinite(float(expected[key])) or abs(float(getattr(row,attribute))-float(expected[key]))>1e-8
                    for key,attribute in (('volume','volume'),('sl','sl'),('tp','tp'))):
                    raise ValueError('Previous entry position closed or changed; no further entry')
        step,minimum,maximum=(float(x) for x in (info.volume_step,info.volume_min,info.volume_max))
        volume=request['volume']
        if not all(math.isfinite(x) and x>0 for x in (step,minimum,maximum,volume)) or volume<minimum-1e-8 or volume>maximum+1e-8 or abs(volume/step-round(volume/step))>1e-7:
            raise ValueError('Invalid chunk volume for this broker account')
        filling=info.filling_mode
        # Full confirmation is required before the next chunk. IOC partial
        # execution remains unknown and stops the whole parent strategy.
        if pending: request['type_filling']=self.mt5.ORDER_FILLING_RETURN
        elif filling&1: request['type_filling']=self.mt5.ORDER_FILLING_FOK
        elif filling&2: request['type_filling']=self.mt5.ORDER_FILLING_IOC
        else: raise ValueError('No supported chunk market filling mode')
        request['price']=price

    def _entry_position_identity(self, target, payload):
        opening = str(payload.get('openingTicket', ''))
        price = float(payload['fillPrice'])
        if (payload.get('side') not in ('buy','sell') or not re.fullmatch(r'[1-9][0-9]{0,19}', opening)
                or target.type != (self.mt5.ORDER_TYPE_BUY if payload['side']=='buy' else self.mt5.ORDER_TYPE_SELL)
                or not math.isfinite(price) or abs(float(target.price_open)-price) > 1e-8):
            raise ValueError('Owned hedge does not match its captured opening')
        identities = (str(target.ticket),str(getattr(target,'identifier',target.ticket)))
        if opening not in identities:
            # Pending opening, position ticket and position identifier may all
            # differ. Only this exact broker order may establish their link;
            # neither symbol-wide history nor a similar live position suffices.
            # Explicit ownership authorization still requires fresh history.
            # The immutable link cache serves snapshot matching only, never a
            # whole-parent protection/close permission after external changes.
            history = self.read_timings.call('orderHistory', self.mt5.history_orders_get, ticket=int(opening))
            position_id=self._remember_opening_link(opening,target.symbol,history)
            if position_id not in identities:
                raise ValueError('No unique exact broker opening-to-position link')

    @serialized
    def entry_position_identity(self, payload):
        """Read-only proof for an opening whose native position IDs differ."""
        self.account()
        target = self.target(payload, True)
        self._entry_position_identity(target, payload)
        return self.envelope(ticket=str(target.ticket), openingTicket=str(payload['openingTicket']),
                             volume=target.volume, fillPrice=target.price_open)

    @serialized
    def entry_position_remainder(self, payload):
        """Prove a reduced owned hedge from exact native incoming/outgoing deals."""
        self.account()
        target = self.target(payload, True)
        self._entry_position_identity(target, payload)
        volume = float(payload['acquiredVolume'])
        if not math.isfinite(volume) or not 0 < target.volume <= volume+1e-8:
            raise ValueError('Remaining hedge does not match its captured opening')
        deals = self.mt5.history_deals_get(position=int(getattr(target,'identifier',target.ticket)))
        if deals is None:
            raise ValueError('Cannot read native partial-exit proof')
        identifier = int(getattr(target,'identifier',target.ticket))
        deals = [row for row in deals if row.symbol==target.symbol and int(row.position_id)==identifier]
        if any(row.entry not in (0,1,3) for row in deals):
            raise ValueError('Ambiguous native hedge deal direction')
        incoming = sum(row.volume for row in deals if row.entry==0)
        outgoing = sum(row.volume for row in deals if row.entry in (1,3))
        if abs(incoming-volume)>1e-8 or outgoing<=0 or abs(incoming-outgoing-target.volume)>1e-8:
            raise ValueError('Reduced hedge volume is not broker-proved')
        return self.envelope(volume=target.volume, acquiredVolume=volume)

    @serialized
    def entry_common_stop(self, payload):
        return self._entry_protection(payload)

    @serialized
    def entry_common_stop_receipt(self, payload):
        """Exact read only; never run a missing or unknown protection command."""
        identifier, signature = payload.get('id'), payload.get('digest')
        if (not isinstance(identifier,str) or not re.fullmatch(r'hybrid-sl:[a-f0-9]{64}',identifier)
                or not isinstance(signature,str) or not re.fullmatch(r'[a-f0-9]{64}',signature)):
            raise ValueError('Invalid common stop receipt identity')
        self.account()
        saved = self.db.execute('SELECT digest,result FROM entry_common_stops WHERE id=?',(identifier,)).fetchone()
        if saved and saved[0]!=signature:
            raise ValueError('Common stop receipt digest mismatch')
        return dict(account=str(self.config['account']),server=self.config['server'],id=identifier,digest=signature,
                    result=json.loads(saved[1]) if saved else None)

    @serialized
    def entry_protection_plan(self, payload):
        """Read-only native validation before a parent modifies any account."""
        return self._entry_protection(payload, dry_run=True)

    def _entry_protection(self, payload, dry_run=False):
        """Confirm one common stop for exact hedging positions, never retry sends.

        The coordinator must confirm cancellation of its pending remainder
        before calling. Tightening protections first keeps every intermediate
        loss no larger than the initial/final checked risk budgets.
        """
        identifier = str(payload.get('id', ''))
        if not re.fullmatch(r'[A-Za-z0-9_.:-]{1,160}', identifier):
            raise ValueError('Invalid common stop command ID')
        signature = hashlib.sha256(json.dumps(payload, sort_keys=True, allow_nan=False).encode()).hexdigest()
        previous = self.db.execute('SELECT digest,result FROM entry_common_stops WHERE id=?', (identifier,)).fetchone()
        if previous:
            if previous[0] != signature:
                raise ValueError('Common stop ID already used with different parameters')
            return json.loads(previous[1])
        started = time.perf_counter()
        claimed = False
        base = dict(account=str(self.config['account']), symbol=payload.get('symbol'))
        protection_timing = {}

        def timed_read(name, call, *args):
            began=time.perf_counter()
            try:return call(*args)
            finally:protection_timing[name]=round(protection_timing.get(name,0.)+(time.perf_counter()-began)*1000,3)

        def observe_stage(stage, duration, **metrics):
            try:
                self.entry_diagnostics.emit('protection-stage',account=base['account'],entry_id=identifier,
                    symbol=base['symbol'],stage=stage,duration_ms=duration,**metrics)
            except Exception:pass  # Optional diagnostics never control a trade.

        def finish(status, reason=None, **details):
            result = {**base, **details, 'status':status,
                      'elapsedMs':round((time.perf_counter()-started)*1000, 3)}
            if status == 'rejected' and not claimed and not details.get('retryableQuoteWait'):
                result['mutationStarted'] = False
                result['finalPreMutationRejection'] = True
            if reason:
                result['reason'] = str(reason)
            result['protectionTiming']=dict(protection_timing)
            if dry_run:
                return result
            receipt_started=time.perf_counter()
            self.db.execute('INSERT OR REPLACE INTO entry_common_stops VALUES (?,?,?)',
                            (identifier, signature, json.dumps(result, allow_nan=False)))
            self.db.execute('DELETE FROM entry_common_stop_targets WHERE command_id=?', (identifier,))
            if status == 'unknown':
                self.db.executemany('INSERT INTO entry_common_stop_targets VALUES (?,?,?)',
                    [(identifier, str(result.get('symbol', '')), str(row['ticket']))
                     for row in result.get('positions', [])])
            self.db.commit()
            receipt_ms=round((time.perf_counter()-receipt_started)*1000,3)
            intent=status=='unknown' and not claimed
            if intent:protection_timing['intentCommitMs']=receipt_ms
            observe_stage('intent-commit' if intent else 'result-commit',receipt_ms)
            # Timings are diagnostic only; the committed result remains the
            # exact trading receipt and does not require another write.
            return result

        try:
            validation_started=time.perf_counter()
            account = timed_read('accountCheckMs',self.account)
            if not self.config.get('tradingEnabled', False) or getattr(account, 'margin_mode', 2) != 2:
                raise ValueError('Common entry stop requires enabled trading on a hedging account')
            symbol, side = payload.get('symbol'), payload.get('side')
            if symbol not in self.config['symbols'] or side not in ('buy','sell'):
                raise ValueError('Invalid common entry stop symbol or side')
            buy = side == 'buy'; direction = 1 if buy else -1
            kind = self.mt5.ORDER_TYPE_BUY if buy else self.mt5.ORDER_TYPE_SELL
            expected = payload.get('positions')
            if not isinstance(expected, list) or not 1 <= len(expected) <= 200:
                raise ValueError('A common stop requires exact captured positions')
            keys = {'ticket','volume','fillPrice','sl','tp'}
            if any(not isinstance(row, dict) or set(row) != keys for row in expected):
                raise ValueError('Invalid common stop position snapshot')
            tickets = [str(row['ticket']) for row in expected]
            if len(set(tickets)) != len(tickets) or any(not re.fullmatch(r'[1-9][0-9]{0,19}', ticket) for ticket in tickets):
                raise ValueError('Common stop position tickets must be unique')
            placeholders = ','.join('?' for _ in tickets)
            if self.db.execute('SELECT 1 FROM entry_common_stop_targets WHERE symbol=? AND ticket IN ('+
                               placeholders+') LIMIT 1', [symbol, *tickets]).fetchone():
                raise ValueError('Previous common stop outcome is unknown; inspect MT5')
            budget = float(payload.get('riskBudget', 0))
            if not math.isfinite(budget) or budget <= 0:
                raise ValueError('Invalid common stop risk budget')
            info = self.mt5.symbol_info(symbol)
            read_started = time.perf_counter()
            tick = self.mt5.symbol_info_tick(symbol)
            captured_at = time.time() * 1000
            if not info or not tick:
                raise ValueError('No common stop symbol or quote')
            tick_size = float(info.trade_tick_size)
            quote = float(tick.bid if buy else tick.ask)
            observation = self.observe_quote(symbol, tick, 'common-stop', captured_at, (time.perf_counter()-read_started)*1000)
            age_ms = observation['nativeAgeMs']
            if (not math.isfinite(tick_size) or tick_size <= 0 or not math.isfinite(quote) or quote <= 0
                    or not math.isfinite(tick.bid) or not math.isfinite(tick.ask) or tick.bid <= 0 or tick.ask < tick.bid
                    or age_ms is None):
                raise ValueError('Common stop quote is stale or invalid')
            if not -250 <= age_ms <= 1000:
                # No native modification or unknown intent exists at this point.
                # Only this exact proof permits the coordinator to wait/retry.
                return finish('rejected', 'Common stop quote is stale; waiting for fresh quote',
                              retryableQuoteWait=True, mutationStarted=False, quoteWait=observation)
            current_rows = timed_read('positionReadMs',self.mt5.positions_get)
            if current_rows is None:
                raise ValueError('Cannot confirm common stop positions')
            current = {str(row.ticket):row for row in current_rows}
            positions = []
            for wanted in expected:
                row = current.get(str(wanted['ticket']))
                if not row or row.symbol != symbol or row.type != kind:
                    raise ValueError('Captured entry position closed or changed')
                for key, attr in (('volume','volume'),('fillPrice','price_open'),('sl','sl'),('tp','tp')):
                    value = float(wanted[key]); actual = float(getattr(row, attr))
                    if not math.isfinite(value) or not math.isfinite(actual) or abs(actual-value) > 1e-8:
                        raise ValueError('Captured entry position changed before common stop')
                if row.volume <= 0 or row.price_open <= 0 or row.sl <= 0:
                    raise ValueError('Every captured entry position must remain protected')
                self.check_rule_unknown(dict(ticket=str(row.ticket), symbol=symbol))
                positions.append(row)
            volume = sum(row.volume for row in positions)
            average = sum(row.volume * row.price_open for row in positions) / volume
            if 'maxVolume' in payload:
                cap = float(payload['maxVolume'])
                if not math.isfinite(cap) or cap <= 0 or volume > cap + 1e-8:
                    raise ValueError('Common stop volume exceeds the reserved plan')
            if sum(key in payload for key in ('stopDistance','sl','breakeven')) != 1:
                raise ValueError('Specify entry stop distance, absolute stop or weighted breakeven')
            if 'stopDistance' in payload or 'breakeven' in payload:
                distance = float(payload.get('stopDistance', 0))
                if ('stopDistance' in payload and (not math.isfinite(distance) or distance <= 0)
                        or 'breakeven' in payload and payload['breakeven'] is not True):
                    raise ValueError('Invalid common entry stop distance or breakeven')
                raw = average - direction * distance
                # A weighted average need not be on the broker price step.
                # Round the protection toward price, never adding risk.
                units = Decimal(str(raw)) / Decimal(str(tick_size))
                stop = float(units.to_integral_value(rounding=ROUND_CEILING if buy else ROUND_FLOOR) * Decimal(str(tick_size)))
            else:
                stop = float(payload['sl'])
            if not math.isfinite(stop) or stop <= 0 or abs(stop/tick_size-round(stop/tick_size)) > 1e-6:
                raise ValueError('Common stop does not match the broker price step')
            take = float(payload['tp']) if 'tp' in payload else None
            if take is not None and (not math.isfinite(take) or take < 0 or take and abs(take/tick_size-round(take/tick_size)) > 1e-6):
                raise ValueError('Take profit does not match the broker price step')
            point = float(getattr(info, 'point', tick_size))
            minimum = float(getattr(info, 'trade_stops_level', 0)) * point
            freeze = float(getattr(info, 'trade_freeze_level', 0)) * point
            if not all(math.isfinite(x) and x >= 0 for x in (minimum,freeze)):
                raise ValueError('Invalid common stop broker distances')
            changes = [row for row in positions if abs(row.sl-stop) > 1e-8 or take is not None and abs(row.tp-take) > 1e-8]
            if changes and (direction*(quote-stop) <= 0 or direction*(quote-stop) < max(minimum,freeze)-1e-8):
                raise ValueError('Common stop is too close to the current quote')
            for row in changes:
                if freeze and direction*(quote-row.sl) < freeze-1e-8:
                    raise ValueError('Existing entry stop is frozen by the broker')
                desired_tp = row.tp if take is None else take
                if desired_tp and (direction*(desired_tp-quote) <= 0 or direction*(desired_tp-quote) < max(minimum,freeze)-1e-8):
                    raise ValueError('Existing entry take profit prevents common stop modification')

            protection_timing['validationMs']=round((time.perf_counter()-validation_started)*1000,3)
            observe_stage('validation',protection_timing['validationMs'],positionCount=len(positions),changedPositions=len(changes))
            losses_by_input={}
            def loss(row, price):
                key=(float(row.volume),float(row.price_open),float(price))
                if key in losses_by_input:return losses_by_input[key]
                profit = timed_read('riskCalculationMs',self.mt5.order_calc_profit,kind,symbol,*key)
                if profit is None or not math.isfinite(float(profit)):
                    raise ValueError('Cannot confirm common stop loss in USD')
                losses_by_input[key]=max(0., -float(profit))
                return losses_by_input[key]

            losses = {row.ticket:(loss(row, row.sl), loss(row, stop)) for row in positions}
            initial_risk = sum(before for before,after in losses.values())
            final_risk = sum(after for before,after in losses.values())
            observe_stage('risk-calculation',protection_timing.get('riskCalculationMs',0.))
            future_risk = 0.
            if 'remainingVolume' in payload or 'adversePrice' in payload:
                remaining, adverse = float(payload.get('remainingVolume', -1)), float(payload.get('adversePrice', 0))
                if not math.isfinite(remaining) or remaining < 0 or not math.isfinite(adverse) or adverse <= 0:
                    raise ValueError('Invalid remaining hybrid target risk')
                future_stop = adverse-direction*float(payload['futureStopDistance']) if 'futureStopDistance' in payload else stop
                if not math.isfinite(future_stop) or future_stop <= 0:
                    raise ValueError('Invalid future hybrid stop')
                profit = self.mt5.order_calc_profit(kind, symbol, remaining, adverse, future_stop) if remaining else 0.
                if profit is None or not math.isfinite(float(profit)):
                    raise ValueError('Cannot confirm remaining target risk in USD')
                future_risk = max(0., -float(profit))
            weighted = payload.get('weightedEntryStop') is True
            if 'weightedEntryStop' in payload and (not weighted or symbol not in ('USTEC','SPX500')
                    or 'stopDistance' not in payload
                    or take is not None):
                raise ValueError('Invalid weighted Nasdaq entry stop policy')
            if final_risk > budget + 1e-8 or not weighted and initial_risk > budget + 1e-8:
                raise ValueError('Common stop exceeds the reserved risk budget')
            base.update(averagePrice=average, sl=stop, riskUsed=final_risk, filledVolume=volume,
                        projectedRisk=final_risk+future_risk,
                        positions=[dict(ticket=str(row.ticket), volume=row.volume, fillPrice=row.price_open,
                                        sl=row.sl, tp=row.tp) for row in positions])
            if take is not None:
                base['tp'] = take
            if dry_run:
                base['positions'] = [dict(row, sl=stop, tp=row['tp'] if take is None else take) for row in base['positions']]
                return finish('ok')
            changes.sort(key=lambda row: direction*(stop-row.sl) < 0)
            # Tighten first. At every later step total loss must remain below
            # the greater of the already existing loss and the captured budget.
            projected = initial_risk
            for row in changes:
                before,after = losses[row.ticket]
                projected += after-before
                if projected > max(initial_risk, budget)+1e-8:
                    return finish('rejected', 'Common stop intermediate risk is not bounded')
            # Save the whole operation before the first potentially mutating
            # RPC. A crash returns unknown and never resumes the sequence.
            if changes:
                finish('unknown', 'Common stop intent saved before modification')
                claimed = True
            for row in changes:
                command_id = 'entry-sl:' + hashlib.sha256((identifier+':'+str(row.ticket)).encode()).hexdigest()
                request = dict(id=command_id, symbol=symbol, ticket=str(row.ticket), target='position',
                               sl=stop, expiresAt=time.time()*1000+3000,
                               entryStopGuard=dict(volume=row.volume, fillPrice=row.price_open, sl=row.sl, tp=row.tp))
                if take is not None:
                    request['tp'] = take
                modification_started=time.perf_counter()
                response = self.execute('modify', request)
                result = response['result']
                elapsed=(time.perf_counter()-modification_started)*1000
                protection_timing['modifyMs']=round(protection_timing.get('modifyMs',0.)+elapsed,3)
                observe_stage('broker-modification',elapsed,**{
                    name:response.get('serverTiming',{}).get(name) for name in ('mt5Ms','ledgerBeforeMs','ledgerAfterMs')})
                if result.get('status') != 'ok':
                    return finish('unknown' if result.get('status') == 'unknown' else 'rejected',
                                  'Common stop modification not confirmed: '+str(result.get('reason',result.get('status'))))
            verified = timed_read('finalReadMs',self.mt5.positions_get)
            verified_at=time.time()*1000
            if verified is None:
                return finish('unknown', 'Cannot verify common stop after modification')
            verified = {str(row.ticket):row for row in verified}
            for row in positions:
                actual = verified.get(str(row.ticket))
                if (not actual or actual.symbol != symbol or actual.type != kind or any(
                        abs(float(getattr(actual, attr))-value) > 1e-8
                        for attr,value in (('volume',row.volume),('price_open',row.price_open),('sl',stop),('tp',row.tp if take is None else take)))):
                    return finish('unknown', 'Entry position closed or changed during common stop modification')
            base['positions'] = [dict(row, sl=stop, tp=row['tp'] if take is None else take) for row in base['positions']]
            # Exact evidence from this final read; only in-memory bookkeeping
            # may reuse it. Every later native command keeps its own guards.
            base['protectionProof']=dict(schema='entry-protection-proof-v1',commandId=identifier,
                account=base['account'],server=account.server,symbol=symbol,checkedAt=verified_at,
                positions=base['positions'])
            observe_stage('final-position-read',protection_timing['finalReadMs'],positionCount=len(positions))
            return finish('ok')
        except (ValueError, TypeError, KeyError, OverflowError) as error:
            return finish('unknown' if claimed else 'rejected', str(error))
        except Exception:
            return finish('unknown', 'Common stop update interrupted; inspect MT5')

    @serialized
    def entry_limit_state(self, ticket, symbol):
        """Read only the captured opening, including a partial pending remainder."""
        if not re.fullmatch(r'[1-9][0-9]{0,19}', str(ticket)) or symbol not in self.config['symbols']:
            raise ValueError('Invalid captured limit ticket or symbol')
        data = self._orders(self.account(), symbol)
        return self._entry_limit_view(ticket, symbol, data)

    def _entry_limit_view(self, ticket, symbol, data):
        evidence={}
        matches = self.opening_rows(ticket, symbol, data['orders'],evidence)
        evidence['matches']=matches
        pending = [row for row in matches if not row['__isPosition']]
        positions = [row for row in matches if row['__isPosition']]
        if len(pending)>1 or len(positions)>1:
            raise ValueError('Ambiguous pending opening identity')
        state = self.opening_state(ticket, symbol, data['orders'],evidence) if not matches else {'phase':'active'}
        return dict(account=data['account'], server=data['server'], pending=pending, positions=positions, terminal=state)

    @serialized
    def entry_cancel_limit(self, request, expected, watch=()):
        """One RPC, same durable cancel, then exact fill/cancel proof. No replay."""
        started=time.perf_counter()
        account=self.account()
        if (getattr(account,'margin_mode',None)!=2 or getattr(account,'trade_allowed',True) is False
                or getattr(account,'trade_expert',True) is False):
            raise ValueError('Account does not permit hedged entry cancellation')
        ticket,symbol=str(request.get('ticket','')),request.get('symbol')
        if not re.fullmatch(r'[1-9][0-9]{0,19}',ticket) or symbol not in self.config['symbols']:
            raise ValueError('Invalid captured limit ticket or symbol')
        if (expected.get('side') not in ('buy','sell') or any(not isinstance(expected.get(key),(int,float))
                or isinstance(expected[key],bool) or not math.isfinite(expected[key])
                or expected[key] < (0 if key=='tp' else 1e-12) for key in ('price','volume','sl','tp'))):
            raise ValueError('Invalid captured cancellation reservation')
        if len(watch)>200 or any(str(row.get('account'))!=str(account.login) or row.get('symbol')!=symbol for row in watch):
            raise ValueError('Invalid cancellation position watch')
        before=self._entry_limit_view(ticket,symbol,self._orders(account,symbol))
        timing=dict(preCheckMs=round((time.perf_counter()-started)*1000,3))
        for row in before['pending']+before['positions']:
            if (row['type']!=expected['side'] or any(abs(float(row[key])-float(expected[key]))>1e-8 for key in ('sl','tp'))):
                raise ValueError('Captured limit protection or side changed')
        if before['pending']:
            pending=before['pending'][0]
            volume=sum(row['lots'] for row in before['pending']+before['positions'])
            if abs(volume-expected['volume'])>1e-8 or abs(pending['open_price']-expected['price'])>1e-8:
                raise ValueError('Captured pending reservation changed')
            receipt=self.execute('cancel',request)['result']
        else:
            receipt=dict(status='rejected',dispatchNotSent=True,reason='No pending remainder; exact broker state obtained')
        timing['cancelFinishedMs']=round((time.perf_counter()-started)*1000,3)
        if receipt.get('status') not in ('ok','rejected'):
            return dict(receipt=receipt,view=None,timing=timing)
        proof=self._orders(self.account(),symbol)
        proof['openingStates']={str(row['ticket']):self.opening_state(str(row['ticket']),symbol,proof['orders']) for row in watch}
        proof['checkedAt']=time.time()*1000
        view=self._entry_limit_view(ticket,symbol,proof)
        timing['totalMs']=round((time.perf_counter()-started)*1000,3)
        return dict(receipt=receipt,view=view,proof=proof,timing=timing)

    def target(self, payload, position):
        text = str(payload.get('ticket', ''))
        if not re.fullmatch(r'[1-9][0-9]{0,19}', text):
            raise ValueError('A specific MT5 ticket is required')
        rows = self.mt5.positions_get(ticket=int(text)) if position else self.mt5.orders_get(ticket=int(text))
        if rows is None:
            raise ValueError('Cannot read MT5 positions or orders')
        target = next((x for x in rows if x.ticket == int(text)), None)
        if target is None:
            raise ValueError('Position or order no longer exists; refresh the list')
        if target.symbol not in self.config['symbols'] or target.symbol != payload.get('symbol'):
            raise ValueError('Ticket symbol does not match the allowed symbol')
        return target

    def control_request(self, operation, payload, timing=None):
        if operation not in ('cancel', 'close', 'modify'):
            raise ValueError('Unsupported operation')
        position = operation == 'close' or (operation == 'modify' and payload.get('target') == 'position')
        if operation == 'modify' and payload.get('target') not in ('position', 'order'):
            raise ValueError('Specify position or pending order')
        target = measured(timing,'targetReadMs',self.target,payload,position)
        if 'entryStopGuard' in payload:
            guard = payload['entryStopGuard']
            if operation != 'modify' or not position or not isinstance(guard,dict) or set(guard) != {'volume','fillPrice','sl','tp'}:
                raise ValueError('Invalid common stop position guard')
            for key,attr in (('volume','volume'),('fillPrice','price_open'),('sl','sl'),('tp','tp')):
                value = float(guard[key])
                if not math.isfinite(value) or abs(float(getattr(target,attr))-value) > 1e-8:
                    raise ValueError('Entry position changed before its stop update')
        request = {'symbol': target.symbol, 'position' if position else 'order': target.ticket}
        if operation == 'cancel':
            return {**request, 'action': self.mt5.TRADE_ACTION_REMOVE}
        if operation == 'modify':
            if not any(key in payload for key in ('sl', 'tp', 'price')):
                raise ValueError('No changes supplied')
            sl, tp = float(payload.get('sl', target.sl)), float(payload.get('tp', target.tp))
            if not all(math.isfinite(x) and x >= 0 for x in (sl, tp)):
                raise ValueError('Invalid absolute SL or TP')
            request.update(action=self.mt5.TRADE_ACTION_SLTP if position else self.mt5.TRADE_ACTION_MODIFY, sl=sl, tp=tp)
            if position and 'price' in payload:
                raise ValueError('An open position entry price cannot be changed')
            if not position:
                price = float(payload.get('price', target.price_open))
                if not math.isfinite(price) or price <= 0:
                    raise ValueError('Invalid pending order price')
                request.update(price=price, type_time=target.type_time, expiration=target.time_expiration,
                               stoplimit=getattr(target, 'price_stoplimit', 0.))
            return request
        # Hedging: a reverse deal MUST carry the selected position ticket.
        # Volume is bounded by the current position; never create a new hedge.
        info = measured(timing,'symbolInfoMs',self.mt5.symbol_info,target.symbol)
        tick = measured(timing,'quoteReadMs',self.mt5.symbol_info_tick,target.symbol)
        if not info or not tick or target.type not in (self.mt5.ORDER_TYPE_BUY, self.mt5.ORDER_TYPE_SELL):
            raise ValueError('Cannot obtain position execution data')
        volume = float(payload.get('volume', 0))
        expected = float(payload.get('expectedVolume', 0))
        if not math.isfinite(expected) or abs(expected - target.volume) > 1e-8:
            raise ValueError('Position volume changed; refresh before closing')
        step, minimum, maximum = info.volume_step, info.volume_min, info.volume_max
        if not math.isfinite(volume) or volume <= 0 or step <= 0 or volume > target.volume + 1e-8:
            raise ValueError('Invalid close volume')
        if volume < minimum - 1e-8 or volume > maximum + 1e-8 or abs(volume / step - round(volume / step)) > 1e-7:
            raise ValueError('Close volume does not match broker lot limits')
        remaining = target.volume - volume
        if remaining > 1e-8 and (remaining < minimum - 1e-8 or abs(remaining / step - round(remaining / step)) > 1e-7):
            raise ValueError('Remaining position volume is below broker lot limits')
        buy = target.type == self.mt5.ORDER_TYPE_BUY
        price = tick.bid if buy else tick.ask
        if not math.isfinite(price) or price <= 0:
            raise ValueError('No valid close quote')
        filling = info.filling_mode
        if filling & 1:
            fill = self.mt5.ORDER_FILLING_FOK
        elif filling & 2:
            fill = self.mt5.ORDER_FILLING_IOC
        elif getattr(info, 'trade_exemode', 2) != getattr(self.mt5, 'SYMBOL_TRADE_EXECUTION_MARKET', 2):
            fill = self.mt5.ORDER_FILLING_RETURN
        else:
            raise ValueError('No supported market filling mode')
        return {**request, 'action': self.mt5.TRADE_ACTION_DEAL,
                'type': self.mt5.ORDER_TYPE_SELL if buy else self.mt5.ORDER_TYPE_BUY,
                'volume': volume, 'price': price, 'type_filling': fill,
                'deviation': int(self.config.get('deviationPoints', 20)),
                'magic': int(self.config.get('magic', 120009)), 'comment': 'ISCC close'}

    def orders(self):
        return self._orders(self.account())

    def _orders(self, account, symbol=None):
        query={} if symbol is None else dict(symbol=symbol)
        positions = self.read_timings.call('positionsRead', self.mt5.positions_get, **query)
        pending = self.read_timings.call('ordersRead', self.mt5.orders_get, **query)
        if positions is None or pending is None:
            raise ValueError('Cannot read MT5 positions or orders')
        orders = []
        # Specifications are fresh for this read batch. Re-reading the same
        # symbol for every owned part adds native calls without new evidence.
        specifications = {}
        for position, rows in ((True, positions), (False, pending)):
            for x in rows:
                if x.symbol not in self.config['symbols']:
                    continue
                if x.symbol not in specifications:
                    specifications[x.symbol] = self.mt5.symbol_info(x.symbol)
                info = specifications[x.symbol]
                orders.append({'ticket': str(x.ticket), 'symbol': x.symbol,
                               'lots': x.volume if position else x.volume_current,
                               'open_price': x.price_open, 'sl': x.sl, 'tp': x.tp,
                               'comment': x.comment, 'pnl': getattr(x, 'profit', None),
                               'currentPrice': getattr(x, 'price_current', None),
                               'type': 'buy' if x.type in (0, 2, 4, 6) else 'sell',
                               'orderType': x.type, '__isPosition': position,
                               'identifier': str(getattr(x, 'identifier', x.ticket)),
                               **({'positionId':str(getattr(x, 'position_id', 0))} if not position else {}),
                               'quantityStep': info.volume_step if info else None,
                               'maxQty': info.volume_max if info else None,
                               'minQty': info.volume_min if info else None})
                if not position:
                    broker_evidence = opening_order_evidence(x, str(x.ticket), x.symbol)
                    if broker_evidence:
                        orders[-1]['brokerOrderEvidence'] = broker_evidence
        observer=getattr(getattr(self,'chunk_entries',None),'progress_observer',None)
        if observer is not None and symbol is None:
            observer.offer(account.login,orders,time.time()*1000)
        return dict(account=account.login,server=account.server,orders=orders)

    def _request_receipt(self, identifier):
        row = self.db.execute('SELECT result FROM requests WHERE id=?', (identifier,)).fetchone()
        return json.loads(row[0]) if row else None

    def receipt(self, identifier):
        with self.lock:
            for table in ('chunk_entry_protections', 'chunk_entry_closures'):
                saved = self.db.execute('SELECT result FROM '+table+' WHERE id=?', (identifier,)).fetchone()
                if saved:
                    return self.envelope(result=json.loads(saved[0]))
        with self.group_lock:
            with self.lock:
                group = self.db.execute('SELECT payload,result FROM groups WHERE id=?', (identifier,)).fetchone()
                if not group:
                    return self.envelope(result=self._request_receipt(identifier))
                stored, result = json.loads(group[0]), json.loads(group[1])
            if result['status']=='unknown':
                results = dict(result['accountResults'])
                for account,request in stored['requests'].items():
                    if results[account]['status']!='unknown': continue
                    if account==str(self.config['account']):
                        with self.lock:
                            receipt = self._request_receipt(request['id'])
                    else:
                        try: receipt = self.linked[account].call('receipt',request['id'])['result']
                        except (OSError, ValueError, RuntimeError): receipt = None
                    if receipt: results[account]=receipt
                result = self.group_result(results)
                with self.lock:
                    self.db.execute('UPDATE groups SET result=? WHERE id=?',(json.dumps(result),identifier))
                    self.db.commit()
            with self.lock: return self.envelope(result=result)

    def local_management(self, identifiers=(), watch=()):
        with self.read_timings.span('localManagement'), self.lock:
            # Validate once for this locked read batch. Command preflight and
            # final execute still validate each actual account independently.
            account=self.account()
            health = self._health(account)
            rules=[dict(id=i,**json.loads(p),state=s,result=json.loads(r)) for i,p,s,r in
                   self.db.execute('SELECT * FROM close_rules ORDER BY rowid DESC LIMIT 100')]
            state = {**health, 'orders':self.decorated_orders(account)['orders'],
                     'rules':rules, 'timeRules':self.time_rules(), 'checkedAt':time.time()*1000}
            state['openingStates']={str(x['ticket']):self.opening_state(str(x['ticket']),x['symbol'],state['orders']) for x in watch}
        # Receipt reconciliation can wait for a linked terminal. Never hold the
        # primary quote lock while acquiring the group lock or doing that RPC.
        return {**state, 'receipts':{x:self.receipt(x)['result'] for x in identifiers}}

    @serialized
    def management_snapshot(self, identifiers=(), watch=()):
        """Native evidence only; formatting/history decoration runs outside RPC."""
        account=self.account()
        state=dict(self._health(account),orders=self._orders(account)['orders'])
        state['openingStates']={str(row['ticket']):self.opening_state(str(row['ticket']),row['symbol'],state['orders']) for row in watch}
        state['_closeRules']=self.db.execute('SELECT * FROM close_rules ORDER BY rowid DESC LIMIT 100').fetchall()
        state['_timeRules']=self.db.execute('SELECT * FROM time_rules ORDER BY rowid DESC LIMIT 100').fetchall()
        state['receipts']={x:self.receipt(x)['result'] for x in identifiers}
        state['checkedAt']=time.time()*1000
        return state

    @serialized
    def entry_management(self, watch=()):
        """Fresh entry proof without UI-only historical group decoration."""
        return self._entry_management_for(watch,self._entry_read_account())

    def _entry_management_for(self, watch, account):
        state = self._orders(account)
        state['openingStates'] = {str(row['ticket']):self.opening_state(str(row['ticket']),row['symbol'],state['orders'])
                                  for row in watch}
        state['checkedAt'] = time.time()*1000
        return state

    @serialized
    def entry_positions(self, watch=()):
        return self._entry_positions_for(watch,self._entry_read_account())

    def _entry_positions_for(self, watch, account):
        """Exact captured positions only; pending reservations use full reads."""
        if len(watch)>200 or any(row.get('symbol') not in self.config['symbols']
                or not re.fullmatch(r'[1-9][0-9]{0,19}',str(row.get('ticket',''))) for row in watch):
            raise ValueError('Invalid exact position watch')
        rows=self.read_timings.call('positionsRead',self.mt5.positions_get)
        if rows is None:raise ValueError('Cannot read captured positions')
        orders=[dict(ticket=str(row.ticket),symbol=row.symbol,lots=row.volume,open_price=row.price_open,
            sl=row.sl,tp=row.tp,type='buy' if row.type==0 else 'sell',__isPosition=True,
            identifier=str(getattr(row,'identifier',row.ticket))) for row in rows if row.symbol in self.config['symbols']]
        return dict(account=account.login,server=account.server,orders=orders,checkedAt=time.time()*1000,
            openingStates={str(row['ticket']):self.opening_state(str(row['ticket']),row['symbol'],orders) for row in watch})

    @serialized
    def entry_reconcile(self, items):
        """Read exact durable replies and broker openings; never submit/replay."""
        if not isinstance(items, list) or not 1 <= len(items) <= 16:
            raise ValueError('A bounded exact reconciliation batch is required')
        for item in items:
            if (not isinstance(item, dict) or set(item) != {'id','ticket','symbol','cancelIds'}
                    or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,160}', str(item['id']))
                    or item['symbol'] not in self.config['symbols']
                    or item['ticket'] and not re.fullmatch(r'[1-9][0-9]{0,19}', str(item['ticket']))
                    or not isinstance(item['cancelIds'], list) or len(item['cancelIds']) > 16
                    or any(not re.fullmatch(r'[A-Za-z0-9_.:-]{1,160}', str(value)) for value in item['cancelIds'])):
                raise ValueError('Invalid exact reconciliation identity')
        account = self.account()
        data = self._orders(account)
        results, quotes = {}, {}
        for item in items:
            saved = self.db.execute('SELECT result FROM requests WHERE id=?', (item['id'],)).fetchone()
            receipt = json.loads(saved[0]) if saved else None
            ticket = str((receipt or {}).get('providerOrderId') or item['ticket'] or '')
            row = dict(receipt=receipt, ticket=ticket, pending=[], positions=[], terminal={})
            if re.fullmatch(r'[1-9][0-9]{0,19}', ticket):
                if item['ticket'] and str(item['ticket']) != ticket:
                    raise ValueError('Reconciled opening ticket changed')
                matches = self.opening_rows(ticket, item['symbol'], data['orders'])
                row.update(pending=[value for value in matches if not value['__isPosition']],
                           positions=[value for value in matches if value['__isPosition']],
                           terminal=self.opening_state(ticket, item['symbol'], data['orders']) if not matches else dict(phase='active'))
            row['cancellations'] = {}
            for identifier in item['cancelIds']:
                cancelled = self.db.execute('SELECT result FROM requests WHERE id=?', (identifier,)).fetchone()
                row['cancellations'][identifier] = json.loads(cancelled[0]) if cancelled else None
            results[item['id']] = row
            if item['symbol'] not in quotes:
                tick = self.mt5.symbol_info_tick(item['symbol'])
                if tick is not None:
                    quotes[item['symbol']] = dict(bid=float(tick.bid), ask=float(tick.ask),
                        tickUtcMs=self.tick_utc_seconds(tick)*1000)
        return dict(account=str(account.login), server=account.server, checkedAt=time.time()*1000,
                    readOnly=True, items=results, quotes=quotes)

    @serialized
    def entry_confirm_reconciled(self, expected):
        """Commit positive native opening proof, preserving its original reply.

        Coordinator-only finalization; no broker mutation and no replay. The
        background observer remains read-only. This also restores the original
        unclaimed deadline after a timeout with an exact broker opening ID.
        """
        if not isinstance(expected, dict) or set(expected) != {'id','ticket','symbol','type','side','volume','price','sl','tp'}:
            raise ValueError('Invalid reconciled opening confirmation')
        identifier, ticket, symbol = str(expected['id']), str(expected['ticket']), expected['symbol']
        if (not re.fullmatch(r'[A-Za-z0-9_.:-]{1,160}', identifier) or not re.fullmatch(r'[1-9][0-9]{0,19}',ticket)
                or symbol not in self.config['symbols'] or expected['type'] not in ('market','limit')
                or expected['side'] not in ('buy','sell')):
            raise ValueError('Invalid reconciled opening identity')
        for name in ('volume','price','sl','tp'):
            value = expected[name]
            if type(value) not in (int,float) or not math.isfinite(value) or value < 0 or name != 'tp' and value == 0:
                raise ValueError('Invalid reconciled opening bounds')
        self.account()
        saved = self.db.execute('SELECT result FROM requests WHERE id=?', (identifier,)).fetchone()
        original = json.loads(saved[0]) if saved else {}
        if (str(original.get('providerOrderId')) != ticket or original.get('operation')
                or original.get('status') not in ('unknown','ok')):
            raise ValueError('Reconciled opening contradicts its native journal')
        if original['status'] == 'ok':
            return original
        rows = self.opening_rows(ticket, symbol, self._orders(self.account())['orders'])
        pending = [row for row in rows if not row['__isPosition']]
        positions = [row for row in rows if row['__isPosition']]
        terminal = self.opening_state(ticket,symbol,[]) if not rows else {}
        proof = terminal.get('openingProof', {})
        if not rows and proof.get('kind') == 'exact-balanced-opening-v1':
            positions = [dict(type=proof['side'],lots=proof['volume'],open_price=proof['fillPrice'],sl=proof['sl'],tp=proof['tp'])]
        cancelled = not rows and expected['type']=='limit' and terminal.get('phase')=='cancelled'
        if cancelled:
            history = self.mt5.history_orders_get(ticket=int(ticket))
            order = next((row for row in history or () if str(row.ticket)==ticket and row.symbol==symbol),None)
            if (order is None or order.type != (self.mt5.ORDER_TYPE_BUY_LIMIT if expected['side']=='buy' else self.mt5.ORDER_TYPE_SELL_LIMIT)
                    or any(not math.isfinite(float(getattr(order,field,float('nan'))))
                           or abs(float(getattr(order,field))-expected[key])>1e-8
                           for field,key in (('volume_initial','volume'),('price_open','price'),('sl','sl'),('tp','tp')))):
                raise ValueError('Cancelled native opening does not match its reservation')
        if (len(pending)>1 or len(positions)>1 or not (pending or positions or cancelled)
                or expected['type']=='market' and (pending or len(positions)!=1)):
            raise ValueError('Native opening is not positively confirmed')
        volume = expected['volume'] if cancelled else sum(float(row['lots']) for row in pending+positions)
        if abs(volume-expected['volume']) > 1e-8:
            raise ValueError('Native opening volume differs from its full reservation')
        for row in pending+positions:
            if (row['type'] != expected['side'] or abs(float(row['sl'])-expected['sl'])>1e-8
                    or abs(float(row['tp'])-expected['tp'])>1e-8
                    or (not row.get('__isPosition', True) or expected['type']=='market')
                        and abs(float(row['open_price'])-expected['price'])>1e-8):
                raise ValueError('Native opening price, side or protection differs')
        result = {**original, 'status':'ok', 'nativeReconciled':True, 'nativePreviousResult':original,
            'nativeProof':dict(kind='exact-opening-state-v1',checkedAt=time.time()*1000,ticket=ticket,symbol=symbol,
                volume=volume,terminal=terminal.get('phase'),pending=bool(pending))}
        if expected['type']=='market':
            result.update(filledVolume=volume,fillPrice=float(positions[0]['open_price']))
        self.db.execute('UPDATE requests SET result=? WHERE id=?',(json.dumps(result,allow_nan=False),identifier))
        deadline = self.db.execute('SELECT payload,state FROM time_rules WHERE id=?',(identifier,)).fetchone()
        if deadline and deadline[1] in ('opening','unknown'):
            rule = json.loads(deadline[0])
            if rule.get('openingId')!=identifier or rule.get('actions') or rule.get('commandId') or str(rule.get('ticket'))!=ticket:
                self.db.rollback()
                raise ValueError('Unknown deadline action cannot be reconciled as an opening')
            self.db.execute('UPDATE time_rules SET state=?,result=? WHERE id=?',
                ('completed' if terminal.get('phase') in ('closed','cancelled') else 'armed',json.dumps(result,allow_nan=False),identifier))
        self.db.commit()
        return result

    def entry_confirm_group(self, identifier):
        """Project exact final account journals into their original group."""
        with self.lock:
            saved = self.db.execute('SELECT payload,result FROM groups WHERE id=?',(identifier,)).fetchone()
        if not saved:
            raise ValueError('Reconciled group is missing')
        stored, original = (json.loads(text) for text in saved)
        requests = stored.get('requests', {})
        primary = str(self.config['account'])
        if stored.get('operation')!='place' or not requests or not set(requests).issubset({primary, *self.linked}):
            raise ValueError('Only an exact opening group can be reconciled')
        outcomes = {}
        for account, request in requests.items():
            expected_id = identifier if account == primary else identifier+':'+account
            if request.get('id')!=expected_id:
                raise ValueError('Reconciled group request identity changed')
            reply = self.receipt(expected_id) if account == primary else self.linked[account].call('receipt',expected_id)
            result = reply.get('result', {})
            old = original.get('accountResults', {}).get(account, {})
            if (result.get('status') not in ('ok','rejected')
                    or old.get('status')=='rejected' and result.get('status')!='rejected'
                    or old.get('providerOrderId') and str(old['providerOrderId'])!=str(result.get('providerOrderId'))):
                raise ValueError('Account opening journal is not final or contradicts its group')
            outcomes[account] = dict(result,requestedVolume=request['volume'])
        status = ('ok' if all(row['status']=='ok' for row in outcomes.values()) else
                  'rejected' if all(row['status']=='rejected' for row in outcomes.values()) else 'unknown')
        result = dict(outcomes[primary],status=status,primaryAccount=primary,accountResults=outcomes,
                      nativePreviousResult=original,nativeReconciled=True)
        with self.lock:
            current = self.db.execute('SELECT payload,result FROM groups WHERE id=?',(identifier,)).fetchone()
            if current != saved:
                raise ValueError('Opening group changed during confirmation')
            self.db.execute('UPDATE groups SET result=? WHERE id=?',(json.dumps(result,allow_nan=False),identifier))
            self.db.commit()
        return result

    def entry_plans(self):
        """Fresh lifecycle summary without unrelated UI history decoration."""
        entries = self.chunk_entries.list()
        if not entries and getattr(self, 'isolated_entries', False):
            return dict(account=int(self.config['account']), server=self.config['server'], entries=[])
        if any(entry.get('executionPolicy') == 'hybrid-market-limit-v1'
               and (entry['phase'] in ('running','sending')
                    or entry.get('accountExecution') in ('independent-v3','independent-v4','independent-v5') and entry['phase'] in ('armed','paused')
                    or entry.get('reconciliation', {}).get('status') == 'checking') for entry in entries):
            # The coordinator already validates native positions before every
            # new part. UI polls must not enqueue a second account scan in its
            # critical path. This response makes no fresh presence/absence claim.
            return dict(account=str(self.config['account']), server=self.config['server'],
                entries=[dict(entry, lifecycleDeferred=True, positionsConfirmed=False) for entry in entries])
        if getattr(self, 'isolated_entries', False):
            states = self.management_accounts()
            return dict(account=str(self.config['account']), server=self.config['server'],
                        entries=self.entry_plan_views(states))
        primary = self.entry_management()
        states = {str(self.config['account']): dict(connected=True, **primary)}
        for account, worker in self.linked.items():
            try:
                state = worker.call('entry_management')
                if str(state.get('account')) != account or not isinstance(state.get('orders'), list):
                    raise ValueError('Entry status account mismatch or incomplete positions')
                states[account] = dict(connected=True, **state)
            except Exception:
                # Missing account evidence must keep owned positions uncertain,
                # never make a card look flat or unlock another entry.
                states[account] = dict(connected=False,
                    reason='Второй терминал недоступен или восстанавливается')
        return dict(account=primary['account'], server=primary['server'], entries=self.entry_plan_views(states))

    def management(self, identifiers=(), receipt_account=None, watch=()):
        # One HTTP round trip; linked state is for management only, never quotes.
        primary_account=str(self.config['account'])
        states = self.management_accounts(identifiers,receipt_account,watch)
        primary = states[primary_account]
        alerts = []
        groups = [(i,json.loads(p),json.loads(r)) for i,p,r in self.management_groups(states)]
        primary_account=str(self.config['account'])
        hybrid_cache = {}
        for identifier,stored,result in groups:
            if stored['operation']!='place': continue
            if stored.get('independentAccounts') is True: continue
            results=result.get('accountResults',{})
            expected=self.expected_group_volumes(stored,result,states,groups,hybrid_cache)
            def matched(account):
                provider=str(results.get(account,{}).get('providerOrderId',''))
                return next((x for x in states[account].get('orders',[]) if x['symbol']==stored['payload']['symbol'] and provider in (str(x['ticket']),str(x.get('identifier')))),None)
            left=matched(primary_account)
            if left:
                left['linkedPositions']=[]
                for mirror in stored['requests']:
                    if mirror==primary_account: continue
                    right=matched(mirror) if mirror in states else None
                    left['linkedPositions'].append(dict(account=mirror,**{key:right[key] for key in ('ticket','lots','quantityStep','minQty','maxQty')}) if right else
                                                  dict(account=mirror,error='Связанная позиция недоступна; обновите список'))
            for mirror in stored['requests']:
                if mirror==primary_account or not states.get(mirror,{}).get('connected'): continue
                right=matched(mirror)
                if not left and not right: continue
                reason=('Позиция или заявка есть только на одном счёте' if not left or not right else
                        'На одном счёте позиция, на другом ещё заявка' if left['__isPosition']!=right['__isPosition'] else
                        'Объёмы на счетах изменены вне связанных команд' if any(abs(row['lots']-expected[account])>1e-8 for account,row in ((primary_account,left),(mirror,right))) else
                        'SL или TP на счетах различаются' if any(abs(float(left[key])-float(right[key]))>1e-8 for key in ('sl','tp')) else None)
                if reason: alerts.append(dict(groupId=identifier,symbol=stored['payload']['symbol'],reason=reason,tickets={primary_account:left and left['ticket'],mirror:right and right['ticket']}))
        for account,state in states.items():
            if not state['connected']: alerts.append({'account':account,'reason':state['reason']})
        receipts = (states.get(receipt_account,{}).get('receipts',{}) if receipt_account in self.linked else
                    {identifier:self.receipt(identifier)['result'] for identifier in identifiers})
        self.chunk_entries.decorate_management(states)
        return {**primary,'accountStates':states,'alerts':alerts,'receipts':receipts,'entryPlans':self.entry_plan_views(states)}

    def management_accounts(self, identifiers=(), receipt_account=None, watch=()):
        primary_account=str(self.config['account'])
        primary = self.local_management(watch=[x for x in watch if str(x['account'])==primary_account])
        states = {str(self.config['account']): {'connected':True, **primary}}
        for account, worker in self.linked.items():
            try:
                state = worker.call('local_management',identifiers if receipt_account==account else (),[x for x in watch if str(x['account'])==account])
                states[account] = {'connected':True, **state}
            except Exception:
                states[account] = {'connected':False, 'reason':'Второй терминал недоступен или восстанавливается'}
        return states

    def entry_plan_views(self, states):
        entries=self.chunk_entries.list()
        for entry in entries:
            live=[]
            for account in entry['accounts']:
                rows=states.get(account,{}).get('orders',[])
                identifiers=set()
                for child in entry['children']:
                    position=child.get('positions',{}).get(account,{})
                    identifiers.update(str(position[key]) for key in ('ticket','openingTicket') if position.get(key))
                    result=child.get('result',{})
                    outcome=result.get('accountResults',{}).get(account,result if account==str(self.config['account']) else {})
                    if outcome.get('providerOrderId'): identifiers.add(str(outcome['providerOrderId']))
                for row in rows:
                    if row.get('__isPosition') and row['symbol']==entry['symbol'] and identifiers.intersection((str(row['ticket']),str(row.get('identifier')))):
                        live.append(dict(account=account,ticket=row['ticket'],volume=row['lots']))
            entry['positionsConfirmed']=all(states.get(account,{}).get('connected')
                and states[account].get('evidenceReady') is not False for account in entry['accounts'])
            entry['livePositions']=live
            entry['hasOpenPositions']=bool(live) if entry['positionsConfirmed'] else None
        # Historical receipts stay durable in SQLite. Keep the frequently read
        # management response focused on active/uncertain entries and parents
        # that still own a live position; old completed histories cannot grow
        # every two-second UI update indefinitely.
        return [entry for entry in entries if entry['phase'] not in ('completed','stopped') or entry['hasOpenPositions'] is not False]


def account_worker(connection, config, database):
    # An orphan worker may still finish an in-flight broker call after its
    # parent dies. Do not let a replacement touch that account's ledger yet.
    with process_lock(pathlib.Path(database).with_suffix('.worker.lock')) as acquired:
        if not acquired:
            connection.send({'ready':False,'error':'Previous account worker is still running'})
            return
        _account_worker(connection,config,database)


def worker_result(method, result):
    # MT5 AccountInfo is a native namedtuple whose generated class cannot be
    # pickled by multiprocessing. Only plain identity/permission fields cross
    # this internal RPC boundary; Bridge.account() has already verified MT5.
    if method == 'account':
        return {key: getattr(result, key) for key in (
            'login', 'server', 'currency', 'margin_mode', 'trade_allowed', 'trade_expert')}
    return result


CANDLE_READ_METHODS=frozenset(('account','candle_levels','candle_symbols','candle_snapshot'))
QUOTE_READ_METHODS=frozenset(('account','snapshot','quote_frame','set_quote_mode'))

def native_worker_method(bridge, method, args, candle_only=False, quote_only=False):
    if candle_only and method not in CANDLE_READ_METHODS:
        raise ValueError('Candle client permits read-only chart operations only')
    if quote_only and method not in QUOTE_READ_METHODS:
        raise ValueError('Quote client permits read-only price operations only')
    return worker_result(method,getattr(bridge,method)(*args))

def _account_worker(connection, config, database):
    import MetaTrader5 as mt5
    bridge = None
    try:
        if not mt5.initialize(path=config['terminalPath']):
            raise RuntimeError('Cannot connect to linked MT5 terminal')
        candle_only=config.get('_candleOnly') is True
        quote_only=config.get('_quoteOnly') is True
        read_only=candle_only or quote_only
        bridge = Bridge(mt5,dict(config,_nativeWorker=True,**({'tradingEnabled':False} if read_only else {})),database)
        bridge.account()
        if not read_only:bridge.start_rules()
        connection.send({'ready':True,'volumeSpecs':{} if read_only else bridge.lot_specs()})
        while True:
            if not connection.poll(1):
                continue
            identifier,method,args = connection.recv()
            urgent = method == 'execute' or method == 'check_opening' or method.startswith('entry_')
            if urgent: bridge.native_command_waiting.set()
            diagnostic=bridge.entry_diagnostics.begin('rpc-lock:'+method,reason='Waiting for native account lock')
            try:
                with bridge.lock:
                    bridge.native_command_revision += 1
                    bridge.entry_diagnostics.step(diagnostic,'rpc:'+method)
                    result = native_worker_method(bridge,method,args,candle_only,quote_only)
                connection.send((identifier,result,None))
            except Exception as error:
                bridge.entry_diagnostics.finish(diagnostic,error=error)
                diagnostic=None
                detail=getattr(error,'entry_diagnostics',None)
                connection.send((identifier,None,dict(reason=str(error),entryDiagnostics=detail) if detail is not None else str(error)))
            finally:
                if urgent: bridge.native_command_waiting.clear()
                bridge.entry_diagnostics.finish(diagnostic)
    except Exception as error:
        try: connection.send({'ready':False,'error':str(error)})
        except OSError: pass
    finally:
        if bridge:
            bridge.rule_stop.set()
            bridge.entry_diagnostics.close()
            if bridge.candle_book is not None:bridge.candle_book.close()
        mt5.shutdown()


class WorkerDispatchNotSent(RuntimeError):
    """A bounded dispatch gate refused the request before any pipe send."""
    dispatch_not_sent = True


class AccountWorker:
    def __init__(self, config, database, target=account_worker):
        self.lock = threading.RLock()
        self.context = multiprocessing.get_context('spawn')
        self.config, self.database, self.target = config, str(database), target
        self.connection = self.process = None
        self.phase = 'reconnecting'
        self.restarts = 0
        self._init_rpc_tracking()
        self.stop = threading.Event()
        self.supervisor = threading.Thread(target=self._supervise, daemon=True)
        self.supervisor.start()

    def available(self):
        return self.phase=='ready' and self.process is not None and self.process.is_alive()

    def _init_rpc_tracking(self):
        self.rpc_condition = threading.Condition()
        self.rpc_urgent_waiters = 0
        self.rpc_owner = None
        self.rpc_pending = None
        self.rpc_pending_method = None
        self.rpc_pending_started = None
        self.rpc_recent = deque(maxlen=48)
        self.rpc_counts = dict(calls=0, notSent=0, timeouts=0, stalledRecoveries=0)
        self.rpc_mutation_waiters = 0

    def rpc_snapshot(self):
        # Parent-side metadata only: no native call, arguments or credentials.
        with self.rpc_condition:
            owner = dict(self.rpc_owner) if self.rpc_owner else None
            if owner:
                owner.pop('started',None)
                owner['heldMs'] = round((time.monotonic()-self.rpc_owner['started'])*1000,3)
            return dict(schema=1, urgentWaiters=self.rpc_urgent_waiters, commandWaiters=self.rpc_mutation_waiters, owner=owner,
                pendingReply=bool(self.rpc_pending), pendingMethod=self.rpc_pending_method,
                pendingAgeMs=round((time.monotonic()-self.rpc_pending_started)*1000,3) if self.rpc_pending_started is not None else None,
                counts=dict(self.rpc_counts), recent=list(self.rpc_recent))

    def _rpc_acquire(self, method, deadline):
        urgent = method=='execute' or method=='check_opening' or method.startswith('entry_')
        mutation = method in ('execute','entry_cancel_limit','entry_common_stop')
        reentrant = self.lock._is_owned()
        with self.rpc_condition:
            self.rpc_urgent_waiters += int(urgent)
            self.rpc_mutation_waiters += int(mutation)
        try:
            while True:
                remaining = deadline-time.monotonic()
                if remaining <= 0:
                    return False
                with self.rpc_condition:
                    if ((not urgent and self.rpc_urgent_waiters)
                            or urgent and not mutation and self.rpc_mutation_waiters and not reentrant):
                        self.rpc_condition.wait(remaining)
                        continue
                if not self.lock.acquire(timeout=remaining):
                    return False
                with self.rpc_condition:
                    # A background read may already have been waiting on the
                    # mutex when a trade arrived. Yield before any pipe send.
                    if ((not urgent and self.rpc_urgent_waiters)
                            or urgent and not mutation and self.rpc_mutation_waiters and not reentrant):
                        self.lock.release()
                        continue
                    self.rpc_owner = dict(method=method, started=time.monotonic(), at=time.time()*1000)
                    return True
        finally:
            with self.rpc_condition:
                self.rpc_urgent_waiters -= int(urgent)
                self.rpc_mutation_waiters -= int(mutation)
                self.rpc_condition.notify_all()

    def _rpc_release(self):
        with self.rpc_condition:
            self.rpc_owner = None
        self.lock.release()
        with self.rpc_condition:
            self.rpc_condition.notify_all()

    def _dispose_locked(self):
        if self.process and self.process.pid is not None:
            if self.process.is_alive(): self.process.terminate()
            self.process.join(timeout=2)
        if self.connection: self.connection.close()
        self.connection = self.process = None
        self.rpc_pending = None
        self.rpc_pending_method = None
        self.rpc_pending_started = None

    def _drain_pending_locked(self):
        while self.rpc_pending and self.connection.poll(0):
            previous_id, _, _ = self.connection.recv()
            if previous_id == self.rpc_pending:
                self.rpc_pending = None
                self.rpc_pending_method = None
                self.rpc_pending_started = None

    def _recover_stalled(self):
        # Recover the Python connector only, never the MT5 terminal. A native
        # mutation was journalled before dispatch; its unknown claim survives
        # process replacement and must be reconciled, never resent here.
        if not self.lock.acquire(blocking=False):
            return False
        try:
            if not self.rpc_pending:
                return False
            self._drain_pending_locked()
            if not self.rpc_pending or self.rpc_pending_started is None or time.monotonic()-self.rpc_pending_started < 10:
                return False
            with self.rpc_condition:
                self.rpc_counts['stalledRecoveries'] += 1
                self.rpc_recent.append(dict(method=self.rpc_pending_method,outcome='worker-recovery',sent=True,
                    totalMs=round((time.monotonic()-self.rpc_pending_started)*1000,3),at=time.time()*1000))
            self.phase = 'recovering'
            return True
        except (OSError, EOFError):
            self.phase = 'reconnecting'
            return True
        finally:
            self.lock.release()

    def _supervise(self):
        delay = .5
        while not self.stop.is_set():
            if self.available() and not self._recover_stalled():
                self.stop.wait(.5)
                continue
            with self.lock:
                if self.stop.is_set(): break
                self.phase = 'reconnecting'
                self._dispose_locked()
                self.connection, child = self.context.Pipe()
                self.process = self.context.Process(target=self.target,
                    args=(child,self.config,self.database),daemon=not self.config.get('_candleOnly',False))
                try:
                    self.process.start()
                    child.close()
                    deadline = time.monotonic()+15
                    ready = None
                    while not self.stop.is_set() and time.monotonic()<deadline:
                        if self.connection.poll(.2):
                            ready = self.connection.recv()
                            break
                        if not self.process.is_alive(): break
                    if ready and ready.get('ready') and self.process.is_alive():
                        self.volume_specs=ready.get('volumeSpecs')
                        self.phase = 'ready'
                        self.restarts += 1
                        delay = .5
                    else: self._dispose_locked()
                except (OSError, EOFError, RuntimeError):
                    child.close()
                    self._dispose_locked()
            if self.phase!='ready':
                self.stop.wait(delay)
                delay = min(delay*2,15)

    def call(self, method, *args):
        return self._call(method, args)

    def call_if_idle(self, method, *args):
        """Low-priority read: do not queue behind an account trade or read."""
        if method != 'entry_reconcile':
            raise ValueError('Idle RPC is restricted to native reconciliation reads')
        if not self.lock.acquire(blocking=False):
            return dict(deferred=True)
        try:
            return self._call(method, args)
        finally:
            self.lock.release()

    def call_dispatched(self, sent, abort, deadline, method, *args):
        """Confirm the actual pipe handoff before another native call can block Python."""
        return self._call(method, args, (sent, abort, deadline))

    def _call(self, method, args, dispatch=None):
        # Recovery creates a new connection; it never resends the old message.
        started=time.perf_counter()
        sent_to_worker=False
        acquired=False
        outcome='error'
        lock_ms=0.
        def refused(reason):
            error=WorkerDispatchNotSent(reason)
            with self.rpc_condition:
                error.rpc_diagnostics=dict(method=method,
                    blockedBy=self.rpc_owner['method'] if self.rpc_owner and not acquired else self.rpc_pending_method,
                    heldMs=round((time.monotonic()-self.rpc_owner['started'])*1000,3) if self.rpc_owner else None,
                    pendingReply=bool(self.rpc_pending))
            raise error
        try:
            if not self.available(): refused('Linked MT5 is reconnecting')
            timeout = min(2., max(0.,dispatch[2]-time.monotonic())) if dispatch else 2.
            # Retain the original bounded wait and response deadline. Priority
            # changes admission order, never quote freshness or native guards.
            acquired=self._rpc_acquire(method,time.monotonic()+timeout)
            if not acquired: refused('Linked MT5 is busy; request was not sent')
            lock_ms=round((time.perf_counter()-started)*1000,2)
            if not self.available(): refused('Linked MT5 is reconnecting')
            if dispatch and (dispatch[1].is_set() or time.monotonic()>=dispatch[2]):
                refused('Follower dispatch deadline expired; request was not sent')
            # A timed-out RPC can still be executing in the terminal. Drain
            # its late reply before accepting another message; never build a
            # queue behind an unknown native operation or resend that ID.
            self._drain_pending_locked()
            if self.rpc_pending:
                refused('Linked MT5 previous response is pending; request was not sent')
            identifier = uuid.uuid4().hex
            send_started=time.perf_counter()
            self.connection.send((identifier,method,args))
            sent_to_worker=True
            self.rpc_pending=identifier
            self.rpc_pending_method=method
            self.rpc_pending_started=time.monotonic()
            send_ms=round((time.perf_counter()-send_started)*1000,2)
            if dispatch:
                dispatch[0]()
            # One combined reply also includes its fresh post-cancel proof.
            # Keep a bounded total budget rather than imposing the old single
            # execute deadline on several formerly separate read operations.
            deadline = time.monotonic()+(3 if method=='entry_cancel_limit' else 2)
            while self.connection.poll(max(0,deadline-time.monotonic())):
                reply_id,result,error = self.connection.recv()
                if reply_id!=identifier: continue
                self.rpc_pending=None
                self.rpc_pending_method=None
                self.rpc_pending_started=None
                if isinstance(error,dict) and isinstance(error.get('entryDiagnostics'),dict):
                    raise EntryPriceRejected(error['reason'],error['entryDiagnostics'])
                if error: raise ValueError(error)
                outcome='ok'
                if method in ('execute','entry_snapshot','entry_management','entry_positions','entry_admission','entry_context','entry_cancel_limit','entry_common_stop','management_snapshot') and isinstance(result,dict):
                    result={**result,'rpcTiming':dict(lockWaitMs=lock_ms,sendMs=send_ms,totalMs=round((time.perf_counter()-started)*1000,2))}
                return result
            outcome='timeout'
            raise RuntimeError('Linked MT5 response unknown; inspect receipt')
        except WorkerDispatchNotSent:
            outcome='not-sent'
            raise
        except (OSError, EOFError):
            self.phase = 'reconnecting'
            raise RuntimeError('Linked MT5 disconnected; inspect receipt')
        finally:
            elapsed=round((time.perf_counter()-started)*1000,3)
            with self.rpc_condition:
                self.rpc_counts['calls']+=1
                self.rpc_counts['notSent']+=int(outcome=='not-sent')
                self.rpc_counts['timeouts']+=int(outcome=='timeout')
                if elapsed>=250 or outcome!='ok':
                    blocker=self.rpc_owner['method'] if self.rpc_owner and not acquired else self.rpc_pending_method if not sent_to_worker else None
                    self.rpc_recent.append(dict(method=method,outcome=outcome,sent=sent_to_worker,
                        waitMs=lock_ms if acquired else elapsed,totalMs=elapsed,blockedBy=blocker,at=time.time()*1000))
            if acquired:self._rpc_release()

    def close(self):
        self.stop.set()
        self.supervisor.join(timeout=16)
        with self.lock:
            self.phase = 'stopped'
            self._dispose_locked()


class QuotePublisher:
    """One collector for all clients; a slow socket never owns collection.

    Only the latest complete frame is retained. Each client skips frames it
    could not send in time instead of building a queue of stale quotes.
    MT5 calls still use the same account validation and serialization lock.
    """
    def __init__(self, bridge):
        self.bridge=bridge
        self.condition=threading.Condition()
        self.subscribers=0
        self.sequence=0
        self.latest=None
        self.failed=False
        self.closed=False
        self.thread=None
        self.captures=0
        self.failures=0
        self.last_capture=None

    def subscribe(self):
        with self.condition:
            if self.closed: raise ValueError('Quote collector stopped')
            self.subscribers+=1
            sequence=self.sequence
            if self.thread is None:
                self.thread=threading.Thread(target=self.run,name='iscc-quotes',daemon=True)
                self.thread.start()
            self.condition.notify_all()
            return sequence

    def unsubscribe(self):
        with self.condition:
            self.subscribers-=1
            self.condition.notify_all()

    def next_frame(self, sequence):
        with self.condition:
            self.condition.wait_for(lambda:self.closed or self.sequence>sequence,timeout=1)
            if self.closed or self.failed: raise ValueError('Fresh MT5 quote unavailable')
            return self.sequence,self.latest if self.sequence>sequence else None

    def snapshot(self):
        with self.condition:
            return dict(schema=1,subscribers=self.subscribers,captures=self.captures,
                        failures=self.failures,lastCaptureAt=self.last_capture,
                        retainedFrames=int(self.latest is not None),backlogPolicy='latest-only')

    def run(self):
        due=None
        while True:
            with self.condition:
                while not self.closed and (not self.subscribers or due is not None and time.perf_counter()<due):
                    if not self.subscribers:
                        due=None
                        self.condition.wait()
                    else: self.condition.wait(timeout=max(0,due-time.perf_counter()))
                if self.closed: return
            started=time.perf_counter()
            if due is not None:
                self.bridge.read_timings.record('quoteProducerDelay',max(0,(started-due)*1000))
            interval=self.bridge.quote_interval()
            try:
                with (nullcontext() if getattr(self.bridge,'isolated_entries',False) else self.bridge.lock):
                    self.bridge.read_timings.record('quoteLockWait',(time.perf_counter()-started)*1000)
                    frame=self.bridge.read_timings.call('quoteCapture',self.bridge.quote_frame)
                interval=frame['intervalMs']/1000
                with self.bridge.read_timings.span('quoteEncode'):
                    body=json.dumps(frame,allow_nan=False).encode()+b'\n'
                self.bridge.quote_work+=time.perf_counter()-started
                self.bridge.frames+=1
                self.bridge.frame_bytes+=len(body)
                with self.condition:
                    self.latest=body
                    self.failed=False
                    self.captures+=1
                    self.last_capture=frame['capturedAt']
                    self.sequence+=1
                    self.condition.notify_all()
            except Exception:
                # Invalidate immediately; do not present the previous quote as
                # fresh after disconnection or an account/server mismatch.
                with self.condition:
                    self.latest=None
                    self.failed=True
                    self.failures+=1
                    self.sequence+=1
                    self.condition.notify_all()
            due=started+interval

    def close(self):
        with self.condition:
            self.closed=True
            self.condition.notify_all()
        if self.thread is not None: self.thread.join(timeout=5)


def create_server(bridge, address, *, tls_context=None, handshake_timeout=3.0,
                  max_handshakes=16, max_connections=128):
    if handshake_timeout <= 0 or max_handshakes < 1 or max_connections < max_handshakes:
        raise ValueError('Invalid receiver admission limits')
    config = bridge.config
    stream_slots = threading.BoundedSemaphore(2)
    level_sync = None
    publisher=QuotePublisher(bridge)
    bridge.quote_publisher=publisher

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def setup(self):
            self.request.settimeout(30)
            self.request.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            super().setup()

        def log_message(self, *_args):
            pass

        def handle(self):
            try:
                super().handle()
            except OSError:
                self.close_connection = True

        def stream_quotes(self):
            if not stream_slots.acquire(blocking=False):
                self.reply(503, {'error': 'Quote stream limit reached'})
                return
            subscribed=False
            try:
                sequence=publisher.subscribe()
                subscribed=True
                self.send_response(200)
                self.send_header('Content-Type', 'application/x-ndjson')
                self.send_header('Transfer-Encoding', 'chunked')
                self.send_header('Cache-Control', 'no-store')
                self.end_headers()
                while True:
                    sequence,body=publisher.next_frame(sequence)
                    if body is None: continue
                    with bridge.read_timings.span('quoteWrite'):
                        self.wfile.write(('%x\r\n' % len(body)).encode() + body + b'\r\n')
                        self.wfile.flush()
            except (OSError, ValueError, TypeError):
                self.close_connection = True
            finally:
                if subscribed: publisher.unsubscribe()
                stream_slots.release()

        def handle_request(self):
            if config.get('clientCertificateRequired', False):
                certificate = self.connection.getpeercert(binary_form=True)
                expected = config.get('clientCertificateSha256', '')
                if not certificate or not expected or not hmac.compare_digest(hashlib.sha256(certificate).hexdigest(),expected):
                    self.close_connection = True
                    self.reply(401, {'error':'Unauthorized client certificate'})
                    return
            if not hmac.compare_digest(self.headers.get('Authorization', ''), 'Bearer ' + config['token']):
                self.close_connection = True
                self.reply(401, {'error': 'Unauthorized'})
                return
            try:
                route = urlsplit(self.path)
                selected, route_path = bridge, route.path
                match = re.fullmatch(r'/accounts/([0-9]+)(/.*)', route_path)
                if match:
                    selected = bridge.linked.get(match[1])
                    if not selected: raise ValueError('Account is not configured')
                    route_path = match[2]
                if selected is bridge and level_sync and level_sync.handle(self, route):
                    return
                if self.command == 'GET' and route.path == '/quotes':
                    self.stream_quotes()
                    return
                payload = None
                operations = ('/place','/cancel','/close','/modify','/schedule-close','/cancel-close')
                if self.command == 'POST' and route_path in (*operations, '/quote-mode', '/account-discovery', '/account-register', '/entry-plan', '/entry-plan-admit', '/entry-plan-stop', '/entry-plan-continue', '/entry-plan-protection', '/entry-plan-close', '/candle-levels'):
                    length = int(self.headers.get('Content-Length', 0))
                    if not 0 < length <= 16384:
                        raise ValueError('Invalid body size')
                    payload = json.loads(self.rfile.read(length))
                received = (time.time()*1000, time.perf_counter())
                if self.command == 'POST' and route_path == '/modify' and isinstance(payload, dict):
                    selected_account = str(config['account']) if selected is bridge else match[1]
                    if bridge.chunk_entries.owned_entry(selected_account, payload.get('ticket')):
                        raise ValueError('Use whole-parent hybrid protection; individual child modification is blocked')
                if selected is bridge and route_path in ('/entry-events','/entry-plans','/entry-readiness','/entry-plan','/entry-plan-admit','/entry-plan-stop','/entry-plan-continue','/entry-plan-protection','/entry-plan-close'):
                    if self.command=='GET' and route_path=='/entry-events':
                        query = parse_qs(route.query, keep_blank_values=True)
                        if set(query) != {'since'} or len(query['since']) != 1 or not re.fullmatch(r'[0-9]{1,16}', query['since'][0]):
                            raise ValueError('A bounded entry event revision is required')
                        result = dict(account=str(config['account']), server=config['server'],
                            **bridge.chunk_entries.entry_events(int(query['since'][0]), timeout=10))
                    elif self.command=='POST' and route_path in ('/entry-plan-protection','/entry-plan-close'):
                        try:
                            method=bridge.chunk_entries.protection if route_path=='/entry-plan-protection' else bridge.chunk_entries.close_position
                            values=method(payload)
                        except (ValueError,TypeError,KeyError) as error:
                            values={'result':dict(status='rejected',reason=str(error),id=payload.get('id') if isinstance(payload,dict) else None,
                                entryId=payload.get('entryId') if isinstance(payload,dict) else None)}
                        with bridge.lock: result=bridge.envelope(**values)
                    elif self.command=='GET' and route_path=='/entry-readiness':
                        readiness=bridge.chunk_entries.account_readiness()
                        with bridge.lock: result=bridge.envelope(entryReadiness=readiness)
                    elif self.command=='GET' and route_path=='/entry-plans':
                        result=bridge.entry_plans()
                    elif self.command=='GET' and route_path=='/entry-plan':
                        query = parse_qs(route.query, keep_blank_values=True)
                        if set(query) != {'id'} or len(query['id']) != 1:
                            raise ValueError('A single exact entry plan ID is required')
                        record = bridge.chunk_entries.get(query['id'][0])
                        with bridge.lock: result = bridge.envelope(entry=record, version=BRIDGE_VERSION,
                            capabilities=bridge._capabilities())
                    elif self.command=='POST' and route_path in ('/entry-plan','/entry-plan-admit','/entry-plan-stop','/entry-plan-continue'):
                        # A checked rejection before admission is a confirmed
                        # outcome, distinct from a lost reply. Validate identity
                        # again outside this catch: if the envelope cannot be
                        # authenticated after a saved plan, the client must keep
                        # its result unknown rather than silently rearming.
                        try:
                            if route_path=='/entry-plan-admit':
                                required=payload.get('requiredCapabilities')
                                if (payload.get('admissionContract')!='atomic-entry-admission-v1'
                                        or not isinstance(required,list) or not required or len(required)>32
                                        or any(not isinstance(item,str) or item not in bridge._capabilities() for item in required)):
                                    raise ValueError('Unsupported atomic entry admission contract')
                                if not bridge.config.get('tradingEnabled',False):
                                    raise ValueError('Trading disabled; no entry admitted')
                            if route_path in ('/entry-plan','/entry-plan-admit') and isinstance(payload, dict) and payload.get('entryMode') in ('zone','boundary'):
                                raise ValueError('Update ISCC: market entry now requires an inclusive LT side zone')
                            if route_path=='/entry-plan-continue':
                                if not bridge.config.get('tradingEnabled',False):
                                    raise ValueError('Trading disabled; continuation blocked')
                                entry=bridge.chunk_entries.continue_account(payload)
                            else:
                                entry=bridge.chunk_entries.stop(payload) if route_path=='/entry-plan-stop' else bridge.chunk_entries.arm(payload,received)
                            values={'entry':entry}
                            if route_path=='/entry-plan-stop' and isinstance(payload.get('cancelContinueIds'),list):
                                values['entryControlReceipt'] = dict(schema='entry-control-recovery-v1',
                                    id=payload['id'],entryId=payload['entryId'],cancelContinueIds=payload['cancelContinueIds'])
                        except (ValueError,TypeError,KeyError) as error:
                            values={'entryResult':{'status':'rejected','reason':str(error)}}
                        with bridge.lock: result=bridge.envelope(**values, version=BRIDGE_VERSION,
                            capabilities=bridge._capabilities(), **({'admissionContract':'atomic-entry-admission-v1'} if route_path=='/entry-plan-admit' else {}))
                    else: raise ValueError('Unsupported chunk entry request')
                elif selected is bridge and route_path in ('/account-catalog','/account-discovery','/account-register'):
                    registry=getattr(bridge,'account_registry',None)
                    if registry is None: raise ValueError('Account enrollment requires receiver update')
                    if self.command=='GET' and route_path=='/account-catalog':
                        result=bridge.envelope(**registry.catalog())
                    elif self.command=='POST' and route_path=='/account-discovery' and payload=={}:
                        registry.scan()
                        result=bridge.envelope(**registry.catalog())
                    elif self.command=='POST' and route_path=='/account-register':
                        result=bridge.envelope(registration=registry.register(payload))
                    else: raise ValueError('Unsupported account enrollment request')
                elif self.command=='GET' and route_path=='/management':
                    watch=json.loads(parse_qs(route.query).get('watch',['[]'])[0])
                    if not isinstance(watch,list) or len(watch)>100 or any(not isinstance(x,dict) or set(x)!= {'account','ticket','symbol'}
                        or str(x['account']) not in {str(config['account']),*bridge.linked} or not re.fullmatch(r'[1-9][0-9]{0,19}',str(x['ticket']))
                        or x['symbol'] not in config['symbols'] for x in watch): raise ValueError('Invalid watched opening list')
                    identifiers = parse_qs(route.query).get('receipts',[''])[0].split(',')
                    identifiers = [x for x in identifiers if x]
                    if len(identifiers)>20 or any(not re.fullmatch(r'[A-Za-z0-9_.:-]{1,160}',x) for x in identifiers):
                        raise ValueError('Invalid receipt list')
                    if selected is bridge:
                        receipt_account = parse_qs(route.query).get('receiptAccount',[str(config['account'])])[0]
                        if receipt_account!=str(config['account']) and receipt_account not in bridge.linked:
                            raise ValueError('Receipt account is not configured')
                        result = bridge.management(identifiers,receipt_account,watch)
                    else:
                        result = selected.call('local_management')
                        result['receipts'] = {x:selected.call('receipt',x)['result'] for x in identifiers}
                elif selected is not bridge:
                    methods = {'/health':('health',()), '/diagnostics':('diagnostics',()), '/orders':('orders',()), '/close-rules':('close_rules',()),
                               '/snapshot':('snapshot',(parse_qs(route.query).get('symbol',[''])[0],)),
                               '/receipt':('receipt',(parse_qs(route.query).get('id',[''])[0],))}
                    if self.command=='GET' and route_path in methods:
                        method,args = methods[route_path]
                        result = selected.call(method,*args)
                    elif self.command=='POST' and route_path in operations:
                        result = selected.call('execute',route_path[1:],payload)
                    else: raise ValueError('Unsupported linked account route')
                elif self.command=='POST' and route_path in operations:
                    result = bridge.group_execute(route_path[1:],payload,received) if payload.get('mirrorAccount') or payload.get('mirrorAccounts') else bridge.execute(route_path[1:],payload,received)
                elif self.command=='GET' and route_path=='/receipt':
                    result = bridge.receipt(parse_qs(route.query).get('id',[''])[0])
                else:
                    methods = {'/health':('health',()), '/close-rules':('close_rules',()),
                               '/diagnostics':('diagnostics',()), '/snapshot':('snapshot',(parse_qs(route.query).get('symbol',[''])[0],)),
                               '/orders':('decorated_orders',()), '/candle-levels':('candle_levels',()),
                               '/candle-symbols':('candle_symbols',(parse_qs(route.query).get('names',[''])[0],)),
                               '/candle-snapshot':('candle_snapshot',(parse_qs(route.query).get('symbol',[''])[0],))}
                    with (nullcontext() if getattr(bridge,'isolated_entries',False) else bridge.lock):
                        if self.command=='GET' and route_path in methods:
                            method,args = methods[route_path]
                            result = getattr(bridge,method)(*args)
                        elif self.command=='POST' and route_path=='/candle-levels':
                            result = bridge.candle_levels(payload)
                        elif self.command=='POST' and route_path=='/quote-mode':
                            result = bridge.set_quote_mode(payload)
                        else:
                            self.close_connection = True
                            self.reply(404, {'error':'Unknown route'})
                            return
                self.reply(200, result)
            except (ValueError, TypeError, KeyError) as error:
                self.close_connection = True
                self.reply(400, {'error': str(error)})
            except OSError:
                self.close_connection = True
            except Exception:
                self.close_connection = True
                self.reply(503, {'error': 'Receiver unavailable; verify MT5'})

        def reply(self, status, value):
            body = json.dumps(value, allow_nan=False, separators=(',', ':') if self.command=='GET' else None).encode()
            compressed = False
            # Only an explicit read client opts in. Trading replies and the
            # streaming quote writer keep their original uncompressed framing.
            if self.command=='GET' and len(body)>=4096 and 'gzip' in [x.strip().lower() for x in self.headers.get('Accept-Encoding','').split(',')]:
                packed = gzip.compress(body, compresslevel=1, mtime=0)
                if len(packed)<len(body): body,compressed=packed,True
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            if compressed: self.send_header('Content-Encoding', 'gzip')
            if self.command=='GET': self.send_header('Vary', 'Accept-Encoding')
            self.end_headers()
            self.wfile.write(body)

        do_GET = handle_request
        do_POST = handle_request

    class ReceiverServer(ThreadingHTTPServer):
        # Accept raw TCP promptly. TLS never runs in serve_forever's accept loop.
        connection_slots = threading.BoundedSemaphore(max_connections)
        handshake_slots = threading.BoundedSemaphore(max_handshakes)

        def process_request(self, request, client_address):
            if not self.connection_slots.acquire(blocking=False):
                self.shutdown_request(request)
                return
            if tls_context is not None and not self.handshake_slots.acquire(blocking=False):
                self.connection_slots.release()
                self.shutdown_request(request)
                return
            try:
                super().process_request(request, client_address)
            except BaseException:
                if tls_context is not None: self.handshake_slots.release()
                self.connection_slots.release()
                raise

        def process_request_thread(self, request, client_address):
            connection = request
            try:
                if tls_context is not None:
                    try:
                        # SSLSocket.do_handshake applies this to the total handshake,
                        # including partial records, rather than each received byte.
                        request.settimeout(handshake_timeout)
                        connection = tls_context.wrap_socket(request, server_side=True,
                                                             do_handshake_on_connect=False)
                        connection.do_handshake()
                    finally:
                        self.handshake_slots.release()
                self.finish_request(connection, client_address)
            except OSError:
                # Silent/invalid TLS clients and normal disconnects need no traceback.
                pass
            except Exception:
                self.handle_error(connection, client_address)
            finally:
                try:
                    self.shutdown_request(connection)
                finally:
                    self.connection_slots.release()

        def server_close(self):
            bridge.chunk_entries.close()
            publisher.close()
            super().server_close()
    server=ReceiverServer(address, Handler)
    bridge.chunk_entries.start()
    return server


def server_tls_context(directory, config):
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(str(directory / 'server.crt'), str(directory / 'server.key'))
    if config.get('clientCertificateRequired', False):
        context.load_verify_locations(cafile=str(directory / 'client-ca.crt'))
        context.verify_mode = ssl.CERT_REQUIRED
        if not re.fullmatch(r'[a-f0-9]{64}',config.get('clientCertificateSha256','')):
            raise ValueError('Exact client certificate fingerprint is required')
    return context


class CoordinatorBridge(Bridge):
    """Both SDK connections live in supervised account processes.

    The parent retains group/control receipts and authenticated routing only.
    Worker recovery never closes terminals or replays an unknown mutation.
    """
    NATIVE_METHODS = frozenset(('entry_snapshot','entry_context','entry_admission','entry_positions','lot_specs',
        'execute','check_opening','entry_position_identity','entry_position_remainder','entry_common_stop','entry_common_stop_receipt',
        'entry_protection_plan','entry_limit_state','entry_cancel_limit','entry_management','entry_reconcile',
        'entry_confirm_reconciled','local_management','orders','close_rules'))

    def __init__(self, config, database, primary_worker):
        self.native_database=pathlib.Path(database)
        coordinator_database=self.native_database.with_name('entry-coordinator.sqlite3')
        self.primary_worker = primary_worker
        self.isolated_entries = True
        self.management_jobs = {}
        self.management_cache = {}
        self.management_queries = {}
        self.management_errors = {}
        self.candle_client = None
        self.candle_client_lock = threading.Lock()
        self.candle_worker_factory = AccountWorker
        self.candle_client_database = pathlib.Path(database).with_name('candle-client.sqlite3')
        self.quote_client = None
        self.quote_client_lock = threading.Lock()
        self.quote_worker_factory = AccountWorker
        self.quote_client_database = pathlib.Path(database).with_name('quote-client.sqlite3')
        self.quote_client_closed = False
        super().__init__(None,dict(config,_coordinatorOnly=True),coordinator_database)
        # This is authenticated routing metadata, never broker evidence.
        # Every selected account still proves its actual identity in admission
        # and again immediately before sending inside its own native worker.
        self.identity = SimpleNamespace(login=int(config['account']),server=config['server'],currency='USD')
        self.chunk_entries.close()
        self._migrate_coordinator_storage()
        from chunk_entry import IsolatedEntries
        self.chunk_entries = IsolatedEntries(self)

    def _migrate_coordinator_storage(self):
        self.db.execute('CREATE TABLE IF NOT EXISTS coordinator_storage_migrations (id TEXT PRIMARY KEY)')
        marker='native-coordinator-copy-v1'
        migrated=bool(self.db.execute('SELECT 1 FROM coordinator_storage_migrations WHERE id=?',(marker,)).fetchone())
        # Copy coordinator-owned receipts once, atomically. Never move, delete
        # or update a native request, rule, protective receipt or trade record.
        tables=('groups','chunk_entries','chunk_entry_stops','chunk_entry_cancels',
                'chunk_entry_protections','chunk_entry_closures')
        source=None
        try:
            if self.native_database.exists():
                source=sqlite3.connect(self.native_database.resolve().as_uri()+'?mode=ro',uri=True)
                source.execute('BEGIN')
                present={row[0] for row in source.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                for table in tables:
                    if table not in present:continue
                    columns=[row[1] for row in source.execute('PRAGMA table_info('+table+')')]
                    if columns!=[row[1] for row in self.db.execute('PRAGMA table_info('+table+')')]:
                        raise ValueError('Legacy coordinator ledger schema differs: '+table)
                    for row in source.execute('SELECT * FROM '+table):
                        old=self.db.execute('SELECT * FROM '+table+' WHERE '+columns[0]+'=?',(row[0],)).fetchone()
                        # A rollback can add new legacy IDs. Import those on
                        # the next upgrade too, but never replace a newer root
                        # projection; its account journals remain authoritative.
                        if old is not None and old!=row and not (migrated and table=='chunk_entries'):
                            raise ValueError('Coordinator migration receipt conflict')
                        if old is None:self.db.execute('INSERT INTO '+table+' VALUES ('+','.join('?' for _ in row)+')',row)
            self.db.execute('INSERT OR IGNORE INTO coordinator_storage_migrations VALUES (?)',(marker,));self.db.commit()
        except Exception:self.db.rollback();raise
        finally:
            if source is not None:source.close()

    def _request_receipt(self, identifier):
        return self.primary_worker.call('receipt',identifier)['result']

    def opening_group(self, ticket, symbol, rows=None):
        if rows is None:rows=self.orders()['orders']
        return Bridge.opening_group(self,ticket,symbol,rows)

    def _decorate_group_rows(self, rows):
        for row in rows:
            try:
                group,data,opened=self.opening_group(row['ticket'],row['symbol'],rows)
                accounts=[account for account in data['requests'] if account!=str(self.config['account'])]
                row.update(groupId=group,mirrorAccounts=accounts,mirrorStatus=opened['status'])
                if accounts:row['mirrorAccount']=accounts[0]
            except ValueError:pass

    def _candle_call(self, method, *args):
        # Chart history/SQLite/file work has a dedicated read-only SDK client.
        # It never occupies either account's execution RPC or runs trade rules.
        with self.candle_client_lock:
            if self.candle_client is None:
                self.candle_client=self.candle_worker_factory(dict(self.config,_candleOnly=True,
                    _coordinatorOnly=False,_primaryWorker=False,tradingEnabled=False,linkedAccounts=[]),self.candle_client_database)
            client=self.candle_client
        deadline=time.monotonic()+5
        while not client.available() and time.monotonic()<deadline:time.sleep(.02)
        if not client.available():raise ValueError('Read-only candle client is starting; retry chart request')
        token=self.entry_diagnostics.begin('chart:'+method,reason='Waiting for the separate read-only chart client')
        try:return client.call(method,*args)
        finally:self.entry_diagnostics.finish(token)

    def candle_levels(self, payload=None):
        return self._candle_call('candle_levels',payload)

    def candle_symbols(self, encoded_names):
        return self._candle_call('candle_symbols',encoded_names)

    def candle_snapshot(self, symbol):
        return self._candle_call('candle_snapshot',symbol)

    def _quote_call(self, method, *args):
        # Display reads own a separate SDK process and RPC pipe. Never fall
        # back to an execution worker, even while this client is recovering.
        with self.quote_client_lock:
            if self.quote_client_closed:raise ValueError('Read-only quote client is closed')
            if self.quote_client is None:
                self.quote_client=self.quote_worker_factory(dict(self.config,_quoteOnly=True,
                    _candleOnly=False,_coordinatorOnly=False,_primaryWorker=False,
                    tradingEnabled=False,linkedAccounts=[]),self.quote_client_database)
            client=self.quote_client
        deadline=time.monotonic()+5
        while not client.available() and time.monotonic()<deadline:time.sleep(.02)
        if not client.available():raise ValueError('Read-only quote client is starting or recovering')
        return client.call(method,*args)

    def snapshot(self, symbol):
        return self._quote_call('snapshot',symbol)

    def quote_frame(self):
        return self._quote_call('quote_frame')

    def close_group_executor(self):
        with self.quote_client_lock:
            self.quote_client_closed=True
            if self.quote_client is not None:
                self.quote_client.close();self.quote_client=None
        if self.candle_client is not None:
            self.candle_client.close();self.candle_client=None
        return super().close_group_executor()

    def __getattribute__(self, name):
        if name in object.__getattribute__(self,'NATIVE_METHODS'):
            worker = object.__getattribute__(self,'primary_worker')
            def dispatch(*args, **kwargs):
                if kwargs:
                    if name=='execute' and set(kwargs)=={'received'}:
                        args=(*args,kwargs['received'])
                    elif name=='local_management' and set(kwargs).issubset({'identifiers','watch'}):
                        args=(kwargs.get('identifiers',()),kwargs.get('watch',()))
                    else:
                        raise ValueError('Native worker requires positional arguments')
                return worker.call(name,*args)
            return dispatch
        return super().__getattribute__(name)

    def account(self, timing=None):
        # Routing identity does not assert connectivity or authorize trading.
        return self.identity

    def health(self):
        available=getattr(self.primary_worker,'available',None)
        if callable(available) and not available():
            native=dict(self._health(self.identity),connected=False,brokerIdentityVerified=False,
                        reason='Primary MT5 connector is starting or recovering')
        else:native = self.primary_worker.call('health')
        return dict(native,capabilities=self._capabilities(),linkedAccounts=list(self.linked),
                    accountReadiness={account:(worker.available() if callable(getattr(worker,'available',None)) else True)
                        for account,worker in {str(self.config['account']):self.primary_worker,**self.linked}.items()},
                    entryProgress=(self.chunk_entries.progress_observer.snapshot() if self.chunk_entries.progress_observer else None))

    def diagnostics(self):
        result=self.primary_worker.call('diagnostics')
        result['nativeEntryDiagnosticLog']=result.get('entryDiagnosticLog')
        result['entryDiagnosticLog']=self.entry_diagnostics.snapshot()
        publisher=getattr(self,'quote_publisher',None)
        uptime=max(time.perf_counter()-self.started,.001)
        result.update(quotePublisher=publisher.snapshot() if publisher else None,
            quoteReader=(dict(role='read-only-quotes',separateProcess=True,
                ready=self.quote_client.available(),rpc=self.quote_client.rpc_snapshot()) if self.quote_client else
                dict(role='read-only-quotes',separateProcess=True,ready=False,rpc=None)),
            coordinatorReadTimings=self.read_timings.snapshot(),quoteStream=dict(frames=self.frames,
                averageBytesPerFrame=round(self.frame_bytes/max(self.frames,1)),
                averageWorkMsPerFrame=round(self.quote_work/max(self.frames,1)*1000,2),
                averageBytesPerSecond=round(self.frame_bytes/uptime),intervalMs=round(self.quote_interval()*1000)),
            linkedWorkerRPC={account:worker.rpc_snapshot() for account,worker in
                {str(self.config['account']):self.primary_worker,**self.linked}.items() if callable(getattr(worker,'rpc_snapshot',None))})
        return result

    def management(self, identifiers=(), receipt_account=None, watch=()):
        # Native snapshots describe a worker, while this endpoint describes
        # the coordinator protocol. Never downgrade the Mac capability cache
        # to the worker's legacy entry engine after an ordinary UI refresh.
        return dict(super().management(identifiers,receipt_account,watch),
                    capabilities=self._capabilities(),linkedAccounts=list(self.linked),
                    entryProgress=self.chunk_entries.progress_observer.snapshot() if self.chunk_entries.progress_observer else None)

    def observe_native(self, account, value):
        # Reuse immutable normal native reads, without a new poll or writer in
        # the SDK process. The primary's existing passive statistics writer is
        # owned by this coordinator; linked workers retain their own observer.
        observer=self.chunk_entries.progress_observer
        if observer is None or not isinstance(value,dict):
            return
        proof=value.get('proof') if 'snapshot' in value else value
        if isinstance(proof,dict) and isinstance(proof.get('orders'),list):
            observer.offer(account,proof['orders'],proof.get('checkedAt',time.time()*1000))

    def set_quote_mode(self, payload, now=None):
        result = super().set_quote_mode(payload,now)
        self._quote_call('set_quote_mode',payload)
        return result

    def start_rules(self):
        # Rules and their recovery run exactly once, inside each native worker.
        # Only the passive, lower-priority observer belongs to this parent.
        self.entry_diagnostics.start()
        from chunk_entry import EntryProgressObserver
        if self.chunk_entries.progress_observer is None:
            self.chunk_entries.progress_observer = EntryProgressObserver(self.config['account'],self.config['server'],
                self.entry_progress_file,self.chunk_entries.list)
            self.chunk_entries.progress_observer.start()

    def management_accounts(self, identifiers=(), receipt_account=None, watch=()):
        # UI snapshots never feed native execution or volume reservations.
        workers = {str(self.config['account']):self.primary_worker,**self.linked}
        executor = self.warm_group_executor()
        signatures = {}
        with self.lock:
            for account,worker in workers.items():
                selected = [row for row in watch if str(row['account'])==account]
                wanted = list(identifiers) if receipt_account==account else []
                signature = json.dumps([wanted,selected],sort_keys=True)
                signatures[account] = signature
                old = self.management_jobs.get(account)
                if old and old[1].done():
                    self._cache_management(account,*old)
                    self.management_jobs.pop(account,None)
                if account not in self.management_jobs:
                    self.management_jobs[account] = (signature,executor.submit(self._read_management,account,worker,wanted,selected))
            jobs = {account:row[1] for account,row in self.management_jobs.items()}
        # Healthy accounts publish without waiting for a peer's native timeout.
        from concurrent.futures import wait
        wait(list(jobs.values()),timeout=.02)
        states = {}
        with self.lock:
            for account,job in jobs.items():
                current = self.management_jobs.get(account)
                if current and current[1] is job and job.done():
                    self._cache_management(account,*current)
                    self.management_jobs.pop(account,None)
                exact=self.management_queries.get(account,{}).get(signatures[account])
                cached=self.management_cache.get(account)
                ready=bool(exact and 0<=time.monotonic()-exact[1]<=2.5)
                chosen=exact if ready else cached
                connected=bool(chosen and 0<=time.monotonic()-chosen[1]<=2.5)
                age=round((time.monotonic()-chosen[1])*1000,3) if chosen else None
                view=copy.deepcopy(chosen[2]) if chosen else dict(account=account,server=self.config['server'],orders=[])
                failed=account in self.management_errors
                states[account]=dict(view,managementEpoch=self.management_epoch,
                    connected=True if connected else False if failed else None,evidenceReady=ready,
                    connectionState='connected' if connected else 'unavailable' if failed else 'checking',snapshotAgeMs=age,
                    **({} if ready else dict(reason='Терминал недоступен' if failed else 'Проверяем состояние сделки',openingStates={},receipts={})))
        return states

    def _read_management(self, account, worker, wanted, selected):
        value=worker.call('management_snapshot',wanted,selected)
        # The native RPC and its account mutex have been released already.
        # Copy raw metadata briefly; JSON parsing and UI decoration hold no
        # native queue or coordinator lock.
        groups=self.management_groups({account:value}, reverse=True)
        return format_management_snapshot(value,account,groups)

    def _cache_management(self, account, signature, job):
        try:
            value=job.result()
            if str(value.get('account'))!=account or not isinstance(value.get('orders'),list):
                raise ValueError('Management account identity mismatch')
            self.observe_native(account,value)
            checked=value.get('checkedAt',time.time()*1000)
            if type(checked) not in (int,float) or not math.isfinite(checked) or checked-time.time()*1000>250:
                raise ValueError('Invalid native management timestamp')
            age=max(0.,(time.time()*1000-checked)/1000)
            cached=(signature,time.monotonic()-age,copy.deepcopy(value))
            self.management_cache[account]=cached
            self.management_errors.pop(account,None)
            queries=self.management_queries.setdefault(account,{})
            queries.pop(signature,None);queries[signature]=cached
            while len(queries)>8:queries.pop(next(iter(queries)))
        except Exception as error:
            self.management_cache.pop(account,None)
            self.management_queries.pop(account,None)
            self.management_errors[account]=type(error).__name__


def start_account_workers(config, file, factory=None):
    factory=AccountWorker if factory is None else factory
    linked=config.get('linkedAccounts',[])
    accounts={str(config['account'])};paths={pathlib.Path(config['terminalPath']).resolve()}
    for row in linked:
        account=str(row['account']);path=pathlib.Path(row['terminalPath']).resolve()
        if account in accounts or path in paths:raise ValueError('Linked account requires a separate terminal')
        accounts.add(account);paths.add(path)
    created=[]
    try:
        primary=factory({**config,'linkedAccounts':[],'_primaryWorker':True},file.with_name('requests.sqlite3'))
        created.append(primary);workers={}
        for row in linked:
            account=str(row['account'])
            workers[account]=factory({**config,**row,'linkedAccounts':[]},file.with_name('requests-'+account+'.sqlite3'))
            created.append(workers[account])
        return primary,workers
    except Exception:
        for worker in created:worker.close()
        raise


def serve(config_file):
    file = pathlib.Path(config_file).resolve()
    config = json.loads(file.read_text(encoding='utf-8'))
    if len(config.get('token', '')) < 32:
        raise ValueError('A strong receiver token is required')
    primary_worker,workers=start_account_workers(config,file)
    try:bridge = CoordinatorBridge(config,file.with_name('requests.sqlite3'),primary_worker)
    except Exception:
        for worker in [primary_worker,*workers.values()]:worker.close()
        raise
    bridge.linked.update(workers)
    from account_registry import AccountRegistry
    def registered_worker(row, wait=True):
        worker=AccountWorker({**config,**{key:row[key] for key in ('account','server','terminalPath')},'linkedAccounts':[]},
                             file.with_name('requests-'+row['account']+'.sqlite3'))
        if wait:
            deadline=time.monotonic()+5
            while not worker.available() and time.monotonic()<deadline: time.sleep(.05)
            if not worker.available():
                worker.close()
                raise ValueError('New MT5 worker is not ready; account was not added')
        return worker
    bridge.account_registry=AccountRegistry(bridge,file.with_name('accounts-registry.json'),registered_worker)
    bridge.account_registry.restore()
    bridge.warm_group_executor()
    bridge.start_rules()

    level_sync = None
    context = server_tls_context(file.parent,config)
    server = create_server(bridge, (config.get('host', '127.0.0.1'), int(config.get('port', 9443))),
                           tls_context=context)
    print('MT5 receiver ready; trading enabled:', config.get('tradingEnabled', False), flush=True)
    mobile = None
    try:
        pass
        server.serve_forever()
    finally:
        pass
        server.server_close()
        bridge.rule_stop.set()
        bridge.close_group_executor()
        if bridge.candle_book is not None: bridge.candle_book.close()
        for worker in bridge.linked.values(): worker.close()
        primary_worker.close()


@contextmanager
def process_lock(filename):
    """OS-owned lock: process death releases it; stale files are harmless."""
    stream=open(filename,'a+b')
    acquired=False
    try:
        stream.seek(0)
        if not stream.read(1):
            stream.write(b'1'); stream.flush()
        stream.seek(0)
        try:
            if os.name=='nt':
                import msvcrt
                msvcrt.locking(stream.fileno(),msvcrt.LK_NBLCK,1)
            else:
                import fcntl
                fcntl.flock(stream,fcntl.LOCK_EX|fcntl.LOCK_NB)
            acquired=True
        except OSError:
            pass
        yield acquired
    finally:
        if acquired:
            if os.name=='nt':
                stream.seek(0); msvcrt.locking(stream.fileno(),msvcrt.LK_UNLCK,1)
            else: fcntl.flock(stream,fcntl.LOCK_UN)
        stream.close()


def supervise(config_file, command=None, stop=None, retry_seconds=RECEIVER_RETRY_SECONDS):
    """Restart a terminated receiver only. Never replay orders or kill a live one."""
    config=pathlib.Path(config_file).resolve()
    stop=stop or threading.Event()
    command=command or [sys.executable,str(pathlib.Path(__file__).resolve()),str(config),'--receiver']
    state_file=config.with_name('supervisor-state.json')
    def record(**values):
        temporary=state_file.with_suffix('.tmp')
        temporary.write_text(json.dumps(dict(at=time.time()*1000,**values)),encoding='utf-8')
        os.replace(temporary,state_file)
    with process_lock(config.with_name('supervisor.lock')) as acquired:
        if not acquired: return
        failures=0; restarts=0; child=None
        try:
            while not stop.is_set():
                started=time.monotonic()
                try:
                    child=subprocess.Popen(command,cwd=config.parent)
                except OSError:
                    failures+=1; delay=min(30,retry_seconds*2**min(failures-1,5))
                    record(state='restarting',restarts=restarts,retrySeconds=delay,reason='Receiver process could not start')
                else:
                    record(state='running',pid=child.pid,restarts=restarts)
                    while child.poll() is None and not stop.wait(.25): pass
                    if stop.is_set(): break
                    code=child.returncode
                    if time.monotonic()-started>=60: failures=0
                    failures+=1
                    delay=min(30,retry_seconds*2**min(failures-1,5))
                    record(state='restarting',exitCode=code,restarts=restarts,retrySeconds=delay)
                restarts+=1
                if stop.wait(delay): break
        finally:
            if child and child.poll() is None:
                child.terminate()
                try: child.wait(timeout=10)
                except subprocess.TimeoutExpired: child.kill(); child.wait()
            record(state='stopped',restarts=restarts)


if __name__ == '__main__':
    config_file=sys.argv[1] if len(sys.argv)>1 else 'bridge-config.json'
    if '--receiver' in sys.argv[2:]:
        with process_lock(pathlib.Path(config_file).resolve().with_name('receiver.lock')) as acquired:
            if acquired: serve(config_file)
    else:
        supervise(config_file)

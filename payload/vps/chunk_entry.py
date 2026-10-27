"""Durable, sequential breakout entries. This module never retries a submission.

Only the coordinator owns this service. Account identities, journalled child
submissions and final price guards remain the responsibility of Bridge.
"""
import copy
import hashlib
import json
import math
import os
import pathlib
import queue
import re
import sqlite3
from types import SimpleNamespace
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from collections import deque
from contextlib import contextmanager
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING
from entry_wait_totals import EntryWaitTotals, record_common_stop_timing


ACTIVE = frozenset(('armed', 'running', 'paused', 'sending'))
DEADLINE_FIELDS = ('closeAtUtc', 'closeTimeZone', 'closeLocalDate',
                   'closeLocalTime', 'cancelPendingAtDeadline')
PUBLIC_FIELDS = ('id', 'symbol', 'side', 'phase', 'trigger', 'volume', 'chunkVolume', 'chunkPercent',
                 'maxGap', 'riskUsd', 'sl', 'tp', 'level', 'closeAtUtc', 'nextChunk',
                 'childCount', 'createdAt', 'updatedAt', 'tickSize', 'stopMode', 'stopOffsetPts', 'fillPriority',
                 'entryMode', 'zoneRadius', 'zoneMultiplier', 'executionPolicy', 'protectionPending', 'protectionPause', 'closurePending', 'positionsClosed',
                 'hasOpenPositions', 'positionsConfirmed', 'positionLifecycle', 'cancelledBeforeAdmission', 'quoteWait', 'reconciliation')
PUBLIC_ACCOUNT_FIELDS = ('multiplier', 'targetVolume', 'filledVolume', 'anchor', 'riskUsd', 'riskUsed', 'averagePrice', 'sl', 'tp', 'protectionOverride', 'entryStatus', 'entryReason', 'lastEntryCheckAt', 'entryTiming', 'userStopped', 'laneStopReason', 'stopCheckRejected', 'commonStopUncertain', 'riskRepairRequired', 'limitRetryAvailable', 'admissionWaiting')
PUBLIC_RESULT_FIELDS = ('status', 'providerOrderId', 'filledVolume', 'fillPrice', 'primaryAccount')
PUBLIC_LIST_BUDGET = 512 * 1024
HYBRID_MAX_MARKET_PARTS = 100
HYBRID_MAX_POSITIONS = 200
HYBRID_MAX_SUBMISSIONS = 512
HYBRID_ROUNDS_PER_TICK = 25
HYBRID_STOP_QUOTE_WAIT = 'Further entry stopped; waiting for a fresh broker quote to confirm the common stop'
ENTRY_STOP_PRIORITY_CAPABILITY = 'entry-stop-priority-v1'


class EntryControlBarrier:
    """Exact continuation revocations; never queried by the acquisition loop."""
    def __init__(self, storage):
        self.storage = storage
        self.condition = threading.Condition()
        self.running = {}
        self.running_accounts = {}
        with storage.lock:
            storage.db.execute('CREATE TABLE IF NOT EXISTS chunk_entry_continue_cancels '
                               '(id TEXT PRIMARY KEY, entry_id TEXT NOT NULL, stop_id TEXT NOT NULL)')
            self.revoked = dict(storage.db.execute('SELECT id,entry_id FROM chunk_entry_continue_cancels'))
            storage.db.commit()

    def check(self, request):
        with self.condition:
            if request.get('id') in self.revoked:
                raise ValueError('Continuation revoked by a later explicit stop')

    @contextmanager
    def continuing(self, request):
        entry_id, identifier = request.get('entryId'), request.get('id')
        if (not isinstance(entry_id, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,100}', entry_id)
                or not isinstance(identifier, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,130}', identifier)):
            raise ValueError('Invalid continuation command identity')
        with self.condition:
            self.check(request)
            self.running[entry_id] = self.running.get(entry_id, 0) + 1
            key = (entry_id, request.get('account'))
            self.running_accounts[key] = self.running_accounts.get(key, 0) + 1
        try:
            yield
        finally:
            with self.condition:
                remaining = self.running[entry_id] - 1
                if remaining: self.running[entry_id] = remaining
                else: self.running.pop(entry_id)
                count = self.running_accounts[key] - 1
                if count: self.running_accounts[key] = count
                else: self.running_accounts.pop(key)
                self.condition.notify_all()

    def revoke(self, request):
        identifiers = request.get('cancelContinueIds', [])
        if (not isinstance(identifiers, list) or len(identifiers) > 2048
                or len(set(x for x in identifiers if isinstance(x, str))) != len(identifiers)
                or any(not isinstance(x, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,130}', x) for x in identifiers)):
            raise ValueError('Invalid continuation revocations')
        entry_id, stop_id = request['entryId'], request['id']
        # Persist before waiting for an MT5 read or a continuation reply. A
        # late request remains revoked after a receiver restart as well.
        with self.condition:
            if any(x in self.revoked and self.revoked[x] != entry_id for x in identifiers):
                raise ValueError('Continuation revocation belongs to another entry')
            additions = [x for x in identifiers if x not in self.revoked]
            if additions:
                with self.storage.lock:
                    try:
                        self.storage.db.executemany('INSERT INTO chunk_entry_continue_cancels VALUES (?,?,?)',
                            [(x, entry_id, stop_id) for x in additions])
                        self.storage.db.commit()
                    except Exception:
                        self.storage.db.rollback()
                        raise
                self.revoked.update((x, entry_id) for x in additions)

    def drain(self, entry_id, account=None):
        with self.condition:
            self.condition.wait_for(lambda: not (self.running.get(entry_id) if account is None
                else self.running_accounts.get((entry_id, account))))

def independent_accounts(payload):
    return payload.get('accountExecution') in ('independent-v2', 'independent-v3', 'independent-v4', 'independent-v5')

def parallel_accounts(payload):
    return payload.get('accountExecution') in ('independent-v3', 'independent-v4', 'independent-v5')

def pending_accounts(child):
    if child.get('state') not in ('pending', 'sending', 'unknown'):
        return set()
    return {account for account in child.get('volumes', {})
            if not child.get('settledAccounts', {}).get(account)
            and child.get('result', {}).get('accountResults', {}).get(account, {}).get('status') != 'rejected'}

def fill_outside_zone(payload, price, lower, upper):
    # Broker price improvement is acceptable only in the new, captured policy.
    # Fresh request prices, stops, risk and ownership still have their own guards.
    if independent_accounts(payload):
        return price > upper+1e-8 if payload['side'] == 'buy' else price < lower-1e-8
    return price < lower-1e-8 or price > upper+1e-8


class QuoteWait(ValueError):
    """A validated price is old; no uncertain native action is implied."""
    def __init__(self, account, data, stage='runtime'):
        super().__init__('Broker quote is stale; entry is paused')
        quote = data.get('quote', {})
        checked = time.time() * 1000
        tick = number(quote.get('utcTime'), 'Quote UTC time') * 1000
        self.details = dict(status='waiting', stage=stage, account=str(account),
                            ageMs=round(checked-tick, 3), tickUtcMs=tick, checkedAtMs=checked)
        for key in ('nativeCapturedAt', 'nativeAgeMs'):
            value = quote.get(key)
            if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value):
                self.details[key] = value


class StopCheckRejected(ValueError):
    """Native proof that no stop modification was started."""


class CommonStopWait(ValueError):
    """This account waits for an exact receipt; sent mutations are not replayed."""


def hybrid_receipt(value):
    """Copy native proof without duplicating diagnostics in every parent save.

    The authoritative Bridge requests/groups journals retain full timings.
    Preserve every other field, including unknown native proof extensions and
    all account receipts used to prove a settled partial pending fill.
    """
    if not isinstance(value, dict):
        return copy.deepcopy(value)
    compact = {}
    for key, item in value.items():
        if key in ('timings', 'serverTiming', 'rpcTiming'):
            continue
        if key == 'accountResults' and isinstance(item, dict):
            compact[key] = {account: hybrid_receipt(outcome) for account, outcome in item.items()}
        else:
            compact[key] = copy.deepcopy(item)
    return compact


def public_record(state):
    """Keep ledger details out of frequently polled authenticated responses."""
    result = {key: copy.deepcopy(state[key]) for key in PUBLIC_FIELDS if key in state}
    result['reason'] = str(state.get('reason', ''))[:512]
    if state.get('accountExecution') in ('independent-v4', 'independent-v5'):
        result['controlContract'] = ENTRY_STOP_PRIORITY_CAPABILITY
    result['accounts'] = {account: {key: details[key] for key in PUBLIC_ACCOUNT_FIELDS if key in details}
                          for account, details in state.get('accounts', {}).items()}
    if independent_accounts(state):
        result['accountExecution'] = state['accountExecution']
        if state.get('accountExecution') in ('independent-v4','independent-v5'):
            result['lanePhases'] = copy.deepcopy(state.get('lanePhases', {}))
        for account, details in result['accounts'].items():
            reserved = sum(max(0., child['volumes'].get(account, 0.)-child.get('positions', {}).get(account, {}).get('volume', 0.))
                           for child in state.get('children', []) if child.get('type') == 'limit'
                           and child.get('state') in ('pending', 'sending', 'unknown')
                           and account in pending_accounts(child)
                           and child.get('result', {}).get('accountResults', {}).get(account, {}).get('status') != 'rejected')
            details['pendingVolume'] = rounded(reserved)
            details['freeVolume'] = rounded(max(0., details['targetVolume']-details['filledVolume']-reserved))
    def receipt(value):
        compact = {key: value[key] for key in PUBLIC_RESULT_FIELDS if key in value}
        if value.get('reason'):
            compact['reason'] = str(value['reason'])[:160]
        return compact
    children = []
    captured = state.get('children', [])
    if state.get('executionPolicy') == 'hybrid-market-limit-v1':
        maximum = HYBRID_MAX_SUBMISSIONS*len(state.get('accounts', {})) if state.get('accountExecution') in ('independent-v4','independent-v5') else HYBRID_MAX_SUBMISSIONS
        if len(captured) > maximum:
            raise ValueError('Hybrid entry exceeds its bounded submission ledger')
    else:
        captured = captured[:50]
    for child in captured:
        entry = {key: copy.deepcopy(child[key]) for key in ('id', 'index', 'state', 'type', 'price', 'volumes', 'fillGuardViolated', 'closedOpenings', 'priceEvidence') if key in child}
        entry['positions'] = {}
        for account, row in child.get('positions', {}).items():
            position = {key: row[key] for key in ('ticket', 'openingTicket', 'volume', 'fillPrice') if key in row}
            if position.get('openingTicket') == position.get('ticket'):
                position.pop('openingTicket', None)
            entry['positions'][account] = position
        if isinstance(child.get('result'), dict):
            confirmed = child.get('state') == 'confirmed' and child['result'].get('status') == 'ok'
            if confirmed:
                # Every confirmed account's actual price, volume and opening
                # identifiers already live in positions; do not repeat them.
                entry['result'] = {key: child['result'][key] for key in ('status', 'primaryAccount') if key in child['result']}
            else:
                entry['result'] = receipt(child['result'])
            if not confirmed and isinstance(child['result'].get('accountResults'), dict):
                entry['result']['accountResults'] = {account: receipt(value) for account, value in child['result']['accountResults'].items()}
        children.append(entry)
    result['children'] = children
    return result


def number(value, name, positive=True):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(name + ' must be a finite number')
    if positive and value <= 0:
        raise ValueError(name + ' must be positive')
    return float(value)


def rounded(value):
    return float(Decimal(str(value)).quantize(Decimal('0.00000001')))


def entry_stop_distance(payload):
    if payload.get('entryMode') in ('zone', 'boundary', 'side-zone') and payload.get('stopMode') == 'entry':
        return number(payload.get('stopOffsetPts'), 'Entry stop points') * number(payload.get('tickSize'), 'Entry stop tick')
    return None


def weighted_nasdaq_stop(payload, details=None):
    """Use the captured panel distance from the actual index lot average."""
    if (payload.get('symbol') not in ('USTEC','SPX500') or payload.get('accountExecution') not in ('independent-v4','independent-v5')
            or payload.get('executionPolicy') != 'hybrid-market-limit-v1'
            or 'sl' in (details or {}).get('protectionOverride', {})):
        return False
    distance = entry_stop_distance(payload)
    return distance is not None and math.isfinite(distance) and distance > 0


def fill_priority(payload):
    """Only explicitly captured new plans opt in; never migrate live parents."""
    if (payload.get('accountExecution') in ('independent-v4','independent-v5')
            and payload.get('executionPolicy') == 'hybrid-market-limit-v1'):
        return 'volume' if payload.get('initialSizingOnly') is True or payload.get('accountExecution')=='independent-v5' else payload.get('fillPriority')
    return None


def market_price_bounds(payload):
    trigger = payload['trigger']
    if payload.get('entryMode') == 'side-zone':
        return (trigger, trigger + payload['zoneRadius']) if payload['side'] == 'buy' else (trigger - payload['zoneRadius'], trigger)
    if payload.get('entryMode') == 'boundary':
        return (trigger, trigger + payload['maxGap']) if payload['side'] == 'buy' else (trigger - payload['maxGap'], trigger)
    return trigger - payload['zoneRadius'], trigger + payload['zoneRadius']


def field(value, key, default=None):
    return value.get(key, default) if isinstance(value, dict) else getattr(value, key, default)


def digest(payload):
    core = {key: value for key, value in payload.items() if key != 'expiresAt'}
    return hashlib.sha256(json.dumps(core, sort_keys=True, allow_nan=False).encode()).hexdigest()


def valid_units(value, step, name):
    units = Decimal(str(value)) / Decimal(str(step))
    integer = units.to_integral_value()
    if abs(units - integer) > Decimal('0.0000001'):
        raise ValueError(name + ' does not match the lot step')
    return int(integer)


def tranche_plan(total, chunk, accounts, max_tranches=50):
    """Allocate rounded account totals over the SAME number of valid tranches."""
    primary = next(iter(accounts))
    spec = accounts[primary]['spec']
    units = valid_units(total, spec['quantityStep'], 'Total volume')
    cap = valid_units(chunk, spec['quantityStep'], 'Chunk volume')
    minimum = valid_units(spec['minQty'], spec['quantityStep'], 'Minimum lot')
    maximum = valid_units(spec['maxQty'], spec['quantityStep'], 'Maximum lot')
    if units < minimum or not minimum <= cap <= maximum:
        raise ValueError('Total or chunk volume is outside broker lot limits')
    count = (units + cap - 1) // cap
    if not 1 <= count <= max_tranches:
        raise ValueError('Chunk entry permits at most ' + str(max_tranches) + ' tranches')
    primary_units = [cap] * (count - 1) + [units - cap * (count - 1)]
    if primary_units[-1] < minimum:
        deficit = minimum - primary_units[-1]
        for index in range(count - 2, -1, -1):
            take = min(deficit, primary_units[index] - minimum)
            primary_units[index] -= take
            primary_units[-1] += take
            deficit -= take
        if deficit:
            raise ValueError('The last tranche is smaller than the minimum lot')
    plans = {}
    for account, details in accounts.items():
        spec = details['spec']; step = Decimal(str(spec['quantityStep']))
        target = int((Decimal(str(total)) * Decimal(str(details['multiplier'])) / step
                      + Decimal('1e-12')).to_integral_value(rounding=ROUND_FLOOR))
        lo = valid_units(spec['minQty'], spec['quantityStep'], 'Minimum lot')
        hi = valid_units(spec['maxQty'], spec['quantityStep'], 'Maximum lot')
        if target < count * lo or target > count * hi:
            raise ValueError('Account ' + account + ': coefficient and lot step cannot provide a valid lot in every tranche; increase chunk volume')
        cumulative = 0; previous = 0; parts = []
        for part in primary_units:
            cumulative += part
            current = target * cumulative // units
            parts.append(max(lo, min(hi, current - previous)))
            previous = current
        difference = target - sum(parts)
        for index in range(count - 1, -1, -1):
            change = min(difference, hi - parts[index]) if difference > 0 else -min(-difference, parts[index] - lo)
            parts[index] += change
            difference -= change
        if difference:
            raise ValueError('Account ' + account + ': cannot allocate valid tranche volumes')
        plans[account] = [float(step * part) for part in parts]
    return plans


class EntryProgressObserver:
    """Compatibility hook: personal movement statistics are not distributed."""
    active = False
    def __init__(self, *args, **kwargs): pass
    def start(self): pass
    def offer(self, *args, **kwargs): pass
    def decorate(self, entry): return entry
    def snapshot(self): return None
    def close(self): pass


class DiagnosticBuffer:
    """Bounded transfer without waking a disk thread for every trade stage."""
    def __init__(self, capacity):
        self.items=deque()
        self.capacity=capacity
        self.lock=threading.Lock()

    def put_nowait(self, row):
        if not self.lock.acquire(blocking=False):raise queue.Full
        try:
            if len(self.items)>=self.capacity:raise queue.Full
            self.items.append(row)
        finally:self.lock.release()

    def take(self, maximum):
        if not self.lock.acquire(blocking=False):return []
        try:return [self.items.popleft() for _ in range(min(maximum,len(self.items)))]
        finally:self.lock.release()

    def qsize(self):return len(self.items)
    def empty(self):return not self.items


class EntryDiagnostics:
    """Best-effort diagnostics only: bounded nonblocking producers, one writer.

    No trading payloads, native reads, durable trade claims or fsyncs enter
    this queue. Losing a diagnostic row can never revoke/permit an order.
    """
    NUMBERS = frozenset(('readWaitMs','readQueueMs','readWorkMs','readResumeMs','quoteAgeMs','contextMs','workerLockWaitMs',
        'workerRpcMs','retcode','lockWaitMs','nativeMs','totalMs','heldMs','repeats',
        'positionCount','changedPositions','mt5Ms','ledgerBeforeMs','ledgerAfterMs'))

    def __init__(self, path, account=None, capacity=512, max_bytes=2*1024*1024):
        self.path, self.account = pathlib.Path(path), str(account) if account is not None else None
        self.queue = DiagnosticBuffer(capacity)
        self.max_bytes = max_bytes
        self.lock = threading.Lock()
        self.stopping = threading.Event()
        self.thread = None
        self.active = False
        self.spans, self.gates = {}, {}
        self.serial = self.written = self.dropped = 0
        self.last_command_at=0.
        self.write_error = None
        self.priority = 'inactive'

    @staticmethod
    def raw(value, maximum=2048):
        try:
            return str(value)[:maximum]
        except Exception:
            return '[unprintable]'

    @staticmethod
    def text(value, maximum=384):
        value=EntryDiagnostics.raw(value)
        lower=value.lower()
        if not any(word in lower for word in ('private key','bearer','token','password','secret','authorization','api','client','http')):
            return value[:maximum]
        if 'PRIVATE KEY' in value:
            return '[sensitive details omitted]'
        value = re.sub(r'(?i)\bbearer\s+[^\s,;]+', 'Bearer [redacted]', value)
        value = re.sub(r'''(?i)(\b(?:token|password|secret|authorization|api[_-]?key|client[_-]?key)\b["'\s]*[:=]\s*)(?:"[^"]*(?:"|$)|'[^']*(?:'|$)|[^\s,;}]+)''',r'\1[redacted]',value)
        value = re.sub(r'(https?://)[^\s/@]+:[^\s/@]+@',r'\1[redacted]@',value)
        value = re.sub(r'(https?://[^\s?]+)\?[^\s]+',r'\1?[redacted]',value)
        return value[:maximum]

    def start(self):
        if self.thread is not None:
            return
        self.active = True
        self.thread = threading.Thread(target=self.run,name='iscc-entry-diagnostics',daemon=True)
        try:
            self.thread.start()
            self.emit('logger-started',stage='service',reason='Asynchronous diagnostic logging enabled')
        except Exception as error:
            self.write_error=type(error).__name__+': '+self.text(error,192)
            self.active=False
            self.thread=None

    def emit(self, event, *, account=None, entry_id=None, symbol=None, stage=None,
             reason='', duration_ms=0., error=None, level='info', **metrics):
        if not self.active:
            return
        try:
            row = dict(schema=1, atMs=round(time.time()*1000,3), event=self.raw(event,64),
                account=self.raw(account if account is not None else self.account,32),
                entryId=self.raw(entry_id,130) if entry_id is not None else None,
                symbol=self.raw(symbol,32) if symbol is not None else None,
                stage=self.raw(stage,96), reason=self.raw(reason), level=level,
                durationMs=round(max(0.,float(duration_ms)),3))
            if error is not None:
                row.update(errorClass=type(error).__name__,reason=self.raw(error),level='error')
                frame=getattr(error,'__traceback__',None)
                while frame is not None and frame.tb_next is not None:frame=frame.tb_next
                if frame is not None:row['errorAt']=frame.tb_frame.f_code.co_name+':'+str(frame.tb_lineno)
            row.update({key:round(float(value),3) for key,value in metrics.items() if key in self.NUMBERS
                and type(value) in (int,float) and math.isfinite(value)})
            if metrics.get('blockedBy') is not None:row['blockedBy']=self.raw(metrics['blockedBy'],64)
            if error is not None:
                if not self.lock.acquire(blocking=False):
                    self.dropped += 1
                    return
                try:
                    key=(row['account'],row['entryId'],row['stage'],row['errorClass'],row['reason'])
                    previous=self.gates.get(key)
                    now=time.monotonic()
                    if previous and now-previous[0]<5:
                        previous[1]+=1
                        return
                    if previous and previous[1]:row['repeats']=previous[1]
                    if len(self.gates)>=256:self.gates.pop(next(iter(self.gates)))
                    self.gates[key]=[now,0]
                finally:
                    self.lock.release()
            self.queue.put_nowait(row)
        except Exception:
            self.dropped += 1

    def begin(self, stage, *, account=None, entry_id=None, symbol=None, reason='', announce=False):
        if not self.active or not self.lock.acquire(blocking=False):
            return None
        try:
            if len(self.spans)>=128:
                self.dropped += 1
                return None
            self.serial+=1
            token=self.serial
            self.spans[token]=dict(stage=stage,account=account,entry_id=entry_id,symbol=symbol,
                reason=reason,started=time.monotonic(),lastWait=0.,announce=announce)
            if announce:self.last_command_at=time.monotonic()
        finally:
            self.lock.release()
        if announce:self.emit('call-started',stage=stage,account=account,entry_id=entry_id,symbol=symbol,reason=reason)
        return token

    def step(self, token, stage, reason=''):
        self.finish(token, next_stage=stage, next_reason=reason)

    def finish(self, token, *, error=None, next_stage=None, next_reason='', **metrics):
        if token is None:
            return
        span=self.spans.get(token)
        if span is not None:span['finished']=True
        if not self.lock.acquire(blocking=False):
            self.dropped+=1
            return
        try:
            span=self.spans.pop(token,None)
            if span is None:return
            duration=(time.monotonic()-span['started'])*1000
            if next_stage is not None:
                self.spans[token]=dict(span,stage=next_stage,reason=next_reason,started=time.monotonic(),lastWait=0.,finished=False)
        finally:
            self.lock.release()
        if error is not None or span['announce'] or duration>=100:
            self.emit('call-finished' if next_stage is None else 'stage-finished',
                **{key:span[key] for key in ('stage','account','entry_id','symbol','reason')},
                duration_ms=duration,error=error,**metrics)

    def waiting(self):
        rows=[]
        if not self.lock.acquire(blocking=False):return rows
        try:
            now=time.monotonic()
            retired=[]
            for token,span in self.spans.items():
                if span.get('finished'):
                    retired.append(token)
                    continue
                duration=(now-span['started'])*1000
                if duration>=1000 and (not span['lastWait'] or now-span['lastWait']>=5):
                    span['lastWait']=now
                    rows.append(dict(schema=1,atMs=round(time.time()*1000,3),event='waiting',level='warning',
                        account=self.text(span['account'] if span['account'] is not None else self.account,32),
                        entryId=self.text(span['entry_id'],130) if span['entry_id'] is not None else None,
                        symbol=self.text(span['symbol'],32) if span['symbol'] is not None else None,
                        stage=self.text(span['stage'],96),reason=self.text(span['reason']),durationMs=round(duration,3)))
            for token in retired:self.spans.pop(token,None)
        finally:
            self.lock.release()
        return rows

    def write(self, rows):
        self.path.parent.mkdir(parents=True,exist_ok=True)
        paths=[self.path,self.path.with_name(self.path.name+'.1'),self.path.with_name(self.path.name+'.2')]
        if any(path.is_symlink() for path in paths) or self.path.parent.is_symlink():
            raise ValueError('Diagnostic log cannot follow a symbolic link')
        size=self.path.stat().st_size if self.path.exists() else 0
        pending=[]
        def append():
            if not pending:return
            fd=os.open(self.path,os.O_APPEND|os.O_CREAT|os.O_WRONLY,0o600)
            with os.fdopen(fd,'ab') as stream:stream.write(b''.join(pending))
            self.written+=len(pending)
            pending.clear()
        for row in rows:
            for key,maximum in (('event',64),('account',32),('entryId',130),('symbol',32),('stage',96),('reason',384),('blockedBy',64)):
                if row.get(key) is not None:row[key]=self.text(row[key],maximum)
            if type(row.get('atMs')) in (int,float):
                row['atUtc']=time.strftime('%Y-%m-%dT%H:%M:%S',time.gmtime(row['atMs']/1000))+'.%03dZ'%int(row['atMs']%1000)
            raw=(json.dumps(row,allow_nan=False,separators=(',',':'))+'\n').encode('utf-8')
            if len(raw)>self.max_bytes:
                self.dropped+=1
                continue
            if size+len(raw)>self.max_bytes:
                append()
                paths[2].unlink(missing_ok=True)
                if paths[1].exists():os.replace(paths[1],paths[2])
                if paths[0].exists():os.replace(paths[0],paths[1])
                size=0
            pending.append(raw)
            size+=len(raw)
        append()

    def run(self):
        try:
            EntryProgressObserver.background_priority(self)
            rows=[];began=0.
            while not self.stopping.is_set() or not self.queue.empty() or rows:
                incoming=self.queue.take(256-len(rows))
                if incoming and not rows:began=time.monotonic()
                rows.extend(incoming)
                rows.extend(self.waiting())
                now=time.monotonic()
                hot=now-self.last_command_at<.1 and now-began<.5
                if rows and (not hot or len(rows)>=256 or self.stopping.is_set()):
                    self.write(rows);rows=[]
                self.stopping.wait(.05 if rows or self.spans else .25)
        except Exception as error:
            self.write_error=type(error).__name__+': '+self.text(error,192)
        finally:
            self.active=False

    def close(self, timeout=.5):
        self.stopping.set()
        if self.thread and self.thread is not threading.current_thread():self.thread.join(timeout=timeout)

    def snapshot(self):
        return dict(active=self.active,file=self.path.name,priority=self.priority,queued=self.queue.qsize(),
            written=self.written,dropped=self.dropped,writeError=self.write_error,maxFileBytes=self.max_bytes,files=3,
            nativeReadsAdded=0,tradingWaitsForLog=False)


class ChunkEntries:
    def __init__(self, bridge, restore=True):
        self.bridge = bridge
        diagnostic=getattr(bridge,'entry_diagnostics',None)
        self.entry_diagnostics=diagnostic if isinstance(diagnostic,EntryDiagnostics) else None
        self.storage = bridge
        self.entry_controls = getattr(getattr(self, 'coordinator', None), 'entry_controls', None) or EntryControlBarrier(bridge)
        # Admission owns the parent lock. Its reads must never queue behind
        # account lanes waiting to merge under that same lock.
        self.admission_executor = ThreadPoolExecutor(max_workers=17, thread_name_prefix='iscc-admission')
        self.lanes = {}
        self.lane_jobs = {}
        self.parent_halts = set()
        self.wake_event = threading.Event()
        self.events = threading.Condition()
        self.event_revision = 0
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.stop_requests_lock = threading.Lock()
        self.stop_requests = {}
        self.stop_digests = {}
        self.public_lock = threading.Lock()
        self.public_records = {}
        self.public_sequence = 0
        self.public_epoch = os.urandom(12).hex()
        self.progress_observer = None
        # Only this coordinator changes parent phases. Keep an index of
        # successfully committed active parents; historical JSON is read once
        # on startup, rather than again on every 50 ms price evaluation.
        self.active_ids = set()
        self.thread = None
        self.pending_poll_at = {}
        self.protection_requests = {}
        self.reconcile_jobs = {}
        self.reconcile_poll_at = {}
        self.owner_index = {}
        self.owner_lock = threading.RLock()
        self.owner_misses = {}
        self.snapshot_jobs = {}
        self.retired_snapshot_jobs = set()
        self.entry_clocks = {}
        with bridge.lock:
            bridge.db.execute('CREATE TABLE IF NOT EXISTS chunk_entries (id TEXT PRIMARY KEY, digest TEXT, payload TEXT, state TEXT)')
            bridge.db.execute('CREATE TABLE IF NOT EXISTS chunk_entry_stops (id TEXT PRIMARY KEY, digest TEXT, result TEXT)')
            bridge.db.execute('CREATE TABLE IF NOT EXISTS chunk_entry_cancels (entry_id TEXT PRIMARY KEY, stop_id TEXT)')
            bridge.db.execute('CREATE TABLE IF NOT EXISTS chunk_entry_protections (id TEXT PRIMARY KEY, digest TEXT, result TEXT)')
            bridge.db.execute('CREATE TABLE IF NOT EXISTS chunk_entry_closures (id TEXT PRIMARY KEY, digest TEXT, result TEXT)')
            self.stop_digests = dict(bridge.db.execute('SELECT id,digest FROM chunk_entry_stops'))
            rows = self._startup_rows(bridge.db)
            restored = []
            for identifier, text in rows:
                state = json.loads(text)
                if not restore or state.get('accountExecution') == 'independent-v5':
                    pass  # The coordinator restores v5 from its account journals.
                elif state.get('protectionPending') or state.get('closurePending'):
                    state.update(phase='unknown', reason='Protection command interrupted; inspect its saved receipt; no automatic replay')
                    bridge.db.execute('UPDATE chunk_entries SET state=? WHERE id=?', (json.dumps(state), identifier))
                elif self._protection_quote_wait(state):
                    # A definite pre-mutation quote rejection permits only a
                    # fresh protective SL intent, never another entry part.
                    state.update(phase='paused', reason=HYBRID_STOP_QUOTE_WAIT)
                    bridge.db.execute('UPDATE chunk_entries SET state=? WHERE id=?', (json.dumps(state), identifier))
                elif state.get('reconciliation', {}).get('status') == 'checking' and callable(getattr(bridge, 'entry_reconcile', None)):
                    # Reconcile saved identities after restart, but never
                    # restore permission to acquire another part.
                    state['reconciliation']['resumeAllowed'] = False
                    state['hybridRemainderStop'] = 'Receiver restarted; further entry stopped while broker state is checked'
                    state.pop('reconcileTransientHalt', None)
                    state.update(phase='paused', reason='Checking exact broker execution; reserved volume remains locked')
                    bridge.db.execute('UPDATE chunk_entries SET state=? WHERE id=?', (json.dumps(state), identifier))
                elif state['phase'] in ACTIVE:
                    claimed = (any(child['state'] in ('sending', 'pending', 'unknown') for child in state['children'])
                               or any(details.get('commonStopUncertain') or details.get('riskRepairRequired')
                                      for details in state['accounts'].values()))
                    state.update(phase='unknown' if claimed else 'stopped',
                                 reason='Receiver restarted; an in-flight tranche may have executed. Inspect positions; automatic resumption is blocked' if claimed else
                                        'Receiver restarted; automatic resumption of an existing entry is blocked',
                                 updatedAt=time.time() * 1000)
                    bridge.db.execute('UPDATE chunk_entries SET state=? WHERE id=?', (json.dumps(state), identifier))
                restored.append(state)
            bridge.db.commit()
            for state in restored:
                # Publish only committed restart outcomes. Restart never re-arms.
                self._index_committed(state)
                self._remember(state)

    @property
    def primary(self):
        return str(self.bridge.config['account'])

    @staticmethod
    def _startup_rows(database):
        # Retain every unresolved/active plan, but load completed history by ID.
        # This changes memory/startup cost only; no durable receipt is removed.
        return database.execute("SELECT id,state FROM chunk_entries WHERE json_extract(state,'$.phase') "
            "IN ('armed','running','paused','sending','unknown') OR id IN "
            "(SELECT id FROM chunk_entries ORDER BY rowid DESC LIMIT 30) ORDER BY rowid").fetchall()

    def _enable_lane_storage(self):
        # Native account calls may hold Bridge.lock for a broker response.
        # A separate WAL connection lets a peer journal its own exact outcome.
        with self.lock:
            if self.storage is not self.bridge:
                return
            with self.bridge.lock:
                path = self.bridge.db.execute('PRAGMA database_list').fetchone()[2]
            if not path:
                raise ValueError('Independent account lanes require durable file storage')
            db = sqlite3.connect(path, check_same_thread=False, timeout=5)
            db.execute('PRAGMA journal_mode=WAL')
            db.execute('PRAGMA synchronous=FULL')
            self.storage = SimpleNamespace(db=db, lock=threading.RLock())

    def _drain_lanes(self, identifier):
        with self.lock:
            jobs = [future for (parent, _), future in self.lane_jobs.items() if parent == identifier]
        if jobs and wait(jobs, timeout=5).not_done:
            raise ValueError('Account command still draining; existing claims remain locked')
        for future in jobs:
            future.result()

    def _schedule_lanes(self, payload, state):
        if self._stopping(state['id']) or self._protecting(state['id']):
            return
        executor = self.bridge.warm_group_executor()
        for slot, account in enumerate(state['accounts']):
            key = (state['id'], account)
            future = self.lane_jobs.get(key)
            if future is not None and not future.done():
                continue
            if future is not None:
                error = future.exception()
                if error is not None:
                    if self.entry_diagnostics:
                        self.entry_diagnostics.emit('account-error',account=account,entry_id=state['id'],symbol=state.get('symbol'),
                            stage='account-lane',error=error)
                    state['accounts'][account].update(entryStatus='stopped', entryReason=str(error)[:256])
                    state.setdefault('lanePhases', {})[account] = 'unknown'
                    phases = [state['lanePhases'].get(key, 'armed') for key in state['accounts']]
                    state['phase'] = 'running' if any(phase in ACTIVE for phase in phases) else 'unknown'
                    self._save(state)
                    self.lane_jobs.pop(key, None)
                    continue
            phase = state.get('lanePhases', {}).get(account, 'armed')
            if phase not in ACTIVE:
                continue
            lane = self.lanes.get(key)
            if lane is None:
                lane = AccountEntryLane(self, account, payload.get('accountLaneSlot',slot))
                self.lanes[key] = lane
            lane.lane_submission_limit = HYBRID_MAX_SUBMISSIONS//len(state['accounts'])
            local = copy.deepcopy(state)
            local['accounts'] = {account:local['accounts'][account]}
            local['children'] = [child for child in local['children'] if account in child.get('volumes', {})]
            local['phase'] = phase
            local['nextChunk'] = sum(child['type'] == 'market' and child['state'] == 'confirmed' for child in local['children'])
            local.pop('lanePhases', None)
            if not local.get('hybridRemainderStop') and local['accounts'][account].get('laneStopReason'):
                local['hybridRemainderStop'] = local['accounts'][account]['laneStopReason']
            # Reconciliation belongs to the account with the unknown command.
            local.pop('reconciliation', None)
            if account in state.get('laneReconciliation', {}):
                local['reconciliation'] = copy.deepcopy(state['laneReconciliation'][account])
            self.lane_jobs[key] = executor.submit(lane.run, copy.deepcopy(payload), local)
            self.lane_jobs[key].add_done_callback(lambda future,lane=lane:self._lane_finished(future,lane))

    def _lane_finished(self, future, lane=None):
        if not future.cancelled() and (future.exception() is not None or future.result()
                or lane is not None and lane.read_ready.is_set()):
            self.wake_event.set()

    def entry_events(self, revision, timeout=20):
        with self.events:
            self.events.wait_for(lambda:self.event_revision != revision or self.stop_event.is_set(), timeout=timeout)
            observed = self.event_revision
        # Snapshot construction can acquire projection locks and publish a
        # newer event. Never hold the condition across that work or acknowledge
        # a revision newer than the snapshot we are about to construct.
        return dict(revision=observed, entries=[dict(row, lifecycleDeferred=True,
            positionsConfirmed=False) for row in self.list()])

    def _read_finished(self, future):
        ready = getattr(self,'read_ready',None)
        if ready is not None:ready.set()
        self.wake_event.set()

    def _load(self, identifier):
        with self.storage.lock:
            row = self.storage.db.execute('SELECT digest,payload,state FROM chunk_entries WHERE id=?', (identifier,)).fetchone()
        return (row[0], json.loads(row[1]), json.loads(row[2])) if row else None

    def _cancelled(self, identifier):
        with self.storage.lock:
            row = self.storage.db.execute('SELECT stops.result FROM chunk_entry_cancels cancels '
                                         'LEFT JOIN chunk_entry_stops stops ON stops.id=cancels.stop_id WHERE cancels.entry_id=?',
                                         (identifier,)).fetchone()
        if not row:
            return None
        if row[0] is None:
            raise ValueError('Entry was cancelled; its saved stop receipt is unavailable, so admission remains blocked')
        return json.loads(row[0])

    def _save(self, state, receipt=None):
        with self.lock:
            if state.get('executionPolicy') == 'hybrid-market-limit-v1':
                # Full immutable submission guards already live in the broker
                # journal. Completed children need only their bounds, not a
                # repeated copy of every preceding position on every commit.
                for child in state.get('children', []):
                    if child.get('state') in ('confirmed', 'cancelled', 'rejected'):
                        for guard in child.get('guards', {}).values():
                            guard.pop('positions', None)
            state['updatedAt'] = time.time() * 1000
            with self.storage.lock:
                try:
                    changed = self.storage.db.execute('UPDATE chunk_entries SET state=? WHERE id=?', (json.dumps(state, allow_nan=False), state['id']))
                    if changed.rowcount != 1:
                        raise ValueError('Chunk entry parent is missing from the durable ledger')
                    if receipt is not None:
                        table, identifier, result = receipt
                        if table not in ('chunk_entry_protections', 'chunk_entry_closures'):
                            raise ValueError('Invalid parent command receipt table')
                        changed = self.storage.db.execute('UPDATE '+table+' SET result=? WHERE id=?',
                            (json.dumps(result, allow_nan=False), identifier))
                        if changed.rowcount != 1:
                            raise ValueError('Parent command intent is missing from the durable ledger')
                    self.storage.db.commit()
                except Exception:
                    self.storage.db.rollback()
                    raise
            self._index_committed(state)
            self._remember(state)

    def _index_committed(self, state):
        self._index_owners(state)
        if state['phase'] in ACTIVE:
            self.active_ids.add(state['id'])
        else:
            self.active_ids.discard(state['id'])

    def _index_owners(self, state):
        if state.get('executionPolicy') == 'hybrid-market-limit-v1':
            pairs=[]
            for child in state.get('children', []):
                for account, row in child.get('positions', {}).items():
                    pairs.append((str(account),str(row['ticket'])))
                result = child.get('result', {})
                primary = next(iter(state['accounts'])) if len(state.get('accounts',{}))==1 else self.primary
                outcomes = result.get('accountResults', {primary:result})
                for account, outcome in outcomes.items():
                    if re.fullmatch(r'[1-9][0-9]{0,19}', str(outcome.get('providerOrderId', ''))):
                        pairs.append((str(account),str(outcome['providerOrderId'])))
            added=[]
            with self.owner_lock:
                for pair in pairs:
                    owners=self.owner_index.setdefault(pair,set())
                    if state['id'] not in owners:
                        owners.add(state['id']);added.append(pair);self.owner_misses.clear()
            callback=getattr(self,'ownership_callback',None)
            if added and callback:callback(state['id'],added)

    def owned_entry(self, account, ticket):
        key=(str(account),str(ticket))
        with self.owner_lock:
            owners = set(self.owner_index.get(key, ()))
            if not owners and key in self.owner_misses:return None
        if not owners and re.fullmatch(r'[1-9][0-9]{0,19}',key[1]):
            # A historical parent can be absent from the bounded RAM cache.
            # Filter text cheaply, then verify exact account/ticket ownership.
            with self.storage.lock:
                rows=self.storage.db.execute('SELECT state FROM chunk_entries WHERE instr(state,?)>0',
                    ('"'+key[1]+'"',)).fetchall()
            for row in rows:self._index_owners(json.loads(row[0]))
            with self.owner_lock:
                owners=set(self.owner_index.get(key,()))
                if not owners:
                    if len(self.owner_misses)>=256:self.owner_misses.pop(next(iter(self.owner_misses)))
                    self.owner_misses[key]=True
        if len(owners) > 1:
            raise ValueError('Position ownership is ambiguous')
        return next(iter(owners), None)

    def decorate_management(self, states):
        cache = {}
        aggregates = {}
        for account, view in states.items():
            for row in view.get('orders', []):
                parent = self.owned_entry(account,row['ticket'])
                if parent and row.get('__isPosition'):
                    aggregates.setdefault((parent,account),[]).append(row)
        for account, view in states.items():
            for row in view.get('orders', []):
                identifier = self.owned_entry(account, row['ticket'])
                if not identifier:
                    continue
                if identifier not in cache:
                    cache[identifier] = self._load(identifier)
                found = cache[identifier]
                if not found:
                    raise ValueError('Owned position parent is missing')
                state = found[2]
                actual = aggregates.get((identifier,account),[])
                actual_volume = sum(float(value['lots']) for value in actual)
                average = sum(float(value['lots'])*float(value['open_price']) for value in actual)/actual_volume if actual_volume else None
                stops = {float(value['sl']) for value in actual}; takes = {float(value['tp']) for value in actual}
                row['entryPlan'] = dict(id=identifier, executionPolicy=state['executionPolicy'], side=state['side'],
                    accounts=list(state['accounts']), phase=state['phase'], averagePrice=average,
                    sl=next(iter(stops)) if len(stops)==1 else None, tp=next(iter(takes)) if len(takes)==1 else None,
                    protectionReady=any(account in child.get('positions', {}) for child in state['children'])
                        and state['phase'] != 'unknown' and not state.get('protectionPending') and not state.get('closurePending')
                        and not state.get('protectionOnlyWait')
                        and len(stops)==1 and len(takes)==1
                        and not any(value.get('commonStopUncertain') or value.get('riskRepairRequired')
                                    for value in state['accounts'].values()))

    def _protecting(self, identifier):
        with self.stop_requests_lock:
            return bool(self.protection_requests.get(identifier))

    def _protection_receipt(self, identifier):
        with self.storage.lock:
            row = self.storage.db.execute('SELECT digest,result FROM chunk_entry_protections WHERE id=?', (identifier,)).fetchone()
        return (row[0], json.loads(row[1])) if row else None

    def protection(self, command):
        identifier, entry_id = command.get('id'), command.get('entryId')
        if (not isinstance(identifier, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,130}', identifier)
                or not isinstance(entry_id, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,100}', entry_id)):
            raise ValueError('Invalid protection command or parent ID')
        fingerprint = digest(command)
        saved = self._protection_receipt(identifier)
        if saved:
            if saved[0] != fingerprint:
                raise ValueError('Protection command ID reused with different parameters')
            return dict(result=saved[1], entry=self.get(entry_id))
        self._expiry(command)
        with self.stop_requests_lock:
            self.protection_requests[entry_id] = self.protection_requests.get(entry_id, 0) + 1
        try:
            self._drain_lanes(entry_id)
            with self.lock:
                return self._protection_locked(command, fingerprint)
        finally:
            with self.stop_requests_lock:
                count = self.protection_requests[entry_id] - 1
                if count:
                    self.protection_requests[entry_id] = count
                else:
                    del self.protection_requests[entry_id]

    def _protection_locked(self, command, fingerprint):
        identifier, entry_id = command['id'], command['entryId']
        saved = self._protection_receipt(identifier)
        if saved:
            if saved[0] != fingerprint:
                raise ValueError('Protection command ID reused with different parameters')
            return dict(result=saved[1], entry=self.get(entry_id))
        self._expiry(command)
        found = self._load(entry_id)
        if not found:
            raise ValueError('Hybrid parent does not exist')
        _, payload, state = found
        if (payload.get('executionPolicy') != 'hybrid-market-limit-v1' or command.get('symbol') != payload['symbol']
                or state['phase'] == 'unknown' or state.get('protectionPending') or state.get('closurePending') or state.get('positionsClosed')
                or any(child['state'] in ('sending','unknown') for child in state['children'])
                or any(details.get('commonStopUncertain') for details in state['accounts'].values())):
            raise ValueError('Hybrid ownership or previous protection is unconfirmed')
        allowed = {'id','entryId','symbol','operation','account','mirrorAccounts','sl','tp','expiresAt'}
        if set(command) - allowed or command.get('operation') not in ('modify','breakeven'):
            raise ValueError('Invalid parent protection operation')
        accounts = [str(command.get('account', ''))]
        mirrors = command.get('mirrorAccounts', [])
        if not isinstance(mirrors, list) or any(not isinstance(row, dict) or set(row) != {'account'} for row in mirrors):
            raise ValueError('Invalid protection follower selection')
        accounts += [str(row['account']) for row in mirrors]
        if len(accounts) != len(set(accounts)) or not set(accounts).issubset(state['accounts']):
            raise ValueError('Protection accounts do not belong to this parent')
        if command['operation'] == 'breakeven' and 'sl' in command or command['operation'] == 'modify' and not {'sl','tp'}.intersection(command):
            raise ValueError('Specify protection prices or server weighted breakeven')
        if 'sl' in command:
            number(command['sl'], 'Stop loss')
        if 'tp' in command and number(command['tp'], 'Take profit', positive=False) < 0:
            raise ValueError('Take profit cannot be negative')
        previous_phase = state['phase']
        result = dict(id=identifier, entryId=entry_id, status='unknown', reason='Protection intent saved; no automatic replay', accountResults={})
        # Persist the parent pause and command intent together, before even a
        # cancellation can race a fill. A restart cannot resume either action.
        state.update(protectionPending=identifier, phase='paused', reason='Updating whole-parent protection')
        with self.storage.lock:
            if any(self.storage.db.execute('SELECT 1 FROM '+table+' WHERE id=?', (identifier,)).fetchone() for table in ('requests','groups','chunk_entry_closures')):
                raise ValueError('Protection ID collides with an existing command')
            try:
                self.storage.db.execute('INSERT INTO chunk_entry_protections VALUES (?,?,?)', (identifier, fingerprint, json.dumps(result)))
                self.storage.db.execute('UPDATE chunk_entries SET state=? WHERE id=?', (json.dumps(state), entry_id))
                self.storage.db.commit()
            except Exception:
                self.storage.db.rollback();raise
        self._index_committed(state); self._remember(state)
        applying = False; cancelled = False
        try:
            for child in state['children']:
                if child.get('type') == 'limit' and child['state'] not in ('confirmed','cancelled','rejected'):
                    self._hybrid_limit(payload, state, child, cancel=True)
            cancelled = True
            reduced = self._refresh_protection_positions(payload,state)
            plans = {}; projected_pause = reduced or state.get('entryReduced', False); lower, upper = market_price_bounds(payload)
            direction = 1 if payload['side'] == 'buy' else -1
            for account in accounts:
                self._account(account)
                positions = [child['positions'][account] for child in state['children'] if account in child.get('positions', {}) and not child['positions'][account].get('closed')]
                if not positions or any(self.owned_entry(account, row['ticket']) != entry_id for row in positions):
                    raise ValueError('No exact owned positions on a selected account')
                request = dict(id='parent-protection:'+hashlib.sha256((identifier+':'+account).encode()).hexdigest(),
                    symbol=payload['symbol'], side=payload['side'], positions=[{key: row[key] for key in ('ticket','volume','fillPrice','sl','tp')} for row in positions],
                    riskBudget=state['accounts'][account]['riskUsd'], maxVolume=state['accounts'][account]['targetVolume'])
                if command['operation'] == 'breakeven':
                    request['breakeven'] = True
                else:
                    request['sl'] = command.get('sl', positions[0]['sl'])
                    if 'sl' not in command and any(abs(row['sl']-request['sl']) > 1e-8 for row in positions):
                        raise ValueError('A TP-only command cannot silently change unequal stops')
                if 'tp' in command:
                    request['tp'] = command['tp']
                if previous_phase in ACTIVE:
                    request.update(remainingVolume=max(0., state['accounts'][account]['targetVolume']-state['accounts'][account]['filledVolume']),
                                   adversePrice=upper if direction == 1 else lower)
                    if 'sl' not in command and command['operation'] != 'breakeven' and 'sl' not in state['accounts'][account].get('protectionOverride', {}):
                        distance = entry_stop_distance(payload)
                        if distance is not None:
                            request['futureStopDistance'] = distance
                planned = self._call(account, 'entry_protection_plan', request)
                if planned.get('status') != 'ok' or str(planned.get('account')) != account:
                    raise ValueError('Protection preflight rejected: '+str(planned.get('reason', 'Account mismatch')))
                # Freeze weighted breakeven now. Later fills cannot move this
                # explicit user level back to the original 16-point policy.
                request.pop('breakeven', None); request['sl'] = planned['sl']
                projected_pause |= planned.get('projectedRisk', 0) > state['accounts'][account]['riskUsd']+1e-8
                plans[account] = request
            self._expiry(command)
            for account, request in plans.items():
                applying = True
                outcome = self._call(account, 'entry_common_stop', request)
                result['accountResults'][account] = hybrid_receipt(outcome)
                if outcome.get('status') != 'ok' or str(outcome.get('account')) != account:
                    raise RuntimeError('Whole-parent protection outcome is unconfirmed')
                rows = {str(row['ticket']): row for row in outcome.get('positions', [])}
                expected = {row['ticket']:row for row in request['positions']}
                if set(rows) != set(expected):
                    raise RuntimeError('Protection returned different owned tickets')
                for child in state['children']:
                    position = child.get('positions', {}).get(account)
                    if position and not position.get('closed'):
                        updated = rows[position['ticket']]
                        if any(abs(float(updated[key])-position[key]) > 1e-8 for key in ('volume','fillPrice')):
                            raise RuntimeError('Position changed during protection')
                        position.update(sl=number(updated['sl'], 'Confirmed stop'), tp=number(updated['tp'], 'Confirmed take profit', positive=False))
                details = state['accounts'][account]
                override = details.setdefault('protectionOverride', {})
                if 'sl' in command or command['operation'] == 'breakeven':
                    override['sl'] = number(outcome['sl'], 'Confirmed whole-parent stop')
                if 'tp' in command:
                    override['tp'] = command['tp']
                details.update(averagePrice=outcome['averagePrice'], sl=outcome['sl'], tp=outcome.get('tp', request['positions'][0]['tp']), commonStopUncertain=False)
                details['stopSignature'] = digest(dict(positions=[{key: row[key] for key in ('ticket','volume','fillPrice')} for row in rows.values()]))
                self._hybrid_recount(payload, state)
                self._save(state)
            state.pop('protectionPause', None)
            if projected_pause:
                state['protectionPause'] = ('Owned position was reduced; further entry remains paused' if state.get('entryReduced') else
                    'Manual protection confirmed; projected remaining target exceeds captured risk; further entry paused')
            state['phase'] = ('paused' if state.get('protectionPause') else 'running') if previous_phase in ACTIVE else previous_phase
            state['reason'] = state.get('protectionPause', 'Whole-parent protection confirmed')
            result.update(status='ok', reason=state['reason'])
        except Exception as error:
            result.update(status='unknown' if applying or not cancelled else 'rejected', reason=str(error))
            state.update(phase='unknown' if applying or not cancelled else 'paused' if previous_phase in ACTIVE else previous_phase, reason=str(error))
            if not applying and cancelled:
                state['protectionPause'] = 'Protection rejected; further entry paused for review'
        if result['status'] != 'unknown':
            state.pop('protectionPending', None)
        self._save(state, ('chunk_entry_protections', identifier, result))
        return dict(result=result, entry=public_record(state))

    def _closure_receipt(self, identifier):
        with self.storage.lock:
            row = self.storage.db.execute('SELECT digest,result FROM chunk_entry_closures WHERE id=?', (identifier,)).fetchone()
        return (row[0], json.loads(row[1])) if row else None

    def close_position(self, command):
        identifier, entry_id = command.get('id'), command.get('entryId')
        if (not isinstance(identifier,str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,130}',identifier)
                or not isinstance(entry_id,str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,100}',entry_id)
                or set(command) != {'id','entryId','symbol','expiresAt'}):
            raise ValueError('Invalid whole-parent close command')
        fingerprint = digest(command)
        saved = self._closure_receipt(identifier)
        if saved:
            if saved[0] != fingerprint:
                raise ValueError('Close command ID reused with different parameters')
            return dict(result=saved[1],entry=self.get(entry_id))
        self._expiry(command)
        with self.stop_requests_lock:
            self.protection_requests[entry_id] = self.protection_requests.get(entry_id,0)+1
        try:
            self._drain_lanes(entry_id)
            with self.lock:
                return self._close_position_locked(command,fingerprint)
        finally:
            with self.stop_requests_lock:
                count = self.protection_requests[entry_id]-1
                if count: self.protection_requests[entry_id]=count
                else: del self.protection_requests[entry_id]

    def _owned_close_view(self, payload, state, account):
        positions = [child['positions'][account] for child in state['children'] if account in child.get('positions',{})]
        if len({row['ticket'] for row in positions}) != len(positions):
            raise ValueError('Owned close tickets are duplicated')
        watch = [dict(account=account,ticket=row['openingTicket'],symbol=payload['symbol']) for row in positions]
        data = self._call(account,'entry_management',watch)
        if str(data.get('account')) != account:
            raise ValueError('Close account identity mismatch')
        result = []
        for captured in positions:
            if self.owned_entry(account,captured['ticket']) != state['id']:
                raise ValueError('Close position ownership is unconfirmed')
            row = next((row for row in data.get('orders',[]) if row.get('__isPosition') and str(row['ticket'])==captured['ticket']),None)
            if row is None:
                proof = data.get('openingStates',{}).get(captured['openingTicket'],{})
                if proof.get('phase') != 'closed':
                    raise ValueError('Absent owned position has no definite broker full-exit proof')
                result.append(dict(ticket=captured['ticket'],status='closed',volume=0.))
                continue
            volume = number(row.get('lots'),'Owned remaining close volume')
            if (row.get('symbol') != payload['symbol'] or row.get('type') != payload['side']
                    or abs(float(row.get('open_price',0))-captured['fillPrice']) > 1e-8
                    or volume > captured['volume']+1e-8):
                raise ValueError('Owned close position identity or remaining volume changed')
            if captured['openingTicket'] not in (str(row['ticket']),str(row.get('identifier'))):
                proof = self._call(account,'entry_position_identity',dict(ticket=captured['ticket'],
                    openingTicket=captured['openingTicket'],symbol=payload['symbol'],side=payload['side'],fillPrice=captured['fillPrice']))
                if (str(proof.get('account'))!=account or str(proof.get('ticket'))!=captured['ticket']
                        or str(proof.get('openingTicket'))!=captured['openingTicket']
                        or abs(float(proof.get('volume',-1))-volume)>1e-8
                        or abs(float(proof.get('fillPrice',0))-captured['fillPrice'])>1e-8):
                    raise ValueError('Exact broker opening-to-position proof changed')
            result.append(dict(ticket=captured['ticket'],status='open',volume=volume,sl=float(row['sl']),tp=float(row['tp'])))
        return result

    def _refresh_protection_positions(self, payload, state):
        reduced = False
        for account in state['accounts']:
            views = {row['ticket']:row for row in self._owned_close_view(payload,state,account)}
            for child in state['children']:
                position = child.get('positions',{}).get(account)
                if not position:
                    continue
                current = views[position['ticket']]
                if current['status']=='closed':
                    reduced |= not position.get('closed',False)
                    position['closed']=True
                    continue
                if position.get('closed') or any(abs(current[key]-position[key])>1e-8 for key in ('sl','tp')):
                    raise ValueError('Owned protection changed outside this parent')
                if current['volume'] < position['volume']-1e-8:
                    acquired = position.get('acquiredVolume',position['volume'])
                    proof = self._call(account,'entry_position_remainder',dict(ticket=position['ticket'],symbol=payload['symbol'],
                        side=payload['side'],openingTicket=position['openingTicket'],fillPrice=position['fillPrice'],acquiredVolume=acquired))
                    if str(proof.get('account')) != account or abs(float(proof.get('volume',-1))-current['volume'])>1e-8:
                        raise ValueError('Partial remaining volume is not broker-confirmed')
                    position.update(acquiredVolume=acquired,volume=current['volume']);reduced=True
        if reduced:
            state['entryReduced'] = True
        self._hybrid_recount(payload,state)
        self._save(state)
        return reduced

    def _close_position_locked(self, command, fingerprint):
        identifier,entry_id=command['id'],command['entryId']
        saved=self._closure_receipt(identifier)
        if saved:
            if saved[0] != fingerprint: raise ValueError('Close command ID reused with different parameters')
            return dict(result=saved[1],entry=self.get(entry_id))
        self._expiry(command)
        found=self._load(entry_id)
        if not found: raise ValueError('Hybrid parent does not exist')
        _,payload,state=found
        if (payload.get('executionPolicy') != 'hybrid-market-limit-v1' or command['symbol'] != payload['symbol']
                or state['phase']=='unknown' or state.get('protectionPending') or state.get('closurePending')
                or any(child['state'] in ('sending','unknown') for child in state['children'])
                or any(details.get('commonStopUncertain') for details in state['accounts'].values())):
            raise ValueError('Whole-parent close blocked by uncertain ownership or protection')
        result=dict(id=identifier,entryId=entry_id,status='unknown',reason='Whole-parent close intent saved; no automatic replay',accountResults={})
        state.update(closurePending=identifier,phase='paused',reason='Closing exact owned parent positions',hasOpenPositions=True,positionsConfirmed=False)
        with self.storage.lock:
            if any(self.storage.db.execute('SELECT 1 FROM '+table+' WHERE id=?',(identifier,)).fetchone() for table in ('requests','groups','chunk_entry_protections')):
                raise ValueError('Close command ID collides with an existing command')
            try:
                self.storage.db.execute('INSERT INTO chunk_entry_closures VALUES (?,?,?)',(identifier,fingerprint,json.dumps(result)))
                self.storage.db.execute('UPDATE chunk_entries SET state=? WHERE id=?',(json.dumps(state),entry_id))
                self.storage.db.commit()
            except Exception:
                self.storage.db.rollback();raise
        self._index_committed(state);self._remember(state)
        try:
            # Every account's pending remainder is cancelled and its fill race
            # accounted before the first close can affect any owned position.
            for child in state['children']:
                if child.get('type')=='limit' and child['state'] not in ('confirmed','cancelled','rejected'):
                    self._hybrid_limit(payload,state,child,cancel=True)
            views={}
            for account in state['accounts']:
                self._account(account)
                views[account]=self._owned_close_view(payload,state,account)
            self._expiry(command)
            for account,rows in views.items():
                results=[];result['accountResults'][account]=dict(status='unknown',positions=results)
                for row in rows:
                    if row['status']=='closed':
                        results.append(dict(ticket=row['ticket'],status='closed',alreadyClosed=True));continue
                    close_id='parent-close:'+hashlib.sha256((identifier+':'+account+':'+row['ticket']).encode()).hexdigest()
                    outcome=self._call(account,'execute','close',dict(id=close_id,ticket=row['ticket'],symbol=payload['symbol'],
                        volume=row['volume'],expectedVolume=row['volume'],expiresAt=time.time()*1000+3000))['result']
                    results.append({**hybrid_receipt(outcome),'ticket':row['ticket']})
                    if outcome.get('status')!='ok':
                        raise RuntimeError('An owned close result is unconfirmed; no automatic retry')
                proof=self._owned_close_view(payload,state,account)
                if any(row['status']!='closed' for row in proof):
                    raise RuntimeError('Broker has not confirmed every owned position fully closed')
                result['accountResults'][account]['status']='ok'
            # Read all accounts again: no successful envelope relies solely on
            # an order-send retcode or an earlier account's stale snapshot.
            for account in state['accounts']:
                if any(row['status']!='closed' for row in self._owned_close_view(payload,state,account)):
                    raise RuntimeError('Final whole-parent full-exit proof is incomplete')
            state.pop('closurePending',None);state.pop('protectionPause',None)
            state.update(phase='stopped',positionsClosed=True,hasOpenPositions=False,positionsConfirmed=True,positionLifecycle='closed',
                         reason='All exact owned parent positions closed and broker-confirmed')
            result.update(status='ok',reason=state['reason'])
        except Exception as error:
            state.update(phase='unknown',reason=str(error));result['reason']=str(error)
        self._save(state, ('chunk_entry_closures', identifier, result))
        return dict(result=result,entry=public_record(state))

    def _active_rows(self):
        # Called under self.lock. Ordering matches the previous full-table
        # scan, so concurrent active parents keep their admission order.
        identifiers = tuple(self.active_ids)
        if not identifiers:
            return []
        with self.storage.lock:
            rows = self.storage.db.execute('SELECT payload,state FROM chunk_entries WHERE id IN ('
                + ','.join('?' for _ in identifiers) + ') ORDER BY rowid', identifiers).fetchall()
        if len(rows) != len(identifiers):
            raise ValueError('An active chunk entry is missing from the durable ledger')
        return rows

    @staticmethod
    def _public_rank(row):
        sequence, state = row[:2]
        return (0 if state['phase'] in ACTIVE else 1 if state['phase'] == 'unknown' else 2,
                -state.get('updatedAt', 0), -sequence)

    def _remember(self, state):
        with self.public_lock:
            self.public_sequence += 1
            public = public_record(state)
            public.update(viewRevision=self.public_sequence,viewEpoch=self.public_epoch)
            size = len(json.dumps(public, separators=(',', ':'), allow_nan=False).encode())
            self.public_records[state['id']] = (self.public_sequence, public, size)
            if len(self.public_records) > 30:
                keep = sorted(self.public_records.values(), key=self._public_rank)[:30]
                self.public_records = {row[1]['id']: row for row in keep}
        with self.events:
            now = time.monotonic()
            signature = (state['phase'], tuple((child['id'],child['state']) for child in state['children']))
            previous = getattr(self, 'event_signatures', {})
            changed = previous.get(state['id']) != signature
            self.event_signatures = {key:value for key,value in previous.items() if key in self.public_records}
            self.event_signatures[state['id']] = signature
            if changed or now-getattr(self, 'last_event_at', 0) >= .1 or state['phase'] not in ACTIVE:
                self.last_event_at = now
                self.event_revision += 1
                self.events.notify_all()

    def _phase(self, state, phase, reason, force=False):
        if not force and state['phase'] == phase and state.get('reason') == reason:
            return
        if self.entry_diagnostics and phase in ('paused','unknown','stopped','completed'):
            for account in state.get('accounts',{}) or {self.primary:None}:
                self.entry_diagnostics.emit('entry-state',account=account,entry_id=state['id'],symbol=state.get('symbol'),
                    stage=phase,reason=reason,level='warning' if phase in ('paused','unknown','stopped') else 'info')
        state.update(phase=phase, reason=reason)
        self._save(state)

    @staticmethod
    def _expiry(payload):
        remaining = number(payload.get('expiresAt'), 'Command expiry') - time.time() * 1000
        ttl = payload.get('intentTtlMs', 3000)
        if ('intentTtlMs' in payload and (payload.get('intentDelivery') != 'entry-intent-delivery-v1'
                or type(ttl) is not int or ttl not in (15000,60000)
                or ttl==60000 and ('entryId' not in payload or 'account' in payload))):
            raise ValueError('Invalid entry intent delivery contract')
        if not 0 < remaining <= ttl:
            raise ValueError('Command expired or client clock is incorrect')

    def _call(self, account, method, *args, **kwargs):
        if method in ('execute','entry_cancel_limit','entry_common_stop','entry_reconcile'):
            getattr(self,'protection_proofs',{}).pop(account,None)
            for key in list(getattr(self,'opening_proofs',{})):
                if key[1]==account:self.opening_proofs.pop(key,None)
        # Never own the primary MT5/SQLite lock while waiting for a worker RPC.
        if account == self.primary:
            with self.bridge.lock:
                return getattr(self.bridge, method)(*args, **kwargs)
        if kwargs:
            raise ValueError('Worker calls require positional arguments')
        return self.bridge.linked[account].call(method, *args)

    def _account(self, account):
        # Coordinator.account() is routing metadata, not native permission or
        # hedging evidence. Read that proof from this account's existing worker
        # without holding the coordinator's shared storage lock across the RPC.
        worker = vars(self.bridge).get('primary_worker') if account == self.primary else None
        current = worker.call('account') if worker is not None else self._call(account, 'account')
        if str(field(current, 'login')) != account or field(current, 'currency') != 'USD':
            raise ValueError('Chunk entry account identity or currency mismatch')
        if field(current, 'margin_mode') != 2:
            raise ValueError('Chunk entry requires a hedging account on every participating terminal')
        if field(current, 'trade_allowed', True) is False or field(current, 'trade_expert', True) is False:
            raise ValueError('Account does not permit algorithmic trading')
        return current

    def _snapshot(self, account, symbol):
        data = self._call(account, 'snapshot', symbol)
        return self._quote_snapshot(account, data)

    def _entry_snapshot(self, account, symbol):
        data = self._call(account, 'entry_snapshot', symbol)
        return self._quote_snapshot(account, data)

    @staticmethod
    def _quote_snapshot(account, data, allow_stale=False):
        if str(data.get('account')) != account:
            raise ValueError('Quote account mismatch')
        quote = data.get('quote', {})
        age = time.time() * 1000 - number(quote.get('utcTime'), 'Quote UTC time') * 1000
        bid = number(quote.get('bid'), 'Bid'); ask = number(quote.get('ask'), 'Ask')
        if ask < bid:
            raise ValueError('Invalid Bid/Ask quote')
        metadata = data.get('metadata', {})
        point = number(metadata.get('contractSize'), 'USD point value')
        if not allow_stale and (age < -250 or age > 1000):
            raise QuoteWait(account, data)
        return data, bid, ask, point

    def account_readiness(self):
        """Read-only exercise of the same account RPC used by arm and tick."""
        accounts = [self.primary, *self.bridge.linked]
        for account in accounts:
            self._account(account)
        return dict(ready=True, accounts=accounts)

    def _child(self, payload, state, index, price):
        child = {key: payload[key] for key in DEADLINE_FIELDS}
        child.update(id=payload['id'] + ':c' + str(index + 1), symbol=payload['symbol'],
                     side=payload['side'], type='market', volume=state['accounts'][self.primary]['parts'][index],
                     price=price, sl=payload['sl'], tp=payload.get('tp', 0),
                     cid=payload.get('cid', ''), expiresAt=time.time() * 1000 + 3000)
        distance = entry_stop_distance(payload)
        if distance is not None:
            child['sl'] = rounded(price - (1 if payload['side'] == 'buy' else -1) * distance)
        if payload.get('mirrorAccounts'):
            child['mirrorAccounts'] = copy.deepcopy(payload['mirrorAccounts'])
        elif payload.get('mirrorAccount'):
            child['mirrorAccount'] = payload['mirrorAccount']
        return child

    def _admission_reads(self, payload, accounts):
        self.admission_batches = {}
        def read(account):
            if payload.get('initialSizingOnly') is True and len(accounts)==1:
                batch=self._call(account,'entry_admission',payload)
                identity=batch['identity']
                if (str(identity.get('login'))!=account or identity.get('currency')!='USD'
                        or identity.get('margin_mode')!=2 or identity.get('trade_allowed') is False
                        or identity.get('trade_expert') is False):
                    raise ValueError('Independent admission account identity or permissions mismatch')
                self.admission_batches[account]=batch
                return batch['snapshot'],batch['spec']
            self._account(account)
            return (self._call(account, 'snapshot', payload['symbol']),
                    self._call(account, 'lot_specs')[payload['symbol']])
        if payload.get('accountExecution') != 'independent-v4':
            return {account:read(account) for account in accounts}
        executor = self.admission_executor
        jobs = {account:executor.submit(read, account) for account in accounts}
        return {account:future.result() for account, future in jobs.items()}

    def _admission_checks(self, payload, accounts, first, plans):
        def check(account):
            request = dict(first, volume=plans[account][0])
            request.pop('mirrorAccounts', None); request.pop('mirrorAccount', None)
            batch=getattr(self,'admission_batches',{}).get(account)
            if batch and {k:v for k,v in batch.get('checkedRequest',{}).items() if k!='expiresAt'}=={
                    k:v for k,v in request.items() if k!='expiresAt'}:
                return True
            return self._call(account, 'check_opening', request)
        if payload.get('accountExecution') != 'independent-v4':
            for account in accounts:
                check(account)
            return
        executor = self.admission_executor
        jobs = [executor.submit(check, account) for account in accounts]
        for future in jobs:
            future.result()

    def arm(self, payload, received=None):
        if payload.get('accountExecution') == 'independent-v4':
            self._enable_lane_storage()
        with self.lock:
            identifier = str(payload.get('id', ''))
            if not re.fullmatch(r'[A-Za-z0-9_.:-]{1,100}', identifier):
                raise ValueError('Invalid chunk entry ID')
            existing = self._load(identifier)
            if existing:
                fingerprint = digest(payload)
                if existing[0] != fingerprint:
                    raise ValueError('Chunk entry ID reused with different parameters')
                return public_record(existing[2])
            cancelled = self._cancelled(identifier)
            if cancelled is not None:
                return public_record(cancelled)
            fingerprint = digest(payload)
            self._expiry(payload)
            if payload.get('symbol') not in self.bridge.config['symbols'] or payload.get('side') not in ('buy', 'sell'):
                raise ValueError('Symbol or side is not allowed')
            for key in ('trigger', 'volume', 'chunkVolume', 'riskUsd', 'sl'):
                number(payload.get(key), key)
            # Additional admission-only cap. Never enables the account risk
            # manager or changes/closes any existing position. The original
            # per-plan risk remains the bound on every subsequent child.
            for limit in (self.bridge.config.get('entryRiskLimitUsd', 0), payload.get('entryRiskLimitUsd', 0)):
                cap = number(limit, 'New entry risk limit', positive=False)
                if cap < 0 or cap > 0 and payload['riskUsd'] > cap+1e-8:
                    raise ValueError('New entry risk exceeds its additional admission limit; existing positions retained')
            for key in ('tickSize', 'stopOffsetPts'):
                if key in payload:
                    number(payload[key], key)
            if 'chunkPercent' in payload and payload.get('executionPolicy') != 'hybrid-market-limit-v1' and not 2 <= number(payload['chunkPercent'], 'Chunk percent') <= 100:
                raise ValueError('Chunk percent must be between 2 and 100')
            if 'level' in payload:
                number(payload['level'], 'LT level')
            if not isinstance(payload.get('cid', ''), str) or len(payload.get('cid', '')) > 64:
                raise ValueError('Invalid chunk entry correlation ID')
            if 'stopMode' in payload and payload['stopMode'] not in ('level', 'entry'):
                raise ValueError('Invalid stop mode')
            if 'fillPriority' in payload and (payload['fillPriority'] not in ('risk', 'volume')
                    or payload.get('accountExecution') != 'independent-v4'
                    or payload.get('executionPolicy') != 'hybrid-market-limit-v1'):
                raise ValueError('Invalid captured fill priority')
            if 'accountExecution' in payload and (payload['accountExecution'] not in ('independent-v2', 'independent-v3', 'independent-v4')
                    or payload.get('executionPolicy') != 'hybrid-market-limit-v1'):
                raise ValueError('Invalid account execution policy')
            mode = payload.get('entryMode', 'breakout')
            if mode not in ('breakout', 'zone', 'boundary', 'side-zone'):
                raise ValueError('Invalid entry mode')
            zone = mode in ('zone', 'boundary', 'side-zone')
            if 'executionPolicy' in payload and (payload['executionPolicy'] not in ('market-limit', 'hybrid-market-limit-v1') or mode != 'side-zone'):
                raise ValueError('Invalid market/limit execution policy')
            if payload.get('executionPolicy') == 'hybrid-market-limit-v1' and not 0 < number(payload.get('chunkPercent'), 'Hybrid chunk percent') <= 100:
                raise ValueError('Hybrid chunk percent must be greater than zero and at most 100')
            distance = entry_stop_distance(payload)
            if zone:
                radius = number(payload.get('zoneRadius'), 'Zone radius', positive=False)
                multiplier = number(payload.get('zoneMultiplier'), 'Zone multiplier')
                if radius < 0 or multiplier not in (1, 2) or payload['trigger'] - radius <= 0:
                    raise ValueError('Invalid market entry zone')
            gap = number(payload.get('maxGap'), 'Maximum gap', positive=False)
            if gap < 0:
                raise ValueError('Maximum gap cannot be negative')
            tp = number(payload.get('tp', 0), 'Take profit', positive=False)
            direction = 1 if payload['side'] == 'buy' else -1
            if mode == 'boundary' and abs(payload['trigger'] - (number(payload.get('level'), 'LT level') + direction * radius)) > 1e-8:
                raise ValueError('Market trigger must equal LT plus/minus play')
            if mode == 'side-zone' and abs(payload['trigger'] - number(payload.get('level'), 'LT level')) > 1e-8:
                raise ValueError('Market side zone must start at LT')
            if direction * (payload['trigger'] - payload['sl']) <= 0 or tp < 0 or (tp and direction * (tp - payload['trigger']) <= 0):
                raise ValueError('Fixed SL/TP must protect the breakout direction')
            if distance is not None and abs(direction * (payload['trigger'] - payload['sl']) - distance) > 1e-8:
                raise ValueError('Entry stop distance differs from the reference protection')
            lower, upper = market_price_bounds(payload) if zone else (payload['trigger'], payload['trigger'])
            favorable, adverse = (lower, upper) if direction == 1 else (upper, lower)
            if zone and ((distance is None and direction * (favorable - payload['sl']) <= 0)
                         or tp and direction * (tp - adverse) <= 0):
                raise ValueError('Market entry zone must stay between the fixed SL and TP')
            if any(key not in payload for key in DEADLINE_FIELDS):
                raise ValueError('An automatic exit deadline is required for chunk entry')
            self.bridge.validate_deadline(payload)
            active = [state for _, text in self._active_rows()
                      if (state := json.loads(text))['phase'] in ACTIVE]
            if any(row['symbol'] == payload['symbol'] and row['side'] == payload['side'] for row in active):
                raise ValueError('An entry in this direction is already active for this symbol; inspect it before arming another')
            if len(active) >= 4:
                raise ValueError('At most four active chunk entries are allowed')
            followers = self.bridge.requested_followers(payload) if payload.get('mirrorAccounts') or payload.get('mirrorAccount') else {}
            accounts = {}; snapshots = {}; quote_wait = None
            hybrid = payload.get('executionPolicy') == 'hybrid-market-limit-v1'
            captured_accounts = dict(((self.primary, 1.), *followers.items()))
            admission_reads = self._admission_reads(payload, captured_accounts)
            for account, multiplier in ((self.primary, 1.), *followers.items()):
                data, spec = admission_reads[account]
                if hybrid:
                    # Capture a cancellable, durable plan even when the last
                    # finite native tick is old. It is only a waiting intent:
                    # every execution round and final native guard still
                    # require a fresh price on every captured account.
                    data, bid, ask, point = self._quote_snapshot(account, data, allow_stale=True)
                    age = time.time()*1000 - data['quote']['utcTime']*1000
                    if age < -250 or age > 1000:
                        quote_wait = QuoteWait(account, data, 'admission').details
                else:
                    data, bid, ask, point = self._quote_snapshot(account, data)
                price = ask if direction == 1 else bid
                if not zone and direction * (price - payload['trigger']) >= 0:
                    raise ValueError('Account ' + account + ': trigger has already been crossed; choose a new trigger')
                metadata = data['metadata']
                tick = number(metadata.get('tickSize'), 'Price step')
                if distance is not None:
                    if abs(tick - payload['tickSize']) > 1e-8:
                        raise ValueError('Entry stop tick differs from the broker price step')
                    valid_units(distance, tick, 'Entry stop distance')
                for key in ('trigger', 'sl', 'tp'):
                    if payload.get(key):
                        valid_units(payload[key], tick, key)
                if payload.get('executionPolicy') in ('market-limit', 'hybrid-market-limit-v1'):
                    valid_units(lower, tick, 'Lower entry boundary')
                    valid_units(upper, tick, 'Upper entry boundary')
                for key in ('quantityStep', 'minQty', 'maxQty'):
                    number(spec.get(key), key)
                accounts[account] = dict(multiplier=multiplier, targetVolume=0, filledVolume=0., anchor=None,
                                         riskUsd=rounded(payload['riskUsd'] * multiplier), riskUsed=0., pointValue=point,
                                         spec=spec, parts=[])
                snapshots[account] = price
            chunk = payload['chunkVolume']
            if payload.get('executionPolicy') == 'hybrid-market-limit-v1':
                spec = accounts[self.primary]['spec']
                units = (Decimal(str(payload['volume']))*Decimal(str(payload['chunkPercent']))/Decimal('100')/Decimal(str(spec['quantityStep']))).to_integral_value(rounding=ROUND_FLOOR)
                percent_cap = max(spec['minQty'], float(units*Decimal(str(spec['quantityStep']))))
                if chunk > min(payload['volume'], percent_cap)+1e-8:
                    raise ValueError('Hybrid chunk volume exceeds its captured percentage')
            if zone:
                # Market mode fills the entire target; broker per-order limits
                # may require sequential market parts on the captured accounts.
                primary_step = accounts[self.primary]['spec']['quantityStep']
                cap = min(details['spec']['maxQty'] / details['multiplier'] for details in accounts.values())
                chunk = min(chunk, float((Decimal(str(cap)) / Decimal(str(primary_step)))
                    .to_integral_value(rounding=ROUND_FLOOR) * Decimal(str(primary_step))))
            plans = tranche_plan(payload['volume'], chunk, accounts,
                                 HYBRID_MAX_MARKET_PARTS if payload.get('executionPolicy') == 'hybrid-market-limit-v1' else 50)
            for account, parts in plans.items():
                accounts[account].update(parts=parts, targetVolume=rounded(sum(parts)))
                if distance is not None and sum(parts) * distance * accounts[account]['pointValue'] > accounts[account]['riskUsd'] + 1e-8:
                    raise ValueError('Entry stop target exceeds the captured account risk')
                if payload.get('executionPolicy') == 'hybrid-market-limit-v1' and distance is None and sum(parts)*direction*(adverse-payload['sl'])*accounts[account]['pointValue'] > accounts[account]['riskUsd']+1e-8:
                    raise ValueError('Hybrid level stop target exceeds the captured account risk')
            state = dict(id=identifier, symbol=payload['symbol'], side=payload['side'], phase='armed',
                         reason='Waiting for market entry boundary' if mode=='boundary' else 'Waiting for market entry zone' if zone else 'Waiting for breakout', trigger=payload['trigger'], volume=payload['volume'],
                         chunkVolume=chunk, maxGap=gap, riskUsd=payload['riskUsd'],
                         sl=payload['sl'], tp=tp, level=payload.get('level'), closeAtUtc=payload['closeAtUtc'],
                         accounts=accounts, nextChunk=0, childCount=len(plans[self.primary]), children=[],
                         triggered=False, previousPrice=snapshots[self.primary],
                         createdAt=time.time() * 1000, updatedAt=time.time() * 1000)
            for key in ('tickSize', 'stopMode', 'stopOffsetPts', 'chunkPercent', 'entryMode', 'zoneRadius', 'zoneMultiplier', 'executionPolicy', 'accountExecution', 'fillPriority', 'initialSizingOnly'):
                if key in payload:
                    state[key] = payload[key]
            if quote_wait is not None:
                state.update(phase='paused', reason='Broker quote is stale; entry is paused', quoteWait=quote_wait)
            first = self._child(payload, state, 0, payload['trigger'])
            self._admission_checks(payload, accounts, first, plans)
            self._expiry(payload)
            with self.storage.lock:
                try:
                    self.storage.db.execute('INSERT INTO chunk_entries VALUES (?,?,?,?)',
                                           (identifier, fingerprint, json.dumps(payload, allow_nan=False), json.dumps(state, allow_nan=False)))
                    self.storage.db.commit()
                except Exception:
                    self.storage.db.rollback()
                    raise
            self._index_committed(state)
            self._remember(state)
            return public_record(state)

    def _stopping(self, identifier):
        with self.stop_requests_lock:
            return self.stop_requests.get(identifier, 0) > 0

    def _rejected_limit_for_retry(self, state, account):
        """A definite opening refusal only; never a cancellation or SL refusal."""
        identifier = state['accounts'][account].get('limitRefusalChild')
        child = next((row for row in state['children'] if row['id']==identifier),None)
        if not child or child.get('type')!='limit' or child.get('state')!='rejected' or account not in child.get('volumes',{}):
            return None
        result = self._limit_results(state,child).get(account,{})
        if (result.get('status')!='rejected' or result.get('retcode')!=10016
                or result.get('providerOrderId') not in (None,0,'0','') or result.get('filledVolume',0) not in (None,0,0.)):
            return None
        return child

    def continue_account(self, request):
        with self.entry_controls.continuing(request):
            return self._continue_account(request)

    def _continue_account(self, request):
        """Explicit permission to check one lane; unknown intents are never replayed."""
        identifier, entry_id, account = (str(request.get(key, '')) for key in ('id','entryId','account'))
        if (not re.fullmatch(r'[A-Za-z0-9_.:-]{1,130}', identifier)
                or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,100}', entry_id)):
            raise ValueError('Invalid continuation command identity')
        with self.lock:
            self.entry_controls.check(request)
            found = self._load(entry_id)
            if not found:
                raise ValueError('Entry not found')
            _, payload, state = found
            fingerprint = digest(request)
            previous = state.get('continueCommands', {}).get(identifier)
            if previous:
                if previous != fingerprint:
                    raise ValueError('Continuation ID reused with different parameters')
                return public_record(state)
            self._expiry(request)
            if payload.get('accountExecution') != 'independent-v4' or account not in state['accounts']:
                raise ValueError('Per-account continuation requires a new independent account entry')
            user_stopped = (state['accounts'][account].get('userStopped') is True and
                state.get('hybridRemainderStop') in (None,'Further entry stopped by user; existing positions remain protected'))
            if (self._stopping(entry_id) or self._protecting(entry_id) or entry_id in self.parent_halts
                    or state.get('hybridRemainderStop') and not user_stopped or state.get('protectionPending') or state.get('closurePending')
                    or state.get('protectionPause') or time.time()*1000 >= payload['closeAtUtc']):
                raise ValueError('Entry stopped, expired or protection is being checked; continuation blocked')
            phase = state.get('lanePhases', {}).get(account, 'armed')
            details = state['accounts'][account]
            local_stopped = bool(details.get('laneStopReason'))
            retry_limit = self._rejected_limit_for_retry(state,account) if details.get('entryHalted') and details.get('limitRetryAvailable') is True else None
            if (phase == 'completed' or phase == 'stopped' and not (user_stopped or local_stopped) or details.get('entryHalted') and not retry_limit or details.get('commonStopUncertain')
                    or details.get('riskRepairRequired') or details['filledVolume'] >= details['targetVolume']-1e-8):
                raise ValueError('This account has completed or stopped entry; continuation blocked')
            key = (entry_id, account)
            future = self.lane_jobs.get(key)
            if future and not future.done():
                raise ValueError('This account is already checking or executing; wait for its result')
            if retry_limit:
                if any(account in child.get('volumes',{}) and (child['state'] in ('sending','unknown','pending')
                        or child.get('cancellations',{}).get(account,{}).get('status') in ('claimed','unknown')) for child in state['children']):
                    raise ValueError('Existing account intents must be settled before a refused limit can resume')
                proof = self._call(account,'receipt',retry_limit['id'])
                result = proof.get('result') or {}
                if (str(proof.get('account'))!=account or proof.get('server')!=self.bridge.config['server']
                        or result.get('status')!='rejected' or result.get('retcode')!=10016
                        or result.get('providerOrderId') not in (None,0,'0','')
                        or result.get('filledVolume',0) not in (None,0,0.)):
                    raise ValueError('Native journal did not confirm a definite unfilled limit refusal')
                self._independent_snapshot(payload,state,account)
            if local_stopped:
                if any(account in child.get('closedOpenings', {}) for child in state['children']):
                    raise ValueError('Captured opening already exited; start a new entry instead')
                # Explicit continuation acknowledges known fills only after a
                # fresh identity/SL/TP check. Unknown claims are never replayed.
                self._positions(payload, state, accounts={account})
                if details.get('stopCheckRejected'):
                    raise ValueError('Common stop rejection requires review before continuation')
            self.entry_controls.check(request)
            if self._stopping(entry_id):
                raise ValueError('Continuation superseded by an explicit stop')
            commands = state.setdefault('continueCommands', {})
            if len(commands) >= 100:
                raise ValueError('Continuation command limit reached')
            commands[identifier] = fingerprint
            if retry_limit:
                details.pop('entryHalted',None)
                details.pop('limitRefusalChild',None)
                details.pop('limitRetryAvailable',None)
                details['supportOptionalAfterRefusal'] = True
            if user_stopped:
                state.pop('hybridRemainderStop', None)
                details.pop('userStopped', None)
            if local_stopped:
                details.pop('laneStopReason', None)
                for child in state['children']:
                    if account in child.get('positions', {}) and child.get('fillGuardViolated'):
                        child['fillGuardAcknowledged'] = True
            uncertain = any(account in child['volumes'] and (child['state'] in ('sending','unknown','pending')
                or child.get('cancellations', {}).get(account, {}).get('status') in ('claimed','unknown'))
                for child in state['children'])
            state.setdefault('lanePhases', {})[account] = 'paused' if uncertain else 'running'
            if uncertain:
                state.setdefault('laneReconciliation', {})[account] = dict(status='checking',
                    startedAt=time.time()*1000, resumeAllowed=True, attempts=0,
                    reason='Explicit account continuation; checking exact existing broker intents')
            details.update(entryStatus='checking', entryReason='Checking existing volume before continuing this account')
            state.update(phase='running', reason='Explicit account continuation requested; original target and guards retained')
            self._save(state)
            self.lane_jobs.pop(key, None)
            self.wake_event.set()
            return public_record(state)

    @staticmethod
    def _protection_quote_wait(state):
        return (state.get('executionPolicy') == 'hybrid-market-limit-v1'
                and state.get('phase') in ACTIVE and state.get('protectionOnlyWait') is True
                and bool(state.get('hybridRemainderStop'))
                and state.get('quoteWait', {}).get('status') == 'waiting'
                and state.get('quoteWait', {}).get('stage') == 'common-stop'
                and not state.get('protectionPending') and not state.get('closurePending')
                and all(child['state'] in ('confirmed', 'cancelled', 'rejected') for child in state['children'])
                and not any(details.get('commonStopUncertain') for details in state['accounts'].values()))

    def stop(self, payload):
        identifier = str(payload.get('id', ''))
        entry_id = str(payload.get('entryId', ''))
        if not re.fullmatch(r'[A-Za-z0-9_.:-]{1,130}', identifier) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,100}', entry_id):
            raise ValueError('Invalid stop command or entry ID')
        fingerprint = digest(payload)
        with self.stop_requests_lock:
            if identifier in self.stop_digests and self.stop_digests[identifier] != fingerprint:
                raise ValueError('Stop command ID reused with different parameters')
        # An expired replay may read its saved result. A fresh stop raises its
        # in-memory flag BEFORE waiting for a broker call/coordinator lock.
        try:
            self._expiry(payload)
        except ValueError:
            with self.storage.lock:
                old = self.storage.db.execute('SELECT digest,result FROM chunk_entry_stops WHERE id=?', (identifier,)).fetchone()
            if old and old[0] == fingerprint:
                return public_record(json.loads(old[1]))
            raise
        with self.stop_requests_lock:
            if identifier in self.stop_digests and self.stop_digests[identifier] != fingerprint:
                raise ValueError('Stop command ID reused with different parameters')
            self.stop_digests[identifier] = fingerprint
            self.stop_requests[entry_id] = self.stop_requests.get(entry_id, 0) + 1
        try:
            self.entry_controls.revoke(payload)
            account_stops = self._begin_account_stops(identifier, entry_id, payload)
            self.entry_controls.drain(entry_id)
            if account_stops:
                wait(account_stops)
            self._drain_lanes(entry_id)
            with self.lock:
                return self._stop_locked(identifier, fingerprint, entry_id, payload)
        finally:
            with self.stop_requests_lock:
                count = self.stop_requests[entry_id] - 1
                if count:
                    self.stop_requests[entry_id] = count
                else:
                    del self.stop_requests[entry_id]

    def _begin_account_stops(self, identifier, entry_id, payload):
        return []

    def _stop_locked(self, identifier, fingerprint, entry_id, payload):
        # The priority protocol validates expiry at admission in stop(). Once
        # accepted, a slow read must not undo the operator's cancellation.
        def deadline():
            if 'cancelContinueIds' not in payload: self._expiry(payload)
        with self.storage.lock:
            old = self.storage.db.execute('SELECT digest,result FROM chunk_entry_stops WHERE id=?', (identifier,)).fetchone()
        if old:
            if old[0] != fingerprint:
                raise ValueError('Stop command ID reused with different parameters')
            return public_record(json.loads(old[1]))
        found = self._load(entry_id)
        if not found:
            # The arm may still be travelling over a different HTTP connection.
            # Cancellation must win even if no parent has been admitted yet.
            deadline()
            state = self._cancelled(entry_id) or dict(id=entry_id, phase='stopped',
                reason='Entry cancelled before admission; a delayed arm will not be started',
                accounts={}, children=[], cancelledBeforeAdmission=True,
                createdAt=time.time() * 1000, updatedAt=time.time() * 1000)
            with self.storage.lock:
                try:
                    self.storage.db.execute('INSERT INTO chunk_entry_stops VALUES (?,?,?)', (identifier, fingerprint, json.dumps(state)))
                    self.storage.db.execute('INSERT OR IGNORE INTO chunk_entry_cancels VALUES (?,?)', (entry_id, identifier))
                    self.storage.db.commit()
                except Exception:
                    self.storage.db.rollback()
                    raise
            self._remember(state)
            return public_record(state)
        state = found[2]
        hybrid = found[1].get('executionPolicy') == 'hybrid-market-limit-v1'
        if hybrid:
            deadline()
            if state.get('reconciliation', {}).get('status') == 'checking':
                state['reconciliation']['resumeAllowed'] = False
            # The explicit cancellation remains durable even when its common
            # SL must wait for a fresh native price. No future tick can acquire.
            state.setdefault('hybridRemainderStop', 'Further entry stopped by user; existing positions remain protected')
            self._save(state)
        if (any(child.get('type') == 'limit' and child['state'] not in ('confirmed', 'cancelled', 'rejected') for child in state['children'])
                or hybrid and (state.get('protectionOnlyWait') or state.get('quoteWait', {}).get('stage') == 'common-stop')):
            deadline()
            safe = self._cancel_limits(found[1], state, identifier)
            waiting = safe and state.get('protectionOnlyWait')
            state.update(phase='paused' if waiting else 'stopped' if safe else 'unknown',
                         reason=HYBRID_STOP_QUOTE_WAIT if waiting else
                                'Остаток лимитного входа отменён; открытые позиции сохранены' if safe else
                                'Отмена лимитного входа не подтверждена; повторный вход заблокирован')
        if state['phase'] in ACTIVE:
            deadline()
            waiting = state.get('protectionOnlyWait')
            state.update(phase='paused' if waiting else 'stopped',
                         reason=HYBRID_STOP_QUOTE_WAIT if waiting else 'Further entry stopped by user; existing positions remain protected',
                         updatedAt=time.time() * 1000)
        if hybrid and state.get('reconciliation', {}).get('status') == 'checking':
            if all(child['state'] in ('confirmed','cancelled','rejected') for child in state['children']):
                state['reconciliation'].update(status='confirmed', checkedAt=time.time()*1000)
            else:
                state.update(phase='paused', reason='Further entry stopped; checking exact broker execution')
        if found[1].get('accountExecution') in ('independent-v4','independent-v5'):
            state['lanePhases'] = {account:'stopped' for account in state['accounts']}
            if state['phase']=='stopped' and state.get('hybridRemainderStop') == 'Further entry stopped by user; existing positions remain protected':
                for details in state['accounts'].values():
                    details['userStopped'] = True
        with self.storage.lock:
            try:
                self.storage.db.execute('UPDATE chunk_entries SET state=? WHERE id=?', (json.dumps(state), state['id']))
                self.storage.db.execute('INSERT INTO chunk_entry_stops VALUES (?,?,?)', (identifier, fingerprint, json.dumps(state)))
                self.storage.db.execute('INSERT OR IGNORE INTO chunk_entry_cancels VALUES (?,?)', (entry_id, identifier))
                self.storage.db.commit()
            except Exception:
                self.storage.db.rollback()
                raise
        self._index_committed(state)
        self._remember(state)
        return public_record(state)

    def _limit_results(self, state, child):
        result = child.get('reconciledResult', child.get('result', {}))
        return result.get('accountResults', {self.primary: result} if len(state['accounts']) == 1 else {})

    def _limit_view(self, payload, account, outcome):
        ticket = str(outcome.get('providerOrderId', ''))
        if not re.fullmatch(r'[1-9][0-9]{0,19}', ticket):
            raise ValueError('Номер лимитной заявки не подтверждён; повторная отправка запрещена')
        self._account(account)
        view = self._call(account, 'entry_limit_state', ticket, payload['symbol'])
        if str(view.get('account')) != account:
            raise ValueError('Limit status account mismatch')
        return ticket, view

    def _cancel_limits(self, payload, state, stop_id):
        """Cancel only captured pending tickets, with a durable intent per account."""
        if payload.get('executionPolicy') == 'hybrid-market-limit-v1':
            try:
                return self._hybrid_cancel_limits(payload, state)
            except QuoteWait as error:
                # Explicit cancellation must remain available when an unused
                # common-stop request merely waits for a fresh tick. Known
                # positions retain their already verified native SLs; no
                # acquisition resumes after this stop command.
                state['quoteWait'] = error.details
                return not any(details.get('commonStopUncertain') or details.get('riskRepairRequired') and not state.get('protectionOnlyWait')
                               for details in state['accounts'].values())
        safe = True
        for child in state['children']:
            if child.get('type') != 'limit' or child['state'] in ('confirmed', 'cancelled', 'rejected'):
                continue
            results = self._limit_results(state, child)
            for account in state['accounts']:
                try:
                    outcome = results.get(account, {})
                    if outcome.get('status') == 'rejected':
                        continue
                    ticket, view = self._limit_view(payload, account, outcome)
                    pending = view.get('pending', [])
                    cancellation = child.setdefault('cancellations', {}).get(account)
                    if cancellation and cancellation.get('status') in ('unknown', 'claimed'):
                        safe = False
                        continue
                    if pending:
                        row = pending[0]
                        if str(row['ticket']) != ticket or row['symbol'] != payload['symbol'] or row['type'] != payload['side']:
                            raise ValueError('Pending cancellation target changed')
                        if cancellation is not None:
                            safe = False  # A claimed cancel is never sent twice.
                            continue
                        command_id = 'entry-cancel:' + hashlib.sha256((stop_id + ':' + child['id'] + ':' + account).encode()).hexdigest()
                        child['cancellations'][account] = dict(status='claimed', id=command_id)
                        self._save(state)
                        request = dict(id=command_id, ticket=ticket, symbol=payload['symbol'], expiresAt=time.time()*1000+3000)
                        try:
                            result = self._call(account, 'execute', 'cancel', request)['result']
                        except Exception as error:
                            result = dict(status='unknown', reason=str(error))
                        child['cancellations'][account] = copy.deepcopy(result)
                        self._save(state)
                        if result.get('status') == 'unknown':
                            safe = False
                            continue
                        _, view = self._limit_view(payload, account, outcome)
                    if view.get('pending') or (not view.get('positions') and view.get('terminal', {}).get('phase') not in ('closed', 'cancelled')):
                        safe = False
                    # A fill can win the cancellation race. Expose that position
                    # before acknowledging stop, so the card never unlocks as empty.
                    if view.get('positions'):
                        row = view['positions'][0]
                        volume = number(row['lots'], 'Filled volume at cancellation')
                        price = number(row['open_price'], 'Fill at cancellation')
                        distance = entry_stop_distance(payload)
                        direction = 1 if payload['side']=='buy' else -1
                        stop = rounded(child['price']-direction*distance) if distance is not None else payload['sl']
                        if (row['symbol'] != payload['symbol'] or row['type'] != payload['side']
                                or abs(float(row.get('sl', 0))-stop)>1e-8 or abs(float(row.get('tp', 0))-payload.get('tp', 0))>1e-8
                                or volume > child['volumes'][account]+1e-8 or direction*(price-stop)<=0):
                            raise ValueError('Filled position changed during cancellation')
                        risk = rounded(volume*direction*(price-stop)*state['accounts'][account]['pointValue'])
                        old = child.setdefault('accounted', {}).get(account, dict(volume=0., risk=0.))
                        details = state['accounts'][account]
                        details['filledVolume'] = rounded(details['filledVolume']+volume-old['volume'])
                        details['riskUsed'] = rounded(details['riskUsed']+risk-old['risk'])
                        child['accounted'][account] = dict(volume=volume, risk=risk)
                        child.setdefault('positions', {})[account] = dict(ticket=str(row['ticket']), openingTicket=ticket,
                            volume=volume, fillPrice=price, sl=stop, tp=payload.get('tp', 0))
                except Exception:
                    safe = False
            if safe:
                child['state'] = 'cancelled'
        return safe

    def _tick_limit(self, payload, state, child):
        if self._stopping(state['id']) or time.time()*1000 >= payload['closeAtUtc']:
            safe = self._cancel_limits(payload, state, state['id'] + ':end')
            self._phase(state, 'stopped' if safe else 'unknown',
                        'Остаток лимитного входа отменён; позиции сохранены' if safe else 'Отмена лимитного входа не подтверждена')
            return
        now = time.monotonic()
        if now < self.pending_poll_at.get(state['id'], 0):
            return
        self.pending_poll_at[state['id']] = now + .25
        results = self._limit_results(state, child)
        direction = 1 if payload['side'] == 'buy' else -1
        updates = {}; ended = False; violation = False
        try:
            self._positions(payload, state)
            for account, details in state['accounts'].items():
                ticket, view = self._limit_view(payload, account, results.get(account, {}))
                pending, positions = view.get('pending', []), view.get('positions', [])
                if len(pending)>1 or len(positions)>1:
                    raise ValueError('Состояние лимитного входа неоднозначно')
                row = positions[0] if positions else None
                volume = number(row['lots'], 'Limit filled volume') if row else 0.
                remaining = number(pending[0]['lots'], 'Pending remainder') if pending else 0.
                previous = child.get('positions', {}).get(account, {})
                if volume + 1e-8 < previous.get('volume', 0) or volume + remaining > child['volumes'][account] + 1e-8:
                    raise ValueError('Открытый объём изменился; проверь счета')
                distance = entry_stop_distance(payload)
                stop = rounded(child['price']-direction*distance) if distance is not None else payload['sl']
                if pending:
                    order = pending[0]
                    if (str(order['ticket']) != ticket or order['type'] != payload['side'] or order['symbol'] != payload['symbol']
                            or abs(float(order['open_price'])-child['price'])>1e-8
                            or abs(float(order.get('sl', 0))-stop)>1e-8
                            or abs(float(order.get('tp', 0))-payload.get('tp', 0))>1e-8
                            or abs(volume+remaining-child['volumes'][account])>1e-8):
                        raise ValueError('Лимитная заявка или её защита изменены')
                if row:
                    guard = child['guards'][account]
                    price = number(row['open_price'], 'Actual limit entry price')
                    if (row['type'] != payload['side'] or row['symbol'] != payload['symbol']
                            or abs(float(row.get('sl', 0))-stop)>1e-8 or abs(float(row.get('tp', 0))-payload.get('tp', 0))>1e-8
                            or direction*(price-stop)<=0):
                        raise ValueError('Защита открытой позиции изменилась')
                    risk = rounded(volume*direction*(price-stop)*details['pointValue'])
                    violation |= fill_outside_zone(payload, price, guard['priceFloor'], guard['priceCeiling'])
                    position = dict(ticket=str(row['ticket']), openingTicket=ticket, volume=volume, fillPrice=price, sl=stop, tp=payload.get('tp', 0))
                else:
                    risk, position = 0., None
                if not pending and volume < child['volumes'][account]-1e-8:
                    if not row and view.get('terminal', {}).get('phase') not in ('closed', 'cancelled'):
                        raise ValueError('Брокер не подтвердил результат лимитного входа')
                    ended = True
                updates[account] = (volume, risk, position)
            progress_changed = False
            for account, (volume, risk, position) in updates.items():
                details = state['accounts'][account]
                old = child.setdefault('accounted', {}).get(account, dict(volume=0., risk=0.))
                progress_changed |= abs(volume-old['volume'])>1e-8 or abs(risk-old['risk'])>1e-8
                details['filledVolume'] = rounded(details['filledVolume'] + volume - old['volume'])
                details['riskUsed'] = rounded(details['riskUsed'] + risk - old['risk'])
                child['accounted'][account] = dict(volume=volume, risk=risk)
                if position:
                    child.setdefault('positions', {})[account] = position
                    if details['anchor'] is None:
                        details['anchor'] = position['fillPrice']
                violation |= details['riskUsed'] > details['riskUsd']+1e-8 or details['filledVolume'] > details['targetVolume']+1e-8
            if ended or violation:
                child['fillGuardViolated'] = violation
                safe = self._cancel_limits(payload, state, state['id'] + ':halt')
                self._phase(state, 'stopped' if safe else 'unknown', 'Лимитный вход завершён не полностью или нарушены границы; остаток остановлен, позиции сохранены')
            elif all(volume >= child['volumes'][account]-1e-8 for account, (volume, _, _) in updates.items()):
                child['state'] = 'confirmed'
                state['nextChunk'] += 1
                self._phase(state, 'completed' if state['nextChunk'] == state['childCount'] else 'running', 'Лимитный вход подтверждён на всех счетах')
            else:
                self._phase(state, 'running', 'Лимитная заявка ждёт исполнения; дополнительный рынок заблокирован')
                if progress_changed:
                    self._save(state)
        except Exception as error:
            # A native remainder can execute without another coordinator tick.
            # Stop that owned remainder on an uncertain/changed position too;
            # cancellation uncertainty never removes the parent lock.
            self._cancel_limits(payload, state, state['id'] + ':uncertain')
            self._phase(state, 'unknown', str(error) + '; повторная отправка запрещена')

    def list(self):
        with self.public_lock:
            rows = sorted(self.public_records.values(), key=self._public_rank)[:30]
            selected = []; size = 2
            for _, value, encoded_size in rows:
                needed = encoded_size + bool(selected)
                if value['phase'] in ACTIVE or size + needed <= PUBLIC_LIST_BUDGET:
                    selected.append(copy.deepcopy(value))
                    size += needed
            return [self.progress_observer.decorate(row) for row in selected] if self.progress_observer else selected

    def get(self, identifier):
        """Read one exact durable parent without admission, mutation or retry."""
        if not isinstance(identifier, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,100}', identifier):
            raise ValueError('Invalid chunk entry ID')
        with self.lock:
            found = self._load(identifier)
            if found:
                record = public_record(found[2])
                return self.progress_observer.decorate(record) if self.progress_observer else record
            cancelled = self._cancelled(identifier)
            return public_record(cancelled) if cancelled is not None else None

    def _positions(self, payload, state, accounts=None, evidence=None):
        for account in state['accounts']:
            if accounts is not None and account not in accounts:
                continue
            expected = [(child, child['positions'][account]) for child in state['children']
                        if account in child.get('positions', {}) and not child['positions'][account].get('closed')
                        and account not in child.get('closedOpenings', {}) and (child['state'] == 'confirmed'
                            or payload.get('executionPolicy') == 'hybrid-market-limit-v1')]
            if not expected:
                continue
            watch = [dict(account=account, ticket=position['openingTicket'], symbol=payload['symbol']) for _, position in expected]
            data = (evidence or {}).get(account)
            cached=getattr(self,'protection_proofs',{}).pop(account,None)
            if data is None and cached is not None:
                entry_id,signature,proof=cached
                if (entry_id==state['id'] and signature==digest(dict(positions=[p for _,p in expected]))
                        and proof.get('account')==account and proof.get('server')==self.bridge.config['server']
                        and 0<=time.time()*1000-proof.get('checkedAt',0)<=250):
                    data=proof
            if data is None:data=self._call(account,
                'entry_positions' if payload.get('initialSizingOnly') is True and hasattr(self,'lane_account') else 'entry_management',watch)
            if str(data.get('account')) != account:
                raise ValueError('Management account mismatch')
            for child, position in expected:
                row = next((row for row in data.get('orders', []) if row.get('__isPosition') and row.get('symbol') == payload['symbol']
                            and str(row.get('ticket')) == position['ticket']), None)
                terminal = data.get('openingStates', {}).get(position['openingTicket'], {})
                proof = terminal.get('openingProof', {})
                if (not row and terminal.get('phase') == 'closed' and proof.get('kind') == 'exact-balanced-opening-v1'
                        and proof.get('openingTicket') == position['openingTicket'] and proof.get('positionId') == position['ticket']
                        and proof.get('symbol') == payload['symbol'] and proof.get('side') == payload['side']
                        and abs(number(proof.get('volume'), 'Closed volume')-position['volume']) <= 1e-8
                        and abs(number(proof.get('fillPrice'), 'Closed fill price')-position['fillPrice']) <= 1e-8):
                    child.setdefault('closedOpenings', {})[account] = dict(openingTicket=position['openingTicket'], positionId=position['ticket'])
                    state['hybridRemainderStop'] = 'Captured opening is confirmed fully closed; remainder stopped'
                    self._hybrid_recount(payload, state, allow_excess_risk=True)
                    self._save(state)
                    raise ValueError(state['hybridRemainderStop'])
                if not row or row.get('type') != payload['side'] or abs(float(row['lots']) - position['volume']) > 1e-8:
                    if not row:
                        state['missingNativeChild'] = child['id']
                    raise ValueError('An entry position was closed or changed; further entry stopped')
                if any(abs(float(row.get(key, 0)) - float(position.get(key, payload.get(key, 0)))) > 1e-8 for key in ('sl', 'tp')):
                    raise ValueError('An entry position SL/TP was changed; further entry stopped')
            if payload.get('initialSizingOnly') is True:
                self.position_proof = getattr(self,'position_proof',{})
                self.position_proof[account] = (time.monotonic(),digest(dict(positions=[position for _,position in expected])))

    def _confirmed_position(self, payload, state, child, account, outcome):
        """Validate one exact full fill, with the same proof for every policy."""
        volume = number(outcome.get('filledVolume'), 'Confirmed fill volume')
        price = number(outcome.get('fillPrice'), 'Confirmed fill price')
        opening = str(outcome.get('providerOrderId', ''))
        if outcome.get('status') != 'ok' or abs(volume - child['volumes'][account]) > 1e-8 or not re.fullmatch(r'[1-9][0-9]{0,19}', opening):
            raise ValueError('A tranche fill does not match the requested volume; inspect every account')
        cached=getattr(self,'opening_proofs',{}).pop((child['id'],account),None)
        data=None
        if (isinstance(cached,dict) and cached.get('schema')=='entry-opening-proof-v1'
                and cached.get('commandId')==child['id'] and str(cached.get('account'))==account
                and cached.get('server')==self.bridge.config['server']
                and type(cached.get('checkedAt')) in (int,float)
                and 0<=time.time()*1000-cached['checkedAt']<=250):
            data=cached
        if data is None:
            data = self._call(account,'entry_positions' if payload.get('initialSizingOnly') is True else 'entry_management',
                [dict(account=account, ticket=opening, symbol=payload['symbol'])])
        if str(data.get('account')) != account:
            raise ValueError('Management account mismatch after entry')
        resolved = data.get('openingStates', {}).get(opening, {})
        ticket = str(resolved.get('ticket', opening))
        row = next((row for row in data.get('orders', []) if row.get('__isPosition') and row.get('symbol') == payload['symbol']
                    and (str(row.get('ticket')) == ticket or opening in (str(row.get('ticket')), str(row.get('identifier'))))), None)
        closed = resolved.get('openingProof', {}) if resolved.get('phase') == 'closed' else {}
        closed_row = (not row and closed.get('kind') == 'exact-balanced-opening-v1'
            and closed.get('openingTicket') == opening and closed.get('symbol') == payload['symbol']
            and closed.get('side') == payload['side'] and re.fullmatch(r'[1-9][0-9]{0,19}', str(closed.get('positionId', '')))
            and abs(number(closed.get('volume'), 'Historical entry volume')-volume) <= 1e-8
            and abs(number(closed.get('fillPrice'), 'Historical entry price')-price) <= 1e-8)
        if closed_row:
            row = dict(ticket=closed['positionId'], lots=closed['volume'], type=closed['side'],
                       sl=closed.get('sl'), tp=closed.get('tp'))
        if not row or abs(float(row['lots']) - volume) > 1e-8 or row.get('type') != payload['side']:
            raise ValueError('Confirmed entry position is not visible or changed; inspect every account')
        hybrid = payload.get('executionPolicy') == 'hybrid-market-limit-v1'
        expected_sl = self._hybrid_stop(payload, state, account, child.get('prices', {}).get(account, price)) if hybrid else payload['sl']
        expected_tp = self._hybrid_tp(payload, state, account) if hybrid else payload.get('tp', 0)
        distance = self._hybrid_distance(payload, state, account) if hybrid else entry_stop_distance(payload)
        if distance is not None:
            submitted_price = number(outcome.get('submittedPrice'), 'Submitted entry price')
            expected_sl = rounded(submitted_price - (1 if payload['side'] == 'buy' else -1) * distance)
            if abs(number(outcome.get('sl'), 'Submitted entry stop') - expected_sl) > 1e-8:
                raise ValueError('Submitted protection differs from the entry stop distance')
        if abs(float(row.get('sl', 0)) - expected_sl) > 1e-8 or abs(float(row.get('tp', 0)) - expected_tp) > 1e-8:
            raise ValueError('Entry protection differs from the fixed SL/TP; inspect every account')
        position=dict(ticket=str(row['ticket']), openingTicket=opening, volume=volume, fillPrice=price,
                      sl=expected_sl, tp=expected_tp, **({'closedAtBroker': True} if closed_row else {}))
        if payload.get('initialSizingOnly') is True and not closed_row:
            captured=[item['positions'][account] for item in state['children']
                if account in item.get('positions',{}) and not item['positions'][account].get('closed')
                and account not in item.get('closedOpenings',{})]+[position]
            rows={str(value['ticket']):value for value in data.get('orders',[]) if value.get('__isPosition') and value.get('symbol')==payload['symbol']}
            if all(str(value['ticket']) in rows and rows[str(value['ticket'])].get('type')==payload['side']
                   and all(abs(float(rows[str(value['ticket'])].get(native_key,-1))-value[key])<=1e-8
                       for key,native_key in (('volume','lots'),('fillPrice','open_price'),('sl','sl'),('tp','tp'))) for value in captured):
                self.position_proof=getattr(self,'position_proof',{})
                self.position_proof[account]=(time.monotonic(),digest(dict(positions=captured)))
        return position

    def _confirm(self, payload, state, child, result):
        accounts = child['volumes'] if payload.get('executionPolicy') == 'hybrid-market-limit-v1' else state['accounts']
        results = result.get('accountResults')
        if results is None and len(state['accounts']) == 1:
            results = {self.primary: result}
        definite_parallel = (parallel_accounts(payload) and isinstance(results, dict) and set(results) == set(accounts)
                             and all(row.get('status') in ('ok', 'rejected') for row in results.values()))
        if (result.get('status') != 'ok' and not definite_parallel) or not isinstance(results, dict) or set(results) != set(accounts):
            raise ValueError('A tranche was rejected, partial or unconfirmed on an account; further entry stopped on every account')
        if definite_parallel:
            accounts = [account for account in accounts if results[account]['status'] == 'ok']
            if not accounts:
                raise ValueError('No confirmed market execution')
        positions = {}; fills = {}
        for account in accounts:
            positions[account] = self._confirmed_position(payload, state, child, account, results[account])
            fills[account] = (positions[account]['volume'], positions[account]['fillPrice'])
        for account, (volume, price) in fills.items():
            details = state['accounts'][account]
            details['anchor'] = price if details['anchor'] is None else details['anchor']
            details['filledVolume'] = rounded(details['filledVolume'] + volume)
            distance = (price - positions[account]['sl']) * (1 if payload['side'] == 'buy' else -1)
            if distance <= 0:
                raise ValueError('Actual fill is beyond the fixed stop; inspect positions')
            details['riskUsed'] = rounded(details['riskUsed'] + volume * distance * details['pointValue'])
        child.update(state='confirmed', positions=positions)
        for account, position in positions.items():
            if position.get('closedAtBroker'):
                child.setdefault('closedOpenings', {})[account] = dict(openingTicket=position['openingTicket'], positionId=position['ticket'])
                state['hybridRemainderStop'] = 'Captured opening is confirmed fully closed; remainder stopped'
        state['nextChunk'] += 1
        direction = 1 if payload['side'] == 'buy' else -1
        guard_violation = any(direction * (price - child['guards'][account]['anchor']) > child['guards'][account]['maxGap'] + 1e-8
                              or direction * (price - child['guards'][account]['maxRiskPrice']) > 1e-8
                              or fill_outside_zone(payload, price, child['guards'][account].get('priceFloor', price), child['guards'][account].get('priceCeiling', price))
                              for account, (_, price) in fills.items())
        if guard_violation and (fill_priority(payload) or all(weighted_nasdaq_stop(payload,state['accounts'][account]) for account in fills)):
            child['actualPriceWarning'] = 'Actual fill differs from captured entry price; weighted stop at captured panel distance required'
            guard_violation = False
        child['fillGuardViolated'] = guard_violation
        if guard_violation:
            self._phase(state, 'stopped', 'Actual execution exceeded the allowed price gap or remaining risk; further entry stopped, existing positions retained')
        elif any(details['filledVolume'] > details['targetVolume'] + 1e-8
                 or details['riskUsed'] > details['riskUsd'] + 1e-8 and not fill_priority(payload) and not weighted_nasdaq_stop(payload,details)
                 for details in state['accounts'].values()):
            self._phase(state, 'stopped', 'Actual execution exceeded the planned volume or risk; further entry stopped')
        elif state['nextChunk'] == state['childCount'] and payload.get('executionPolicy') != 'hybrid-market-limit-v1':
            self._phase(state, 'completed', 'All tranches confirmed on every account')
        elif self._stopping(state['id']):
            self._phase(state, 'stopped', 'Further entry stopped by user; existing positions remain protected')
        else:
            self._phase(state, 'running', 'Tranche confirmed; checking the next entry')

    def _hybrid_recount(self, payload, state, allow_excess_risk=False):
        direction = 1 if payload['side'] == 'buy' else -1
        for account, details in state['accounts'].items():
            positions = [child['positions'][account] for child in state['children'] if account in child.get('positions', {}) and not child['positions'][account].get('closed')]
            if len({row['ticket'] for row in positions}) != len(positions):
                raise ValueError('Hybrid position was accounted more than once')
            if len(positions) > HYBRID_MAX_POSITIONS:
                raise ValueError('Hybrid entry exceeds its owned position limit')
            details['filledVolume'] = rounded(sum(row['volume'] for row in positions))
            details['riskUsed'] = rounded(sum(row['volume'] * max(0., direction * (row['fillPrice'] - row['sl'])) * details['pointValue'] for row in positions))
            # Keep historical acquired volume/risk for the public receipt, but
            # an exact balanced-history exit has no remaining live exposure.
            live = [child['positions'][account] for child in state['children']
                    if account in child.get('positions', {}) and not child['positions'][account].get('closed')
                    and account not in child.get('closedOpenings', {})]
            live_risk = sum(row['volume'] * max(0., direction * (row['fillPrice'] - row['sl'])) * details['pointValue'] for row in live)
            details['riskRepairRequired'] = live_risk > details['riskUsd'] + 1e-8 and not fill_priority(payload)
            weighted_repair = weighted_nasdaq_stop(payload,details)
            if positions:
                details['anchor'] = details['anchor'] or positions[0]['fillPrice']
            if details['riskRepairRequired'] and not weighted_repair:
                # A real broker gap is already a fill. Journal it before
                # cancelling the remainder and tightening the common stop;
                # rejecting this measurement would skip that repair.
                state['hybridRemainderStop'] = 'Actual hybrid fill exceeded protected risk; remainder stopped'
            if (details['filledVolume'] > details['targetVolume'] + 1e-8
                    or details['riskRepairRequired'] and not allow_excess_risk and not weighted_repair):
                raise ValueError('Hybrid filled volume or protected risk exceeds the captured budget')

    @staticmethod
    def _hybrid_distance(payload, state, account):
        return None if 'sl' in state['accounts'][account].get('protectionOverride', {}) else entry_stop_distance(payload)

    @staticmethod
    def _hybrid_tp(payload, state, account):
        return state['accounts'][account].get('protectionOverride', {}).get('tp', payload.get('tp', 0))

    def _hybrid_stop(self, payload, state, account, price):
        override = state['accounts'][account].get('protectionOverride', {})
        if 'sl' in override:
            return override['sl']
        distance = entry_stop_distance(payload)
        return rounded(price-(1 if payload['side'] == 'buy' else -1)*distance) if distance is not None else payload['sl']

    @staticmethod
    def _hybrid_cancel_id(child, account):
        attempt = child.get('cancelAttempts', {}).get(account, 0)
        suffix = ':r'+str(attempt) if attempt else ''
        return 'hybrid-cancel:'+hashlib.sha256((child['id']+':'+account+suffix).encode()).hexdigest()

    def _hybrid_parallel_cancel(self, payload, state, child, results):
        """Only exact two-account pending openings use the existing warm pool.

        Account workers return data only. This coordinator alone journals the
        two claims before dispatch and every receipt after joining both calls.
        No market reservation or common protection is released by this helper.
        """
        warm = getattr(self.bridge, 'warm_group_executor', None)
        if (not callable(warm) or child.get('state') != 'pending' or child.get('positions')
                or child.get('cancellations') or len(state['accounts']) != 2 or self.primary not in state['accounts']
                or set(results) != set(state['accounts'])
                or any(not isinstance(row, dict) or row.get('status') != 'ok' for row in results.values())):
            return None
        views = {}
        try:
            for account in state['accounts']:
                ticket, view = self._limit_view(payload, account, results[account])
                pending, positions = view.get('pending'), view.get('positions')
                if (not isinstance(pending, list) or len(pending) != 1
                        or not isinstance(positions, list) or positions):
                    return None
                row = pending[0]
                if (str(row.get('ticket')) != ticket or row.get('symbol') != payload['symbol']
                        or row.get('type') != payload['side']
                        or abs(number(row.get('lots'), 'Pending volume')-child['volumes'][account]) > 1e-8
                        or abs(number(row.get('open_price'), 'Pending price')-child['prices'][account]) > 1e-8
                        or abs(number(row.get('sl'), 'Pending stop')-child['stops'][account]) > 1e-8
                        or abs(number(row.get('tp', 0), 'Pending take', positive=False)-child.get('takes', {}).get(account, payload.get('tp', 0))) > 1e-8):
                    return None
                views[account] = ticket
            executor = warm()
        except Exception:
            # No claim/send exists yet. Preserve the original strict serial
            # path, including cancellation of an independently safe account.
            return None
        requests = {account: dict(id=self._hybrid_cancel_id(child, account),
            ticket=ticket, symbol=payload['symbol'], expiresAt=time.time()*1000+3000)
            for account, ticket in views.items()}
        child['cancellations'] = {account: dict(status='claimed', id=request['id'])
                                  for account, request in requests.items()}
        child['cancelIds'] = {account: request['id'] for account, request in requests.items()}
        self._save(state)
        # Snapshot only immutable arguments. Neither native task can mutate a
        # parent field, write its journal, or release a reserved market part.
        command = copy.deepcopy(payload)
        outcomes = copy.deepcopy(results)
        follower = next(account for account in state['accounts'] if account != self.primary)
        dispatched = getattr(self.bridge.linked[follower], 'call_dispatched', None)
        ready, abort = threading.Event(), threading.Event()
        deadline = time.monotonic()+.25
        handoff = dict(sent=False)
        def sent():
            handoff['sent'] = True
            handoff['onTime'] = time.monotonic() <= deadline and not abort.is_set()
            ready.set()
        def cancel_one(account):
            errors = []
            try:
                if account == follower and callable(dispatched):
                    receipt = dispatched(sent, abort, deadline, 'execute', 'cancel', requests[account])['result']
                else:
                    receipt = self._call(account, 'execute', 'cancel', requests[account])['result']
                if not isinstance(receipt, dict):
                    raise ValueError('Cancellation receipt is not an object')
            except Exception as error:
                receipt = (dict(status='rejected', dispatchNotSent=True, reason=str(error),workerContention=getattr(error,'rpc_diagnostics',{}))
                    if getattr(error, 'dispatch_not_sent', False) else dict(status='unknown', reason=str(error)))
            finally:
                if account == follower:
                    ready.set()
            if receipt.get('status') not in ('ok', 'rejected'):
                errors.append('Limit cancellation result is unknown; market reservation remains locked')
            try:
                ticket, view = self._limit_view(command, account, outcomes[account])
            except Exception as error:
                ticket, view = views[account], None
                errors.append(str(error))
            return dict(ticket=ticket, view=view, receipt=hybrid_receipt(receipt), errors=errors)
        try:
            future = executor.submit(cancel_one, follower)
        except Exception as error:
            # A failed dispatch cannot prove whether a custom executor queued
            # work. Durable claims remain unresolved; never replay either ID.
            raise ValueError('Parallel cancellation dispatch is unconfirmed; no automatic retry') from error
        # The real worker confirms its actual pipe handoff before a primary
        # MT5 extension can hold the GIL. Legacy workers retain normal calls.
        if callable(dispatched) and (not ready.wait(max(0., deadline-time.monotonic()))
                or not handoff['sent'] or not handoff.get('onTime')):
            abort.set()
        resolved = {}
        try:
            resolved[self.primary] = cancel_one(self.primary)
        except Exception as error:
            resolved[self.primary] = dict(ticket=views[self.primary], view=None,
                receipt=dict(status='unknown', reason=str(error)), errors=[str(error)])
        try:
            resolved[follower] = future.result()
        except Exception as error:
            resolved[follower] = dict(ticket=views[follower], view=None,
                receipt=dict(status='unknown', reason=str(error)), errors=[str(error)])
        # Always join BOTH accounts before the coordinator publishes receipts,
        # captures a fill race or permits any common-stop/next-market work.
        for account in state['accounts']:
            child['cancellations'][account] = resolved[account]['receipt']
        self._save(state)
        return resolved

    def _observe_limit_price(self, payload, state, child, account, ticket, view):
        """Use existing native rows only; metadata never controls execution."""
        changed = False
        try:
            candidates = [row.get('brokerOrderEvidence') for row in view.get('pending', [])]
            candidates.append(view.get('terminal', {}).get('brokerOrderEvidence'))
            for evidence in candidates:
                if (not isinstance(evidence, dict) or evidence.get('schema') != 'broker-opening-order-v1'
                        or evidence.get('openingTicket') != ticket or evidence.get('symbol') != payload['symbol']):
                    continue
                previous = child.get('brokerOrderEvidence', {}).get(account)
                if previous != evidence:
                    child.setdefault('brokerOrderEvidence', {})[account] = copy.deepcopy(evidence)
                    if (previous is None or previous.get('acceptedOrderPrice') != evidence.get('acceptedOrderPrice')
                            or previous.get('orderType') != evidence.get('orderType')):
                        history = child.setdefault('brokerOrderChanges', {}).setdefault(account, [])
                        history.append(dict(checkedAtMs=time.time()*1000, **copy.deepcopy(evidence)))
                        if len(history) > 4:
                            del history[1:-3]  # Keep the first observation and the latest three.
                    changed = True
            position = child.get('positions', {}).get(account)
            if not position:
                return changed
            submitted, filled = child['prices'][account], position['fillPrice']
            if abs(filled-submitted) <= 1e-8:
                return changed
            price_evidence = dict(openingTicket=ticket, submittedPrice=submitted,
                fillPrice=filled, priceDifference=rounded(filled-submitted))
            broker = child.get('brokerOrderEvidence', {}).get(account)
            if broker:
                price_evidence.update(brokerOrder=copy.deepcopy(broker),
                    observedOrderChanges=copy.deepcopy(child.get('brokerOrderChanges', {}).get(account, [])))
            else:
                price_evidence['brokerOrderEvidenceAvailable'] = False
            if child.get('priceEvidence', {}).get(account) != price_evidence:
                child.setdefault('priceEvidence', {})[account] = price_evidence
                changed = True
                if self.entry_diagnostics:
                    self.entry_diagnostics.emit('limit-price-observation', account=account, entry_id=state['id'],
                        symbol=payload['symbol'], stage='limit-fill',
                        reason='opening='+ticket+' submitted='+str(submitted)+' fill='+str(filled)
                            +' accepted='+str(broker.get('acceptedOrderPrice') if broker else 'unavailable')
                            +' nativeType='+str(broker.get('orderType') if broker else 'unavailable'))
            return changed
        except Exception:
            return changed  # Missing optional evidence never halts or releases a trade.

    def _hybrid_apply_limit_view(self, payload, state, child, account, ticket, view, cancel, retain_pending_fill=False):
        """D492 exact proof; joined tasks also retain proven partial fills."""
        direction = 1 if payload['side'] == 'buy' else -1
        changed = False
        pending, positions = view.get('pending', []), view.get('positions', [])
        if len(pending) > 1 or len(positions) > 1 or cancel and pending and not retain_pending_fill:
            raise ValueError('Limit remainder cancellation or opening identity is unconfirmed')
        terminal = view.get('terminal', {})
        closed_proof = terminal.get('openingProof', {})
        historical = (not pending and not positions and terminal.get('phase') == 'closed'
            and closed_proof.get('kind') == 'exact-balanced-opening-v1'
            and str(closed_proof.get('openingTicket')) == ticket
            and str(closed_proof.get('positionId')) == str(terminal.get('positionId'))
            and closed_proof.get('symbol') == payload['symbol'] and closed_proof.get('side') == payload['side'])
        if historical:
            positions = [dict(ticket=str(closed_proof['positionId']), symbol=payload['symbol'], type=payload['side'],
                lots=closed_proof['volume'], open_price=closed_proof['fillPrice'], sl=closed_proof['sl'], tp=closed_proof['tp'])]
        if (cancel and not pending and not positions and terminal.get('phase') == 'closed'
                and re.fullmatch(r'[1-9][0-9]{0,19}', str(terminal.get('positionId', '')))):
            # This is exact native balanced-history proof for the captured
            # opening, never mere absence. A user/broker exit can beat our
            # first fill read or an explicit stop. Retain its receipt and
            # any captured fill, while preventing further acquisition.
            previous = child.get('positions', {}).get(account)
            if previous is not None:
                if str(previous['ticket']) != str(terminal['positionId']):
                    raise ValueError('Closed hybrid position identity changed')
            closed = child.setdefault('closedOpenings', {})
            proof = dict(openingTicket=ticket, positionId=str(terminal['positionId']))
            changed |= closed.get(account) != proof
            closed[account] = proof
            state['hybridRemainderStop'] = 'Captured hybrid opening is confirmed fully closed; remainder stopped'
            return False, changed
        price = child['prices'][account]
        stop = child['stops'][account]
        take = child.get('takes', {}).get(account, payload.get('tp', 0))
        previous = child.get('positions', {}).get(account, {})
        volume = number(positions[0]['lots'], 'Hybrid limit filled volume') if positions else 0.
        remaining = number(pending[0]['lots'], 'Hybrid limit remainder') if pending else 0.
        if volume + 1e-8 < previous.get('volume', 0) or volume + remaining > child['volumes'][account] + 1e-8:
            raise ValueError('Hybrid opening volume changed or exceeds its reservation')
        for row, position in [(row, False) for row in pending] + [(row, True) for row in positions]:
            if (row.get('symbol') != payload['symbol'] or row.get('type') != payload['side']
                    or abs(float(row.get('sl', 0)) - stop) > 1e-8
                    or abs(float(row.get('tp', 0)) - take) > 1e-8):
                raise ValueError('Hybrid limit or position protection changed')
            if not position and (str(row['ticket']) != ticket or abs(float(row['open_price'])-price) > 1e-8
                    or abs(volume+remaining-child['volumes'][account]) > 1e-8):
                raise ValueError('Hybrid pending reservation changed')
        if positions:
            row = positions[0]
            fill = number(row['open_price'], 'Hybrid limit fill price')
            guard = child['guards'][account]
            actual = dict(ticket=str(row['ticket']), openingTicket=ticket, volume=volume,
                          fillPrice=fill, sl=stop, tp=take)
            outside = fill_outside_zone(payload, fill, guard['priceFloor'], guard['priceCeiling'])
            weighted = weighted_nasdaq_stop(payload,state['accounts'][account])
            if outside and (weighted or fill_priority(payload)):
                child['actualPriceWarning'] = 'Actual fill differs from captured entry price; weighted stop at captured panel distance required'
            if outside and not weighted and not fill_priority(payload) or direction*(fill-stop) <= 0:
                child['fillGuardViolated'] = True
                state['hybridRemainderStop'] = 'Actual hybrid fill left the captured zone; remainder stopped'
            if previous != actual:
                child.setdefault('positions', {})[account] = actual
                # Preserve every known fill even if another account's
                # subsequent status read fails or the receiver restarts.
                self._hybrid_recount(payload, state, allow_excess_risk=True)
                self._save(state)
                changed = True
            if historical:
                child.setdefault('closedOpenings', {})[account] = dict(openingTicket=ticket, positionId=str(row['ticket']))
                state['hybridRemainderStop'] = 'Captured hybrid opening is confirmed fully closed; remainder stopped'
        elif not pending and view.get('terminal', {}).get('phase') != 'cancelled':
            raise ValueError('Hybrid opening closed or its final broker result is unknown')
        if not pending and volume < child['volumes'][account]-1e-8 and not cancel and account not in child.get('cancellations', {}):
            raise ValueError('Hybrid limit remainder was changed outside this entry')
        if cancel and pending:
            raise ValueError('Limit remainder cancellation or opening identity is unconfirmed')
        changed = self._observe_limit_price(payload, state, child, account, ticket, view) or changed
        return bool(pending), changed

    def _hybrid_limit(self, payload, state, child, cancel=False, accounts=None):
        """Reconcile every joined account before deciding the shared outcome."""
        results = self._limit_results(state, child)
        selected = set(child['volumes']) if accounts is None else set(accounts).intersection(child['volumes'])
        resolved = self._hybrid_parallel_cancel(payload, state, child, results) if cancel and selected == set(child['volumes']) else None
        active = False
        changed = False
        errors = []
        for account in child['volumes']:
            if account not in selected or child.get('settledAccounts', {}).get(account):
                continue
            outcome = results.get(account, {})
            if outcome.get('status') == 'rejected':
                continue
            if (resolved is None and cancel and hasattr(self, 'lane_account')
                    and callable(getattr(self.bridge, 'entry_cancel_limit', None))):
                ticket, view = self._lane_cancel_limit(payload, state, child, account, outcome)
            elif resolved is None:
                ticket, view = self._limit_view(payload, account, outcome)
                if cancel and view.get('pending'):
                    cancellations = child.setdefault('cancellations', {})
                    if account in cancellations:
                        raise ValueError('A previous limit cancellation was not confirmed; no retry')
                    identifier = self._hybrid_cancel_id(child, account)
                    cancellations[account] = dict(status='claimed', id=identifier)
                    child.setdefault('cancelIds', {})[account] = identifier
                    self._save(state)
                    try:
                        receipt = self._call(account, 'execute', 'cancel', dict(id=identifier, ticket=ticket,
                            symbol=payload['symbol'], expiresAt=time.time()*1000+3000))['result']
                    except Exception as error:
                        receipt = (dict(status='rejected', dispatchNotSent=True, reason=str(error),workerContention=getattr(error,'rpc_diagnostics',{}))
                            if getattr(error, 'dispatch_not_sent', False) else dict(status='unknown', reason=str(error)))
                    cancellations[account] = hybrid_receipt(receipt)
                    if not hasattr(self, 'lane_account') or receipt.get('status') not in ('ok', 'rejected'):
                        self._save(state)
                    if receipt.get('status') not in ('ok', 'rejected'):
                        raise ValueError('Limit cancellation result is unknown; market reservation remains locked')
                    _, view = self._limit_view(payload, account, outcome)
            else:
                result = resolved[account]
                errors.extend(result['errors'])
                if result['view'] is None:
                    continue
                ticket, view = result['ticket'], result['view']
            try:
                pending, updated = self._hybrid_apply_limit_view(payload, state, child, account, ticket, view, cancel,
                                                                  retain_pending_fill=resolved is not None)
                active |= pending
                changed |= updated
                if parallel_accounts(payload) and not pending:
                    child.setdefault('settledAccounts', {})[account] = True
                    changed = True
            except Exception as error:
                if resolved is None:
                    raise
                # Another account can already have an exact accepted fill.
                # Retain it even when a cancellation/proof on its peer fails.
                errors.append(str(error))
        if errors:
            self._hybrid_recount(payload, state, allow_excess_risk=True)
            self._save(state)
            raise ValueError(errors[0])
        if parallel_accounts(payload):
            active = bool(pending_accounts(child))
        if not active:
            phase = 'confirmed' if child.get('positions') else 'cancelled'
            changed |= child['state'] != phase
            child['state'] = phase
        self._hybrid_recount(payload, state, allow_excess_risk=True)
        if changed:
            self._save(state)
        return active

    def _lane_cancel_limit(self, payload, state, child, account, outcome):
        ticket=str(outcome.get('providerOrderId',''))
        if not re.fullmatch(r'[1-9][0-9]{0,19}',ticket):
            raise ValueError('Captured limit ticket is unconfirmed; no cancellation sent')
        cancellations=child.setdefault('cancellations',{})
        if account in cancellations:
            raise ValueError('A previous limit cancellation was not confirmed; no retry')
        decision=time.time()*1000;started=time.monotonic()
        identifier=self._hybrid_cancel_id(child,account)
        cancellations[account]=dict(status='claimed',id=identifier)
        child.setdefault('cancelIds',{})[account]=identifier
        self._save(state)
        claimed=time.time()*1000
        request=dict(id=identifier,ticket=ticket,symbol=payload['symbol'],expiresAt=time.time()*1000+3000)
        expected=dict(side=payload['side'],price=child['prices'][account],sl=child['stops'][account],
            tp=child.get('takes',{}).get(account,payload.get('tp',0)),volume=child['volumes'][account])
        watch=[dict(account=account,ticket=row['openingTicket'],symbol=payload['symbol'])
            for part in state['children'] for row in [part.get('positions',{}).get(account)]
            if row and not row.get('closed') and account not in part.get('closedOpenings',{})]
        try:
            reply=self._call(account,'entry_cancel_limit',request,expected,watch)
            receipt=reply['receipt']
        except Exception as error:
            receipt=(dict(status='rejected',dispatchNotSent=True,reason=str(error),workerContention=getattr(error,'rpc_diagnostics',{}))
                if getattr(error,'dispatch_not_sent',False) else dict(status='unknown',reason=str(error)))
            reply={}
        cancellations[account]=hybrid_receipt(receipt)
        timing=dict(decisionAt=decision,claimFinishedAt=claimed,resultAt=time.time()*1000,
            claimMs=round(claimed-decision,3),totalMs=round((time.monotonic()-started)*1000,3),
            native=reply.get('timing',{}),rpc=reply.get('rpcTiming',{}))
        diagnostics=state['accounts'][account].setdefault('entryTiming',{})
        diagnostics['lastCancellation']=timing
        history=diagnostics.setdefault('cancellations',[])
        history.append(dict(childId=child['id'],**timing));del history[:-8]
        if receipt.get('status') not in ('ok','rejected') or not isinstance(reply.get('view'),dict):
            self._save(state)
            raise ValueError('Limit cancellation result is unknown; market reservation remains locked')
        view=reply['view']
        if str(view.get('account'))!=account or view.get('server')!=self.bridge.config['server']:
            self._save(state)
            raise ValueError('Cancellation proof account/server mismatch')
        proof=reply.get('proof')
        if isinstance(proof,dict):
            self.cancel_proofs[(state['id'],account)]=proof
        return ticket,view

    def _hybrid_sync_stop(self, payload, state, accounts=None):
        """No live pending order may reset a shared SL while it is updated."""
        if state.get('protectionPending') or state.get('closurePending'):
            raise ValueError('Protection command interrupted; inspect its saved receipt; no automatic replay')
        if any(child['type'] == 'market' and child['state'] not in ('confirmed', 'rejected') for child in state['children']):
            raise ValueError('Common stop requires confirmed outcomes of every market part')
        if any(child['type'] == 'limit' and child['state'] not in ('confirmed', 'cancelled', 'rejected')
               and (accounts is None or pending_accounts(child).intersection(accounts)) for child in state['children']):
            raise ValueError('Common stop requires a confirmed cancellation of every pending remainder')
        for account, details in state['accounts'].items():
            if accounts is not None and account not in accounts:
                continue
            positions = [child['positions'][account] for child in state['children'] if account in child.get('positions', {})
                         and not child['positions'][account].get('closed') and account not in child.get('closedOpenings', {})]
            if not positions:
                continue
            recovering = bool(details.get('commonStopUncertain'))
            if recovering and not (details.get('commonStopRequest') and details.get('commonStopRecoveryAllowed')):
                raise ValueError('A previous common stop update is unconfirmed; no automatic repeat')
            signature = digest(dict(positions=[{key: row[key] for key in ('ticket', 'volume', 'fillPrice')} for row in positions]))
            if not recovering and details.get('stopSignature') == signature:
                continue
            if not recovering and payload.get('initialSizingOnly') is True and not details.get('stopCheckRejected'):
                # Only a current full position proof can avoid a no-op native
                # stop command. A real weighted-stop change still uses the
                # durable broker operation and verifies every affected ticket.
                proof = getattr(self,'position_proof',{}).get(account)
                distance = self._hybrid_distance(payload,state,account)
                average = sum(row['volume']*row['fillPrice'] for row in positions)/sum(row['volume'] for row in positions)
                direction = 1 if payload['side']=='buy' else -1
                if distance is None:
                    target = self._hybrid_stop(payload,state,account,average)
                else:
                    units=Decimal(str(average-direction*distance))/Decimal(str(payload['tickSize']))
                    target=float(units.to_integral_value(rounding=ROUND_CEILING if direction==1 else ROUND_FLOOR)*Decimal(str(payload['tickSize'])))
                if (proof and time.monotonic()-proof[0]<=.25 and proof[1]==digest(dict(positions=positions))
                        and all(abs(row['sl']-target)<=1e-8 and abs(row['tp']-self._hybrid_tp(payload,state,account))<=1e-8 for row in positions)):
                    details.update(stopSignature=signature,sl=target,averagePrice=rounded(average))
                    self._save(state)
                    continue
            observation_started = time.monotonic()
            if recovering:
                request = details['commonStopRequest']
                clocks = getattr(self, 'common_stop_poll_at', {})
                self.common_stop_poll_at = clocks
                if time.monotonic() < clocks.get((state['id'],account), 0):
                    raise CommonStopWait('Waiting for this account common stop receipt')
                clocks[(state['id'],account)] = time.monotonic()+.25
                expected = hashlib.sha256(json.dumps(request,sort_keys=True,allow_nan=False).encode()).hexdigest()
                try:
                    proof = self._call(account,'entry_common_stop_receipt',dict(id=request['id'],digest=expected))
                except Exception as error:
                    raise CommonStopWait('Common stop receipt is not available yet') from error
                if (proof.get('account')!=account or proof.get('server')!=self.bridge.config['server']
                        or proof.get('id')!=request['id'] or proof.get('digest')!=expected
                        or not isinstance(proof.get('result'),dict)):
                    raise CommonStopWait('Exact common stop receipt is not confirmed yet')
                result = proof['result']
                if result.get('status')=='unknown':
                    raise CommonStopWait('Common stop outcome remains unknown; no replay')
            else:
                revision = int(state.get('stopRevision', 0)) + 1
                state['stopRevision'] = revision
                request = dict(id='hybrid-sl:' + hashlib.sha256((state['id']+':'+str(revision)+':'+account).encode()).hexdigest(),
                    symbol=payload['symbol'], side=payload['side'], positions=[{key: row[key] for key in ('ticket', 'volume', 'fillPrice', 'sl', 'tp')} for row in positions], riskBudget=details['riskUsd'], maxVolume=details['targetVolume'])
                distance = self._hybrid_distance(payload, state, account)
                if fill_priority(payload):
                    request['riskBudget'] = max(request['riskBudget'], rounded(sum(
                        row['volume'] * max(0., (1 if payload['side']=='buy' else -1)*(row['fillPrice']-row['sl']))
                        * details['pointValue'] for row in positions)))
                if distance is None:
                    request['sl'] = self._hybrid_stop(payload, state, account, positions[0]['fillPrice'])
                else:
                    request['stopDistance'] = distance
                if weighted_nasdaq_stop(payload) and distance is not None:
                    request['weightedEntryStop'] = True
                details.update(commonStopUncertain=True,commonStopRequest=request)
                self._save(state)
                try:
                    result = self._call(account, 'entry_common_stop', request)
                except Exception as error:
                    if getattr(error,'dispatch_not_sent',False):
                        details['commonStopUncertain'] = False
                        details.pop('commonStopRequest',None)
                    else:
                        details['commonStopRecoveryAllowed'] = True
                    self._save(state)
                    raise CommonStopWait('Waiting for this account common stop confirmation') from error
            try:
                record_common_stop_timing(details.setdefault('entryTiming', {}), result,
                    (time.monotonic()-observation_started)*1000, receipt_only=recovering)
            except Exception:
                pass  # Optional timings never control a protection operation.
            if (result.get('status') == 'rejected' and result.get('mutationStarted') is False
                    and result.get('account') == account and result.get('symbol') == payload['symbol']
                    and result.get('finalPreMutationRejection') is True):
                details['commonStopUncertain'] = False
                details['stopCheckRejected'] = str(result.get('reason', 'Common stop rejected'))[:256]
                self._save(state)
            proof = result.get('quoteWait', {})
            if (result.get('status') == 'rejected' and result.get('retryableQuoteWait') is True
                    and result.get('mutationStarted') is False and result.get('account') == account
                    and result.get('symbol') == payload['symbol'] and isinstance(proof, dict)
                    and all(isinstance(proof.get(key), (int, float)) and not isinstance(proof[key], bool)
                            and math.isfinite(proof[key]) for key in ('nativeTickUtcMs', 'nativeCapturedAtMs', 'nativeAgeMs'))
                    and proof['nativeTickUtcMs'] > 0 and proof['nativeCapturedAtMs'] > 0
                    and (proof['nativeAgeMs'] < -250 or proof['nativeAgeMs'] > 1000)
                    and abs(proof['nativeCapturedAtMs']-proof['nativeTickUtcMs']-proof['nativeAgeMs']) < .1):
                # Only native proof of a pre-mutation rejection releases the
                # uncertain marker. Unknown/partial changes remain locked.
                details['commonStopUncertain'] = False
                observation = dict(quote=dict(utcTime=proof['nativeTickUtcMs']/1000,
                    nativeCapturedAt=proof['nativeCapturedAtMs'], nativeAgeMs=proof['nativeAgeMs']))
                if state.get('hybridRemainderStop'):
                    state['protectionOnlyWait'] = True
                    state['quoteWait'] = QuoteWait(account, observation, 'common-stop').details
                self._save(state)
                raise QuoteWait(account, observation, 'common-stop')
            if result.get('status') != 'ok' or str(result.get('account', account)) != account:
                if (result.get('status') == 'rejected' and result.get('mutationStarted') is False
                        and result.get('finalPreMutationRejection') is True
                        and result.get('account') == account and result.get('symbol') == payload['symbol']):
                    raise StopCheckRejected(str(result.get('reason', 'Common stop rejected')))
                raise ValueError('Common stop was not confirmed: ' + str(result.get('reason', result.get('status'))))
            rows = {str(row['ticket']): row for row in result.get('positions', [])}
            if set(rows) != {row['ticket'] for row in positions}:
                raise ValueError('Common stop returned different position identities')
            for child in state['children']:
                position = child.get('positions', {}).get(account)
                if position and not position.get('closed') and account not in child.get('closedOpenings', {}):
                    updated = rows[position['ticket']]
                    if any(abs(float(updated[key])-position[key]) > 1e-8 for key in ('volume', 'fillPrice', 'tp')):
                        raise ValueError('Position changed during common stop update')
                    position['sl'] = number(updated['sl'], 'Confirmed common stop')
            details.update(averagePrice=number(result['averagePrice'], 'Average entry price'),
                           sl=number(result['sl'], 'Common stop'), stopSignature=signature, commonStopUncertain=False)
            details.pop('commonStopRequest',None)
            details.pop('commonStopRecoveryAllowed',None)
            details.pop('stopCheckRejected', None)
            if recovering:
                # A durable receipt settles the old operation, not the current
                # position identity. Recheck without sending a protection change.
                self._save(state)
                try:self._positions(payload,state,accounts={account})
                except Exception as error:
                    if getattr(error,'dispatch_not_sent',False):
                        raise CommonStopWait('Waiting for fresh account positions after stop confirmation') from error
                    raise
            proof=result.get('protectionProof')
            current=[child['positions'][account] for child in state['children'] if account in child.get('positions',{})
                and not child['positions'][account].get('closed') and account not in child.get('closedOpenings',{})]
            if (isinstance(proof,dict) and proof.get('schema')=='entry-protection-proof-v1'
                    and proof.get('commandId')==request['id'] and proof.get('account')==account
                    and proof.get('server')==self.bridge.config['server'] and proof.get('symbol')==payload['symbol']
                    and type(proof.get('checkedAt')) in (int,float) and 0<=time.time()*1000-proof['checkedAt']<=250
                    and proof.get('positions')==[{key:p[key] for key in ('ticket','volume','fillPrice','sl','tp')} for p in current]):
                native=dict(account=account,server=proof['server'],checkedAt=proof['checkedAt'],orders=[
                    dict(ticket=p['ticket'],symbol=payload['symbol'],type=payload['side'],lots=p['volume'],
                         open_price=p['fillPrice'],sl=p['sl'],tp=p['tp'],__isPosition=True) for p in current])
                self.protection_proofs=getattr(self,'protection_proofs',{})
                self.protection_proofs[account]=(state['id'],digest(dict(positions=current)),native)
            # Another account can still await its own repair. Enforce every
            # account's live budget after all exact native stop updates.
            self._hybrid_recount(payload, state, allow_excess_risk=True)
            if account == self.primary:
                state['sl'] = details['sl']
            self._save(state)
        self._hybrid_recount(payload, state)

    def _hybrid_wait_limits(self, payload, state):
        """Observe accepted limits during a quote-only wait without replacing them."""
        if (state.get('hybridRemainderStop') or self._stopping(state['id'])
                or time.time()*1000 >= payload['closeAtUtc']
                or state.get('protectionPending') or state.get('closurePending') or state.get('protectionPause')
                or self._protecting(state['id']) or self._hybrid_fill_violation(state)
                or any(child['state'] in ('sending', 'unknown') for child in state['children'])
                or any(row.get('commonStopUncertain') or row.get('riskRepairRequired') for row in state['accounts'].values())):
            return False
        # A newly observed fill still requires cancellation of its reserved
        # remainder before a common stop can be recalculated on fresh quotes.
        for account, details in state['accounts'].items():
            positions = [child['positions'][account] for child in state['children']
                         if account in child.get('positions', {}) and not child['positions'][account].get('closed')
                         and account not in child.get('closedOpenings', {})]
            if positions and details.get('stopSignature') != digest(dict(positions=[
                    {key: row[key] for key in ('ticket', 'volume', 'fillPrice')} for row in positions])):
                return False
        now = time.monotonic()
        if now < self.pending_poll_at.get(state['id'], 0):
            return True
        self.pending_poll_at[state['id']] = now + .25
        before = sum(row['filledVolume'] for row in state['accounts'].values())
        for child in state['children']:
            if child['type'] == 'limit' and child['state'] == 'pending':
                # Exact native ticket, reservation, SL/TP and fill proofs are
                # the same as on a fresh cycle. This path sends no orders.
                self._hybrid_limit(payload, state, child, cancel=False)
        return (abs(sum(row['filledVolume'] for row in state['accounts'].values())-before) <= 1e-8
                and not state.get('hybridRemainderStop') and not self._hybrid_fill_violation(state)
                and not self._stopping(state['id']) and not self._protecting(state['id'])
                and time.time()*1000 < payload['closeAtUtc']
                and not any(row.get('riskRepairRequired') for row in state['accounts'].values()))

    def _hybrid_cancel_limits(self, payload, state, sync_stop=True):
        safe = True
        for child in state['children']:
            if child.get('type') != 'limit' or child['state'] in ('confirmed', 'cancelled', 'rejected'):
                continue
            try:
                self._hybrid_limit(payload, state, child, cancel=True)
            except Exception as error:
                state['reason'] = str(error)
                safe = False
        if safe and sync_stop:
            try:
                self._hybrid_sync_stop(payload, state)
                if state.get('protectionOnlyWait'):
                    # Earlier accounts can be skipped by stopSignature on a
                    # retry. Prove their actual volume and SL/TP again too.
                    self._positions(payload, state)
                    state.pop('protectionOnlyWait', None)
                    if state.get('quoteWait', {}).get('status') == 'waiting':
                        state['quoteWait'].update(status='resumed', resumedAtMs=time.time()*1000)
            except (QuoteWait, CommonStopWait):
                raise
            except StopCheckRejected as error:
                state['reason'] = str(error)
                # Cancellation and all native outcomes are definite. The lane
                # stops with its refusal, never masquerading as a lost reply.
                return True
            except Exception as error:
                state['reason'] = str(error)
                safe = False
        return safe

    def _hybrid_fill_violation(self, state):
        return any(child.get('fillGuardViolated') and not child.get('fillGuardAcknowledged') for child in state['children'])

    def _fill_budget(self, payload, state, account):
        details = state['accounts'][account]
        if fill_priority(payload) != 'volume':
            return details['riskUsd']
        lower, upper = market_price_bounds(payload)
        direction = 1 if payload['side']=='buy' else -1
        worst = upper if direction == 1 else lower
        distance = self._hybrid_distance(payload,state,account)
        cost = distance if distance is not None else direction*(worst-self._hybrid_stop(payload,state,account,worst))
        return max(details['riskUsd'], rounded(details['riskUsed']+
            max(0.,details['targetVolume']-details['filledVolume'])*max(0.,cost)*details['pointValue']))

    def _hybrid_guards(self, payload, state, prices, volumes, reserved=None, support=False):
        lower, upper = market_price_bounds(payload)
        direction = 1 if payload['side'] == 'buy' else -1
        guards = {}
        for account in volumes:
            details = state['accounts'][account]
            price, volume = prices[account], volumes[account]
            distance = self._hybrid_distance(payload, state, account)
            stop = self._hybrid_stop(payload, state, account, price)
            budget = self._fill_budget(payload,state,account)-details['riskUsed']-(reserved or {}).get(account, 0.)
            if payload.get('initialSizingOnly') is True:
                # Initial sizing has already used risk/stop. There is no money
                # reservation during acquisition; native per-order protection
                # remains bounded by this volume and the captured price zone.
                worst = upper if direction == 1 else lower
                cost = distance if distance is not None else direction*(worst-stop)
                budget = rounded(volume*max(cost,direction*(price-stop))*details['pointValue'])+1e-8
            if budget <= 0 or volume*direction*(price-stop)*details['pointValue'] > budget+1e-8:
                raise ValueError('Hybrid reservation exceeds remaining protected risk')
            guard = dict(anchor=price, maxGap=payload['maxGap'],
                maxRiskPrice=(upper if direction == 1 else lower) if distance is not None or payload.get('initialSizingOnly') is True else stop+direction*budget/(volume*details['pointValue']),
                priceFloor=lower, priceCeiling=upper,
                positions=[{key: child['positions'][account][key] for key in ('ticket', 'volume', 'sl', 'tp')}
                    for child in state['children'] if account in child.get('positions', {})])
            if distance is not None:
                guard.update(stopDistance=distance, riskBudget=budget)
            override = details.get('protectionOverride', {})
            if override:
                guard['fixedProtection'] = dict(tp=self._hybrid_tp(payload, state, account))
                if 'sl' in override:
                    guard['fixedProtection'].update(sl=stop, riskBudget=budget)
            if support:
                guard.update(limitPolicy='support', limitPrice=price)
                if callable(getattr(self.bridge, 'entry_reconcile', None)):
                    guard['supportReprice'] = True
            guards[account] = guard
        return guards

    def _hybrid_capture_incomplete_market(self, payload, state, child, results):
        """Preserve exact accepted legs; a rejected/unknown leg never retries."""
        final = all(row.get('status') in ('ok', 'rejected') for row in results.values())
        state['hybridRemainderStop'] = ('Hybrid market was accepted on only some accounts; remainder stopped' if final else
                                        'Hybrid market placement is unconfirmed on an account; remainder stopped')
        if not final:
            state['reconcileTransientHalt'] = True
        # This halt survives interruption before the first native proof read.
        # Keep the original aggregate and every account's receipt untouched.
        self._save(state)
        direction = 1 if payload['side'] == 'buy' else -1
        for account, outcome in results.items():
            if outcome.get('status') != 'ok':
                continue
            position = self._confirmed_position(payload, state, child, account, outcome)
            child['positions'][account] = position
            if position.get('closedAtBroker'):
                child.setdefault('closedOpenings', {})[account] = dict(openingTicket=position['openingTicket'], positionId=position['ticket'])
            guard = child['guards'][account]
            price = position['fillPrice']
            child['fillGuardViolated'] = child.get('fillGuardViolated', False) or (
                direction * (price - guard['anchor']) > guard['maxGap'] + 1e-8
                or direction * (price - guard['maxRiskPrice']) > 1e-8
                or fill_outside_zone(payload, price, guard.get('priceFloor', price), guard.get('priceCeiling', price))
                or direction * (price - position['sl']) <= 0)
            # A known broker fill is not erased if the next account's proof
            # fails. Its actual gap risk is repaired only after pending limits
            # are confirmed cancelled and every market outcome is definite.
            self._hybrid_recount(payload, state, allow_excess_risk=True)
            self._save(state)
        child['state'] = 'confirmed' if final else 'unknown'
        if final:
            state['nextChunk'] += 1
        self._save(state)
        raise ValueError(state['hybridRemainderStop'])

    def _hybrid_send(self, payload, state, kind, prices, volumes, guards, support=False):
        if payload.get('initialSizingOnly') is True:
            # Native request receipts remain immutable. Only old, proven empty
            # rejections leave the hot parent; unknown or accepted claims stay.
            empty = [row for row in state['children'] if row.get('state')=='rejected'
                     and not row.get('positions') and all(outcome.get('status')=='rejected'
                         and not outcome.get('providerOrderId') and not outcome.get('filledVolume')
                         for outcome in row.get('result',{}).get('accountResults',{}).values())
                     and row.get('result',{}).get('status')=='rejected'
                     and not row.get('result',{}).get('providerOrderId') and not row.get('result',{}).get('filledVolume')]
            removed = {row['id'] for row in empty[:-8]}
            if removed:
                state['children'] = [row for row in state['children'] if row['id'] not in removed]
                details = state['accounts'][next(iter(state['accounts']))]
                details['archivedRejections'] = details.get('archivedRejections',0)+len(removed)
        maximum = getattr(self, 'lane_submission_limit', HYBRID_MAX_SUBMISSIONS)
        if len(state['children']) >= maximum:
            raise ValueError('Hybrid entry reached its bounded submission limit')
        if kind == 'market' and sum(child.get('type') == 'market' and child.get('state') == 'confirmed'
                                    for child in state['children']) >= HYBRID_MAX_MARKET_PARTS:
            raise ValueError('Hybrid entry reached its bounded market part limit')
        for account in volumes:
            positions = {child['positions'][account]['ticket'] for child in state['children']
                         if account in child.get('positions', {})}
            pending_slots = sum(child.get('type') == 'limit' and child.get('state') not in ('confirmed', 'cancelled', 'rejected')
                                and account in child['volumes'] and account not in child.get('positions', {}) for child in state['children'])
            # A support order must leave room for its already-reserved market
            # part too. An unobserved limit fill occupies its pending slot.
            needed = 1 + (1 if kind == 'limit' and support else 0)
            if len(positions) + pending_slots + needed > HYBRID_MAX_POSITIONS:
                raise ValueError('Hybrid entry has no reserved position slot for this submission')
        if self._stopping(state['id']) or time.time()*1000 >= payload['closeAtUtc']:
            raise ValueError('Hybrid entry stopped before submission')
        index = len(state['children'])
        if payload.get('initialSizingOnly') is True:
            details = state['accounts'][next(iter(state['accounts']))]
            index = details.get('submissionSerial',0)
            details['submissionSerial'] = index+1
        if hasattr(self, 'lane_slot'):
            index = index*32+self.lane_slot
        direction = 1 if payload['side'] == 'buy' else -1
        stops = {account: self._hybrid_stop(payload, state, account, price) for account, price in prices.items()}
        takes = {account: self._hybrid_tp(payload, state, account) for account in volumes}
        selected = self.primary if self.primary in volumes else next(iter(volumes))
        request = {key: payload[key] for key in DEADLINE_FIELDS}
        request.update(id=state['id']+':h'+str(index+1), symbol=payload['symbol'], side=payload['side'],
            type=kind, volume=volumes[selected], price=prices[selected], sl=stops[selected],
            tp=takes[selected], cid=payload.get('cid', ''), expiresAt=time.time()*1000+3000)
        for key in ('mirrorAccounts', 'mirrorAccount'):
            if key in payload:
                request[key] = copy.deepcopy(payload[key])
        child = dict(id=request['id'], index=index+1, state='sending', type=kind, support=support,
            price=request['price'], prices=copy.deepcopy(prices), stops=stops, takes=takes, volumes=volumes,
            guards=copy.deepcopy(guards), positions={}, claimedAt=time.time()*1000)
        state['children'].append(child)
        self._phase(state, 'sending', 'Hybrid volume reserved; awaiting broker confirmation')
        try:
            if hasattr(self, 'lane_account'):
                request['entryGuard'] = guards[self.lane_account]
                if payload.get('initialSizingOnly') is True:request['positionProofRequested']=True
                try:
                    reply = self._call(self.lane_account, 'execute', 'place', request)
                except Exception as error:
                    if not getattr(error,'dispatch_not_sent',False):
                        raise
                    reply = dict(result=dict(status='rejected',dispatchNotSent=True,reason=str(error),
                        workerContention=getattr(error,'rpc_diagnostics',{})))
                outcome = reply['result']
                if kind=='market' and outcome.get('status')=='ok' and isinstance(reply.get('positionProof'),dict):
                    self.opening_proofs=getattr(self,'opening_proofs',{})
                    self.opening_proofs[(child['id'],self.lane_account)]=reply['positionProof']
                result = dict(outcome, accountResults={self.lane_account:dict(outcome, timings={'server':reply.get('serverTiming', {}), 'workerRpc':reply.get('rpcTiming', {})})})
            elif len(state['accounts']) > 1:
                options = dict(volumes=volumes, guards=guards)
                if parallel_accounts(payload):
                    options['independent'] = True
                result = self.bridge.group_execute('place', request, **options)['result']
            else:
                request['entryGuard'] = guards[self.primary]
                result = self.bridge.execute('place', request)['result']
            child['result'] = hybrid_receipt(result)
            if parallel_accounts(payload):
                for account, outcome in result.get('accountResults', {self.primary: result}).items():
                    timing = outcome.get('timings', {}).get('server', {})
                    state['accounts'][account].setdefault('entryTiming', {}).update(
                        lastIntentAfterArmMs=round(child['claimedAt']-state['createdAt'], 3),
                        lastNativeMs=timing.get('mt5Ms'), lastCommandMs=timing.get('totalMs'))
            if kind == 'limit' and support:
                for account, outcome in self._limit_results(state, child).items():
                    self._adopt_support_price(payload, state, child, account, outcome)
            if result.get('status') == 'rejected':
                child['state'] = 'rejected'
                reason = result.get('reason', 'Hybrid order rejected')
                if parallel_accounts(payload):
                    self._phase(state, 'running', 'Definite account refusals received; other accounts remain independent')
                    return False
                transient = any(text in reason.lower() for text in ('quote', 'tick is stale', 'price moved outside', 'limit requires'))
                quote_wait = any(text in reason.lower() for text in ('quote', 'tick is stale'))
                if independent_accounts(payload) and transient:
                    child['retryablePriceRejection'] = True
                    self._phase(state, 'running', 'Account price changed; waiting for a fresh permitted price')
                    return False
                self._phase(state, 'running' if kind == 'limit' and support and not quote_wait else 'paused' if transient else 'stopped', reason)
                return False
            if kind == 'market':
                results = self._limit_results(state, child)
                if (result.get('status') == 'unknown' and isinstance(results, dict) and set(results) == set(volumes)
                        and all(isinstance(row, dict) and row.get('status') in ('ok', 'rejected', 'unknown') for row in results.values())
                        and any(row['status'] == 'ok' for row in results.values())
                        and not (parallel_accounts(payload) and all(row['status'] in ('ok', 'rejected') for row in results.values()))):
                    self._hybrid_capture_incomplete_market(payload, state, child, results)
                self._confirm(payload, state, child, result)
                if state['phase'] == 'completed' and any(row['filledVolume'] < row['targetVolume']-1e-8 for row in state['accounts'].values()):
                    self._phase(state, 'running', 'Market part confirmed; checking remaining volume')
            else:
                results = self._limit_results(state, child)
                if (result.get('status') == 'unknown' and set(results) == set(volumes)
                        and all(isinstance(row, dict) and row.get('status') in ('ok', 'rejected') for row in results.values())
                        and {row['status'] for row in results.values()} == {'ok', 'rejected'}
                        and all(re.fullmatch(r'[1-9][0-9]{0,19}', str(row.get('providerOrderId', '')))
                                for row in results.values() if row['status'] == 'ok')):
                    # Bridge's aggregate is unknown for mixed FINAL results.
                    # Its accepted tickets are nevertheless exact. Halt the
                    # remainder before any reserved market part, then let the
                    # normal cancellation/fill-race accounting settle them.
                    # A real unknown leg never takes this path.
                    child['state'] = 'pending'
                    if parallel_accounts(payload):
                        self._phase(state, 'running', 'Exact limit outcomes received independently')
                        return True
                    state['hybridRemainderStop'] = 'Hybrid limit was accepted on only some accounts; remainder stopped'
                    self._save(state)
                    raise ValueError(state['hybridRemainderStop'])
                if result.get('status') != 'ok' or set(results) != set(volumes) or any(
                        row.get('status') != 'ok' or not re.fullmatch(r'[1-9][0-9]{0,19}', str(row.get('providerOrderId', '')))
                        for row in results.values()):
                    raise ValueError('Hybrid limit placement is unconfirmed on an account')
                child['state'] = 'pending'
                self._phase(state, 'running', 'Hybrid limit remainder is reserved')
            return True
        except Exception:
            if child['state'] == 'sending':
                child['state'] = 'unknown'
            self._save(state)
            raise

    @staticmethod
    def _reconcile_token(state):
        return digest(dict(children=state['children'], accounts=state['accounts'],
            stopRevision=state.get('stopRevision'), protectionPending=state.get('protectionPending'),
            closurePending=state.get('closurePending')))

    def _adopt_support_price(self, payload, state, child, account, outcome):
        if outcome.get('status') != 'ok' or not outcome.get('supportRepriced'):
            return
        guard = child['guards'][account]
        if guard.get('supportReprice') is not True:
            raise ValueError('Unrequested native support repricing')
        price = number(outcome.get('submittedPrice'), 'Native support price')
        stop = number(outcome.get('sl'), 'Native support stop')
        take = number(outcome.get('tp', 0), 'Native support take', positive=False)
        lower, upper = market_price_bounds(payload)
        direction = 1 if payload['side'] == 'buy' else -1
        if (price < lower-1e-8 or price > upper+1e-8
                or direction*(price-guard['anchor']) > guard['maxGap']+1e-8
                or direction*(price-guard['maxRiskPrice']) > 1e-8
                or abs(stop-self._hybrid_stop(payload, state, account, price)) > 1e-8
                or abs(take-child['takes'][account]) > 1e-8):
            raise ValueError('Native support repricing exceeded captured bounds or protection')
        child['prices'][account], child['stops'][account] = price, stop
        if account == self.primary:
            child['price'] = price

    def _begin_reconciliation(self, payload, state, error):
        if (not callable(getattr(self.bridge, 'entry_reconcile', None))
                or not callable(getattr(self.bridge, 'warm_group_executor', None))
                or state.get('protectionPending') or state.get('closurePending')
                or any(row.get('commonStopUncertain') for row in state['accounts'].values())):
            return False
        targets = [child for child in state['children'] if child['state'] in ('sending','unknown','pending')
                   or child['id'] == state.get('missingNativeChild')]
        if not targets:
            return False
        resume = not state.get('hybridRemainderStop') or state.get('reconcileTransientHalt', False)
        state['reconciliation'] = dict(status='checking', startedAt=time.time()*1000,
            resumeAllowed=bool(resume and not self._stopping(state['id'])),
            reason=str(error)[:512], attempts=0)
        self._phase(state, 'paused', 'Checking exact broker execution; reserved volume remains locked')
        return True

    def _read_reconciliation(self, account, items):
        # A read already inside the MT5 extension cannot be preempted. Do not
        # queue a low-priority observer behind a trade or change quote cadence.
        if account == self.primary:
            if not self.storage.lock.acquire(blocking=False):
                return dict(deferred=True)
            try:
                return self.bridge.entry_reconcile(items)
            finally:
                self.storage.lock.release()
        worker = self.bridge.linked[account]
        idle = getattr(worker, 'call_if_idle', None)
        return idle('entry_reconcile', items) if callable(idle) else worker.call('entry_reconcile', items)

    def _native_opening_id(self, child, account):
        # Independent lanes send directly to the account journal. Only the
        # legacy grouped dispatcher appends the linked account suffix.
        return child['id'] if hasattr(self, 'lane_account') or account == self.primary else child['id']+':'+account

    def _settle_reconciled_opening(self, payload, child, account, outcome, view):
        if not outcome.get('nativeReconciled') or (view.get('receipt') or {}).get('status') != 'unknown':
            return
        position = child.get('positions', {}).get(account, {})
        expected = dict(id=self._native_opening_id(child, account),
            ticket=str(outcome['providerOrderId']), symbol=payload['symbol'], type=child['type'], side=payload['side'],
            volume=child['volumes'][account], price=position['fillPrice'] if child['type']=='market' else child['prices'][account],
            sl=position['sl'] if child['type']=='market' else child['stops'][account],
            tp=position['tp'] if child['type']=='market' else child['takes'][account])
        result = self._call(account, 'entry_confirm_reconciled', expected)
        if result.get('status')!='ok' or str(result.get('providerOrderId'))!=expected['ticket']:
            raise ValueError('Native opening confirmation did not settle its exact journal')

    def _reconcile_tick(self, payload, state):
        status = state['reconciliation']
        if (self._stopping(state['id']) or time.time()*1000 >= payload['closeAtUtc']
                or state.get('hybridRemainderStop') and not state.get('reconcileTransientHalt')):
            status['resumeAllowed'] = False
        job = self.reconcile_jobs.get(state['id'])
        if job is None:
            if time.monotonic() < self.reconcile_poll_at.get(state['id'], 0):
                return
            targets = [child for child in state['children'] if child['state'] in ('sending','unknown','pending')
                       or child['id'] == state.get('missingNativeChild')][:16]
            if not targets:
                raise ValueError('Reconciliation lost its captured opening identities')
            items = {}
            for account in state['accounts']:
                for child in targets:
                    if account not in child['volumes']:
                        continue
                    outcome = self._limit_results(state, child).get(account, {})
                    cancellation = child.get('cancellations', {}).get(account)
                    cancel_id = child.get('cancelIds', {}).get(account) or self._hybrid_cancel_id(child, account)
                    items.setdefault(account, []).append(dict(id=self._native_opening_id(child, account),
                        ticket=str(outcome.get('providerOrderId') or ''), symbol=payload['symbol'],
                        cancelIds=[cancel_id] if cancellation else []))
            executor = self.bridge.warm_group_executor()
            status['attempts'] = min(1000000, status.get('attempts', 0)+1)
            token = self._reconcile_token(state)
            futures = {}
            try:
                for account in items:
                    futures[account] = executor.submit(self._read_reconciliation, account, copy.deepcopy(items[account]))
            except Exception:
                # Keep any already submitted reads; a later tick joins them.
                pass
            self.reconcile_jobs[state['id']] = dict(token=token, targets=[row['id'] for row in targets],
                items=items, futures=futures)
            for future in futures.values():
                future.add_done_callback(self._read_finished)
            return
        if any(not future.done() for future in job['futures'].values()):
            return
        del self.reconcile_jobs[state['id']]
        self.reconcile_poll_at[state['id']] = time.monotonic()+.25
        if job['token'] != self._reconcile_token(state):
            return  # A stop/protection changed the captured generation.
        try:
            data = {account: future.result() for account, future in job['futures'].items()}
            if set(data) != set(job['items']):
                return
            now = time.time()*1000
            for account, row in data.items():
                if row.get('deferred'):
                    return
                if (str(row.get('account')) != account or row.get('server') != self.bridge.config['server']
                        or row.get('readOnly') is not True or not isinstance(row.get('items'), dict)
                        or not -250 <= now-number(row.get('checkedAt'), 'Native reconciliation time') <= 3000):
                    raise ValueError('Native reconciliation account, freshness or identity mismatch')
            unsettled = False
            for child in state['children']:
                if child['id'] not in job['targets']:
                    continue
                outcomes, views = {}, {}
                original = self._limit_results(state, child)
                for account in child['volumes']:
                    identifier = self._native_opening_id(child, account)
                    view = data[account]['items'].get(identifier)
                    if not isinstance(view, dict):
                        raise ValueError('Native opening proof is incomplete')
                    saved = view.get('receipt') or {}
                    previous = original.get(account, {})
                    if saved.get('status') not in ('ok','rejected') and previous.get('status') in ('ok','rejected'):
                        saved = previous
                    if saved.get('status') not in ('ok','rejected'):
                        # Positive exact broker rows can settle an ambiguous
                        # reply. Empty rows alone can never prove rejection.
                        ticket = str(view.get('ticket', ''))
                        positions, pending = view.get('positions', []), view.get('pending', [])
                        proof = view.get('terminal', {}).get('openingProof', {})
                        if (not re.fullmatch(r'[1-9][0-9]{0,19}', ticket) or
                                not (positions or pending or proof.get('kind') == 'exact-balanced-opening-v1'
                                     or child['type']=='limit' and view.get('terminal',{}).get('phase')=='cancelled')):
                            unsettled = True
                            continue
                        saved = {**saved, 'status':'ok', 'providerOrderId':ticket, 'nativeReconciled':True}
                        if child['type'] == 'market':
                            if len(positions) == 1 and not pending:
                                saved.update(filledVolume=number(positions[0]['lots'], 'Native fill volume'),
                                             fillPrice=number(positions[0]['open_price'], 'Native fill price'))
                            elif proof.get('kind') == 'exact-balanced-opening-v1':
                                saved.update(filledVolume=proof['volume'], fillPrice=proof['fillPrice'])
                            else:
                                unsettled = True
                                continue
                    if (previous.get('status') == 'rejected' and saved.get('status') != 'rejected'
                            or previous.get('providerOrderId') and str(previous['providerOrderId']) != str(saved.get('providerOrderId'))):
                        raise ValueError('Native receipt contradicts its captured opening')
                    outcomes[account], views[account] = hybrid_receipt(saved), view
                if len(outcomes) != len(child['volumes']):
                    unsettled = True
                    continue
                child['reconciledResult'] = dict(status='ok' if all(row.get('status') == 'ok' for row in outcomes.values()) else
                    'rejected' if all(row.get('status') == 'rejected' for row in outcomes.values()) else 'unknown', accountResults=outcomes)
                if child['type'] == 'market':
                    if any(row['status'] == 'rejected' and not self._retryable_market_protection_refusal(
                            payload, child, row) for row in outcomes.values()):
                        state['hybridRemainderStop'] = 'Hybrid market was accepted on only some accounts; remainder stopped'
                        status['resumeAllowed'] = False
                    for account, outcome in outcomes.items():
                        if outcome['status'] == 'rejected':
                            if self._retryable_market_protection_refusal(payload, child, outcome):
                                quote = data[account].get('quotes', {}).get(payload['symbol'], {})
                                state['accounts'][account].setdefault('priceRejections', {})['market'] = dict(
                                    quote=[quote.get('bid'), quote.get('ask')], volume=child['volumes'][account],
                                    at=now, retryMs=250, waitForQuoteChange=True)
                            continue
                        position = self._confirmed_position(payload, state, child, account, outcome)
                        child.setdefault('positions', {})[account] = position
                        self._settle_reconciled_opening(payload, child, account, outcome, views[account])
                        if position.get('closedAtBroker'):
                            child.setdefault('closedOpenings', {})[account] = dict(openingTicket=position['openingTicket'], positionId=position['ticket'])
                            state['hybridRemainderStop'] = 'Captured opening is confirmed fully closed; remainder stopped'
                            status['resumeAllowed'] = False
                            state.pop('reconcileTransientHalt', None)
                    if child['state'] not in ('confirmed','rejected'):
                        state['nextChunk'] += 1
                    child['state'] = 'confirmed' if any(row['status'] == 'ok' for row in outcomes.values()) else 'rejected'
                else:
                    pending = False
                    for account, outcome in outcomes.items():
                        if outcome['status'] == 'rejected':
                            if not self._optional_support_refusal(payload, state, child, account, outcome):
                                state['hybridRemainderStop'] = 'Hybrid limit was accepted on only some accounts; remainder stopped'
                                status['resumeAllowed'] = False
                            continue
                        view = views[account]
                        self._adopt_support_price(payload, state, child, account, outcome)
                        cancel_id = child.get('cancelIds', {}).get(account) or self._hybrid_cancel_id(child, account)
                        latest = view.get('cancellations', {}).get(cancel_id)
                        current_cancel = child.get('cancellations', {}).get(account)
                        if (current_cancel and current_cancel.get('status') in ('claimed','unknown')
                                and isinstance(latest, dict) and latest.get('status') in ('ok','rejected')):
                            child['cancellations'][account] = hybrid_receipt(latest)
                        current = dict(account=account, **{key:view.get(key, []) if key != 'terminal' else view.get(key, {})
                            for key in ('pending','positions','terminal')})
                        live, _ = self._hybrid_apply_limit_view(payload, state, child, account,
                            str(outcome.get('providerOrderId')), current, cancel=not bool(current['pending']), retain_pending_fill=True)
                        self._settle_reconciled_opening(payload, child, account, outcome, view)
                        pending |= live
                    child['state'] = ('rejected' if all(row['status']=='rejected' for row in outcomes.values())
                        else 'pending' if pending else 'confirmed' if child.get('positions') else 'cancelled')
                    if pending and child.get('cancellations'):
                        if not self._allow_frozen_cancel_retry(payload, state, child, data):
                            unsettled = True
                # Publish final native receipts consistently. Original replies
                # remain in both journals for audit, never used to resend.
                if child.get('result', {}).get('status') == 'unknown':
                    if hasattr(self, 'lane_account'):
                        native = self._call(self.lane_account, 'receipt', self._native_opening_id(child, self.lane_account))['result']
                        final = dict(native, accountResults={self.lane_account:copy.deepcopy(native)})
                    elif len(state['accounts']) > 1:
                        final = self.bridge.entry_confirm_group(child['id'])
                    else:
                        final = self.bridge.receipt(child['id'])['result']
                    child.setdefault('originalResult', child['result'])
                    child['result'] = child['reconciledResult'] = hybrid_receipt(final)
                self._hybrid_recount(payload, state, allow_excess_risk=True)
                self._save(state)
            if unsettled or any(child['state'] in ('sending','unknown') for child in state['children']):
                return
            status.update(status='confirmed', checkedAt=now)
            state.pop('missingNativeChild', None)
            if status.get('resumeAllowed') and not self._stopping(state['id']):
                if state.pop('reconcileTransientHalt', False):
                    state.pop('hybridRemainderStop', None)
            else:
                state.setdefault('hybridRemainderStop', 'Further entry stopped; exact broker state confirmed')
            self._phase(state, 'running', 'Exact broker execution confirmed; checking remaining volume')
        except Exception as error:
            # Native reads can fail or arrive late; keep both reservations.
            # No task in this path sends, cancels, closes or changes a stop.
            status['reason'] = str(error)[:512]
            if time.time()*1000-status.get('checkedAt', 0) >= 1000:
                status['checkedAt'] = time.time()*1000
                self._save(state)

    def _allow_frozen_cancel_retry(self, payload, state, child, data):
        pending_accounts = []
        for account in child['volumes']:
            identifier = self._native_opening_id(child, account)
            if data[account]['items'][identifier].get('pending'):
                pending_accounts.append(account)
        quotes = {}
        for account in pending_accounts:
            receipt = child.get('cancellations', {}).get(account, {})
            quote = data[account].get('quotes', {}).get(payload['symbol'], {})
            not_sent = receipt.get('dispatchNotSent') is True
            if (receipt.get('status') != 'rejected' or not (receipt.get('retcode') == 10029 or not_sent)
                    or child.get('cancelAttempts', {}).get(account, 0) >= 100
                    or not -250 <= time.time()*1000-number(quote.get('tickUtcMs'), 'Retry quote time') <= 1000):
                return False
            quotes[account] = [number(quote.get('bid'), 'Retry Bid'), number(quote.get('ask'), 'Retry Ask')]
        old = child.get('frozenCancelQuotes')
        if old is None:
            child['frozenCancelQuotes'] = quotes
            return False
        if any(old.get(account) == quote and child['cancellations'][account].get('dispatchNotSent') is not True
               for account, quote in quotes.items()):
            return False
        child['frozenCancelQuotes'] = quotes
        for account in pending_accounts:
            receipt = child['cancellations'].pop(account)
            history = child.setdefault('cancelHistory', {}).setdefault(account, [])
            history.append(receipt)
            del history[:-8]
            attempts = child.setdefault('cancelAttempts', {})
            attempts[account] = attempts.get(account, 0)+1
        return True

    def _independent_status(self, state, account, status, reason):
        details = state['accounts'][account]
        changed = (details.get('entryStatus'), details.get('entryReason')) != (status, reason)
        details.update(entryStatus=status, entryReason=reason, lastEntryCheckAt=time.time()*1000)
        if changed:
            self._save(state)

    def _independent_snapshot(self, payload, state, account):
        data, bid, ask, point = self._entry_snapshot(account, payload['symbol'])
        if abs(point-state['accounts'][account]['pointValue']) > 1e-8:
            raise ValueError('Instrument USD point value changed')
        return data, bid, ask

    def _independent_sync_stop(self, payload, state, account):
        try:
            self._hybrid_sync_stop(payload, state, accounts={account})
            return True
        except QuoteWait as error:
            self._independent_status(state, account, 'waiting-quote', str(error))
            return False
        except CommonStopWait as error:
            self._independent_status(state, account, 'checking', str(error))
            return False

    def _priority_volume(self, payload, state, account, kind, price, volume, reserved=None):
        details = state['accounts'][account]
        if fill_priority(payload) == 'risk' and payload.get('initialSizingOnly') is not True:
            direction = 1 if payload['side']=='buy' else -1
            lower, upper = market_price_bounds(payload)
            worst = (upper if direction == 1 else lower) if kind == 'market' else price
            distance = self._hybrid_distance(payload,state,account)
            cost = (distance if distance is not None else direction*(worst-self._hybrid_stop(payload,state,account,price)))*details['pointValue']
            budget = max(0.,details['riskUsd']-details['riskUsed']-(reserved or {}).get(account,0.))
            if cost <= 0:
                raise ValueError('New entry must remain before the captured stop')
            step = Decimal(str(details['spec']['quantityStep']))
            cap = float((Decimal(str(budget/cost))/step+Decimal('1e-9')).to_integral_value(rounding=ROUND_FLOOR)*step)
            volume = min(volume,cap)
            if volume < details['spec']['minQty']-1e-8:
                return 0.
        return volume

    def _independent_send(self, payload, state, account, kind, price, volume, bid, ask, reserved=None):
        """One durable reservation, one account, no retry of an unknown result."""
        details = state['accounts'][account]
        volume = self._priority_volume(payload,state,account,kind,price,volume,reserved)
        if volume <= 0:
            self._independent_status(state,account,'waiting-risk','Remaining risk does not permit the broker minimum; waiting for a permitted price')
            return False
        rejections = details.setdefault('priceRejections', {})
        last = rejections.get(kind, {})
        if last and last.get('volume') == volume and (last.get('quote') == [bid, ask] or time.time()*1000-last['at'] < 250):
            self._independent_status(state, account, 'waiting-price', 'Waiting for a changed permitted quote')
            return False
        prices, volumes = {account: price}, {account: volume}
        before = len(state['children'])
        sent = self._hybrid_send(payload, state, kind, prices, volumes,
            self._hybrid_guards(payload, state, prices, volumes, reserved=reserved, support=kind == 'limit'), support=kind == 'limit')
        child = state['children'][-1]
        if len(state['children']) != before+1:
            raise ValueError('Independent entry reservation changed')
        if not sent and child.get('retryablePriceRejection'):
            rejections[kind] = dict(quote=[bid, ask], volume=volume, at=time.time()*1000)
            self._independent_status(state, account, 'waiting-price', str(child['result'].get('reason', 'Price rejected'))[:256])
            return False
        rejections.pop(kind, None)
        if not sent:
            raise ValueError(child['result'].get('reason', 'Account entry was rejected'))
        self._independent_status(state, account, 'limit' if kind == 'limit' else 'filled',
                                 'Return limit reserved at broker' if kind == 'limit' else 'Market part confirmed')
        return True

    def _tick_independent_hybrid(self, payload, state):
        """Support first with reserved market risk; independent account residuals.

        Old parents never enter this policy. Per-account sends keep the existing
        durable intent, native final price guard and exact broker proof. An
        unknown submission, cancellation or protection still locks the parent.
        """
        started = time.perf_counter()
        try:
            if state.get('reconciliation', {}).get('status') == 'checking':
                self._reconcile_tick(payload, state)
                return
            if state.get('protectionPending') or state.get('closurePending') or state.get('protectionPause'):
                return
            if any(child['state'] in ('sending', 'unknown') for child in state['children']):
                raise ValueError('Hybrid submission is unconfirmed; automatic repeat is blocked')
            if state.get('hybridRemainderStop') or self._stopping(state['id']) or time.time()*1000 >= payload['closeAtUtc']:
                if state.get('protectionOnlyWait'):
                    for account in state['accounts']:
                        self._independent_snapshot(payload, state, account)
                safe = self._hybrid_cancel_limits(payload, state)
                self._phase(state, 'stopped' if safe else 'unknown', 'Hybrid remainder stopped; existing positions retained')
                return
            # First consume actual broker fills. A resting pending reservation
            # blocks every other send on its account until its exact fate is known.
            poll_at = time.monotonic()
            for child in list(state['children']):
                if child['type'] != 'limit' or child['state'] != 'pending':
                    continue
                if poll_at < self.pending_poll_at.get(child['id'], 0):
                    continue
                self.pending_poll_at[child['id']] = poll_at + .25
                before = sum(row.get('volume', 0.) for row in child.get('positions', {}).values())
                active = self._hybrid_limit(payload, state, child, cancel=False)
                after = sum(row.get('volume', 0.) for row in child.get('positions', {}).values())
                if active and after > before+1e-8:
                    # A partial fill changes the weighted stop. Resolve only its
                    # own remainder before changing protection or rebuilding it.
                    self._hybrid_limit(payload, state, child, cancel=True)
            self._positions(payload, state)
            if state.get('hybridRemainderStop'):
                raise ValueError(state['hybridRemainderStop'])
            if self._hybrid_fill_violation(state):
                raise ValueError('Actual execution exceeded the allowed price gap or remaining risk; further entry stopped, existing positions retained')
            lower, upper = market_price_bounds(payload)
            direction = 1 if payload['side'] == 'buy' else -1
            for account, details in state['accounts'].items():
                if self._protecting(state['id']) or self._stopping(state['id']):
                    return
                pending = [child for child in state['children'] if child['type'] == 'limit'
                           and child['state'] == 'pending' and account in child['volumes']]
                try:
                    _, bid, ask = self._independent_snapshot(payload, state, account)
                except QuoteWait as error:
                    self._independent_status(state, account, 'waiting-quote', str(error))
                    continue
                closing_quote = bid if direction == 1 else ask
                for child in state['children']:
                    position = child.get('positions', {}).get(account)
                    if position and (direction*(closing_quote-position['sl']) <= 0
                            or position.get('tp') and direction*(closing_quote-position['tp']) >= 0):
                        raise ValueError('Captured SL or TP reached; further hybrid entry stopped')
                if not pending and not self._independent_sync_stop(payload, state, account):
                    continue
                if details['filledVolume'] >= details['targetVolume']-1e-8:
                    self._independent_status(state, account, 'completed', 'Target volume confirmed')
                    continue
                quote = ask if direction == 1 else bid
                inside = lower-1e-8 <= quote <= upper+1e-8
                adverse = direction*(quote-(upper if direction == 1 else lower)) > 1e-8
                residual = rounded(details['targetVolume']-details['filledVolume'])
                reserved_volume = sum(max(0., child['volumes'][account]-child.get('positions', {}).get(account, {}).get('volume', 0.)) for child in pending)
                reserved_risk = sum(max(0., child['volumes'][account]-child.get('positions', {}).get(account, {}).get('volume', 0.))
                    * max(0., direction*(child['prices'][account]-child['stops'][account]))*details['pointValue'] for child in pending)
                free = rounded(residual-reserved_volume)
                if pending and not inside:
                    self._independent_status(state, account, 'limit', 'Return limit reserved at broker')
                    continue
                if pending and free < details['spec']['minQty']-1e-8:
                    # Price returned inside: release the old reservation only
                    # after exact cancellation/fill proof, then resize the
                    # support for the next market part from the actual residue.
                    for child in pending:
                        self._hybrid_limit(payload, state, child, cancel=True)
                    if not self._independent_sync_stop(payload, state, account):
                        continue
                    residual = rounded(details['targetVolume']-details['filledVolume'])
                    if residual <= 1e-8:
                        self._independent_status(state, account, 'completed', 'Target volume confirmed')
                        continue
                    pending, reserved_risk, free = [], 0., residual
                    try:
                        _, bid, ask = self._independent_snapshot(payload, state, account)
                    except QuoteWait as error:
                        self._independent_status(state, account, 'waiting-quote', str(error))
                        continue
                    quote = ask if direction == 1 else bid
                    inside = lower-1e-8 <= quote <= upper+1e-8
                    adverse = direction*(quote-(upper if direction == 1 else lower)) > 1e-8
                if inside:
                    spec = details['spec']
                    volume = min(free, details['parts'][0], spec['maxQty'])
                    volume = float((Decimal(str(volume))/Decimal(str(spec['quantityStep']))+Decimal('1e-9')).to_integral_value(rounding=ROUND_FLOOR)*Decimal(str(spec['quantityStep'])))
                    if 1e-8 < free-volume < spec['minQty']-1e-8:
                        volume = rounded(free-spec['minQty'])
                    if volume < spec['minQty']-1e-8:
                        raise ValueError('Hybrid residual cannot satisfy the broker minimum lot on every account')
                    support_volume = min(rounded(residual-volume), spec['maxQty'])
                    if not pending and support_volume >= spec['minQty']-1e-8:
                        support_price = max(lower, bid) if direction == 1 else min(upper, ask)
                        distance = self._hybrid_distance(payload, state, account)
                        worst_price = upper if direction == 1 else lower
                        market_risk = volume*max(0., distance if distance is not None else direction*(worst_price-self._hybrid_stop(payload,state,account,quote)))*details['pointValue']
                        accepted = self._independent_send(payload, state, account, 'limit', support_price,
                            support_volume, bid, ask, reserved={account:market_risk})
                        if accepted:
                            child = state['children'][-1]
                            pending = [child]
                            support_volume = child['volumes'][account]
                            reserved_risk = support_volume*max(0., direction*(child['prices'][account]-child['stops'][account]))*details['pointValue']
                        # The market uses another fresh native snapshot after
                        # the pending intent. A definite price rejection of the
                        # support does not consume its market reservation.
                        try:
                            _, bid, ask = self._independent_snapshot(payload, state, account)
                        except QuoteWait as error:
                            self._independent_status(state, account, 'waiting-quote', str(error))
                            continue
                        quote = ask if direction == 1 else bid
                        if not lower-1e-8 <= quote <= upper+1e-8:
                            self._independent_status(state, account, 'limit' if pending else 'waiting-price',
                                'Return limit reserved at broker' if pending else 'No valid market price inside the captured zone')
                            continue
                    if not self._independent_send(payload, state, account, 'market', quote, volume, bid, ask,
                                                 reserved={account:reserved_risk}):
                        if state['phase'] not in ACTIVE:
                            return
                        continue
                    if state['phase'] not in ACTIVE or state.get('hybridRemainderStop'):
                        raise ValueError(state.get('reason', 'Hybrid remainder stopped'))
                    if pending:
                        try:
                            _, bid, ask = self._independent_snapshot(payload, state, account)
                        except QuoteWait as error:
                            self._independent_status(state, account, 'waiting-quote', str(error))
                            continue
                        quote = ask if direction == 1 else bid
                        if lower-1e-8 <= quote <= upper+1e-8:
                            for child in pending:
                                self._hybrid_limit(payload, state, child, cancel=True)
                            if not self._independent_sync_stop(payload, state, account):
                                continue
                            self._independent_status(state, account,
                                'completed' if details['filledVolume'] >= details['targetVolume']-1e-8 else 'filled',
                                'Target volume confirmed' if details['filledVolume'] >= details['targetVolume']-1e-8 else 'Market part confirmed')
                        else:
                            self._independent_status(state, account, 'limit', 'Return limit reserved at broker')
                        continue
                    if not self._independent_sync_stop(payload, state, account):
                        continue
                    residual = rounded(details['targetVolume']-details['filledVolume'])
                    if residual <= 1e-8:
                        self._independent_status(state, account, 'completed', 'Target volume confirmed')
                        continue
                    try:
                        _, bid, ask = self._independent_snapshot(payload, state, account)
                    except QuoteWait as error:
                        self._independent_status(state, account, 'waiting-quote', str(error))
                        continue
                    quote = ask if direction == 1 else bid
                    adverse = direction*(quote-(upper if direction == 1 else lower)) > 1e-8
                # Above a buy zone (below a sell zone), clamp to the inclusive
                # outer edge. Native repricing moves inward only if broker
                # distance requires it. Below/above the opposite edge a valid
                # limit may not exist: never turn it into an out-of-zone market.
                if adverse:
                    price = upper if direction == 1 else lower
                elif lower-1e-8 <= quote <= upper+1e-8:
                    price = max(lower, bid) if direction == 1 else min(upper, ask)
                else:
                    self._independent_status(state, account, 'waiting-price', 'No valid limit inside the captured zone')
                    continue
                volume = min(residual, details['spec']['maxQty'])
                if volume < details['spec']['minQty']-1e-8:
                    raise ValueError('Hybrid residual cannot satisfy the broker minimum lot on every account')
                self._independent_send(payload, state, account, 'limit', price, volume, bid, ask)
                if state['phase'] not in ACTIVE:
                    return
            complete = all(row['filledVolume'] >= row['targetVolume']-1e-8 and row.get('entryStatus') == 'completed'
                           for row in state['accounts'].values())
            self._phase(state, 'completed' if complete else 'running',
                        'Hybrid target confirmed with a common stop on every account' if complete else 'Independent account entry is active')
        except Exception as error:
            if any(child['state'] in ('sending', 'unknown') or any(
                    row.get('status') in ('claimed','unknown') for row in child.get('cancellations', {}).values())
                    for child in state['children']) and self._begin_reconciliation(payload, state, error):
                return
            if isinstance(error, QuoteWait) and state.get('protectionOnlyWait'):
                state['quoteWait'] = error.details
                self._phase(state, 'paused', str(error))
                return
            try:
                safe = self._hybrid_cancel_limits(payload, state)
            except QuoteWait as deferred:
                state['quoteWait'] = deferred.details
                self._phase(state, 'paused', str(deferred))
                return
            except Exception:
                safe = False
            self._phase(state, 'stopped' if safe else 'unknown', str(error))
        finally:
            state['lastCycleMs'] = round((time.perf_counter()-started)*1000, 3)

    def _parallel_note(self, state, account, phase, reason, **metrics):
        """Volatile bounded timing/status; trading claims still commit normally."""
        now = time.monotonic()
        clock = self.entry_clocks.setdefault((state['id'], account), dict(start=now, phase=phase, since=now))
        if self.entry_diagnostics and self.entry_diagnostics.active:
            key=(phase,reason)
            changed=clock.get('diagnosticStage')!=key
            reportable=(phase not in ('ready','waiting-read') or phase=='waiting-read' and metrics.get('readWaitMs',0)>=100)
            if reportable and (changed or phase.startswith('waiting-') and now-clock.get('diagnosticAt',0)>=5):
                if changed and clock.get('diagnosticStage'):
                    previous=clock['diagnosticStage']
                    self.entry_diagnostics.emit('stage-ended',account=account,entry_id=state['id'],symbol=state.get('symbol'),
                        stage=previous[0],reason=previous[1],duration_ms=(now-clock['diagnosticStarted'])*1000)
                if changed:clock['diagnosticStarted']=now
                self.entry_diagnostics.emit('stage-started' if changed else 'waiting',account=account,
                    entry_id=state['id'],symbol=state.get('symbol'),stage=phase,reason=reason,
                    duration_ms=(now-clock['diagnosticStarted'])*1000,**metrics)
                clock.update(diagnosticStage=key,diagnosticAt=now)
        trace = clock.setdefault('trace', []) if hasattr(self, 'lane_account') else None
        if clock['phase'] != phase:
            if trace is not None:
                trace.append(dict(phase=clock['phase'], ended=True,
                    durationMs=round((now-clock['since'])*1000, 3)))
            clock.update(phase=phase, since=now)
        details = state['accounts'][account]
        if not (details.get('entryStatus') == 'completed' and phase in ('ready', 'waiting-read', 'waiting-quote')):
            details.update(entryStatus=phase, entryReason=reason)
        details['lastEntryCheckAt'] = time.time()*1000
        timing = details.setdefault('entryTiming', {})
        timing.update(phase=phase, phaseElapsedMs=round((now-clock['since'])*1000, 3), **metrics)
        try:
            totals = clock.get('waitTotals')
            if totals is None:
                totals = clock['waitTotals'] = EntryWaitTotals()
            totals.observe(phase, EntryDiagnostics.text(reason, 160), now*1000, metrics.get('quoteAgeMs'))
            if now-clock.get('waitTotalsPublishedAt', float('-inf')) >= .25 or phase in totals.TERMINAL:
                timing['waitSummary'] = copy.deepcopy(totals.data)
                clock['waitTotalsPublishedAt'] = now
        except Exception:
            pass  # Diagnostic accounting adds no trading dependency or reads.
        if hasattr(self, 'lane_account'):
            timing['recentStages'] = trace
            if not trace or trace[-1]['phase'] != phase or now-clock.get('lastTrace', 0) >= 1:
                trace.append(dict(phase=phase, sinceAccountStartMs=round((now-clock['start'])*1000, 3),
                    durationMs=timing['phaseElapsedMs'], **metrics))
                del trace[:-48]
                clock['lastTrace'] = now

    def _parallel_quotes(self, payload, state):
        """Only immutable read results cross threads; coordinator owns all state."""
        self.retired_snapshot_jobs = {future for future in self.retired_snapshot_jobs if not future.done()}
        executor = self.bridge.warm_group_executor()
        for account in state['accounts']:
            key = (state['id'], account)
            if key not in self.snapshot_jobs:
                watch = [dict(account=account, ticket=row['openingTicket'], symbol=payload['symbol'])
                         for child in state['children'] for row in [child.get('positions', {}).get(account)]
                         if row and not row.get('closed') and account not in child.get('closedOpenings', {})]
                queued = time.monotonic()
                def read(account=account, watch=watch, queued=queued):
                    started = time.monotonic()
                    if hasattr(self, 'lane_account'):
                        context = self._call(account, 'entry_context', payload['symbol'], watch)
                        finished = time.monotonic()
                        return context['snapshot'], context['proof'], dict(contextMs=context['contextMs'],
                            readQueueMs=round((started-queued)*1000, 3),readWorkMs=round((finished-started)*1000, 3),
                            _readFinishedAt=finished,
                            workerLockWaitMs=context.get('rpcTiming', {}).get('lockWaitMs'),
                            workerRpcMs=context.get('rpcTiming', {}).get('totalMs'))
                    snapshot = self._call(account, 'entry_snapshot', payload['symbol'])
                    snapshot_ms = (time.monotonic()-started)*1000
                    proof_started = time.monotonic()
                    proof = self._call(account, 'entry_management', watch) if watch else None
                    finished = time.monotonic()
                    return snapshot, proof, dict(snapshotMs=round(snapshot_ms, 3),
                        readQueueMs=round((started-queued)*1000, 3),readWorkMs=round((finished-started)*1000, 3),
                        _readFinishedAt=finished,positionProofMs=round((finished-proof_started)*1000, 3),
                        workerLockWaitMs=snapshot.get('rpcTiming', {}).get('lockWaitMs'),
                        workerRpcMs=snapshot.get('rpcTiming', {}).get('totalMs'))
                self.snapshot_jobs[key] = (queued, executor.submit(read))
                self.snapshot_jobs[key][1].add_done_callback(self._read_finished)
        jobs = [self.snapshot_jobs[(state['id'], account)][1] for account in state['accounts']]
        if jobs and not all(future.done() for future in jobs):
            # Coalesce near-simultaneous ready reads for at most 2ms; a slow
            # account never makes a ready peer wait for its full RPC timeout.
            wait(jobs, timeout=.002)
        ready, evidence = {}, {}
        for account, details in state['accounts'].items():
            key = (state['id'], account)
            started, future = self.snapshot_jobs[key]
            elapsed = round((time.monotonic()-started)*1000, 3)
            if not future.done():
                self._parallel_note(state, account, 'waiting-read', 'Waiting for this account native snapshot', readWaitMs=elapsed)
                continue
            del self.snapshot_jobs[key]
            timing = {}
            try:
                snapshot, proof, timing = future.result()
                timing = dict(timing)
                finished = timing.pop('_readFinishedAt', None)
                if finished is not None:
                    timing['readResumeMs'] = round(max(0., time.monotonic()-finished)*1000, 3)
                _, bid, ask, point = self._quote_snapshot(account, snapshot)
                if abs(point-details['pointValue']) > 1e-8:
                    raise ValueError('Instrument USD point value changed')
                if proof is not None and (str(proof.get('account')) != account
                        or time.time()*1000-number(proof.get('checkedAt'), 'Position evidence time') > 1000):
                    raise ValueError('Position proof is not fresh or belongs to another account')
                ready[account] = (bid, ask)
                evidence[account] = proof
                details['latestQuoteProof'] = dict(tickUtcMs=snapshot['quote']['utcTime']*1000,
                    capturedAtMs=snapshot['quote'].get('nativeCapturedAt', 0))
                self._parallel_note(state, account, 'ready', 'Fresh native account snapshot', readWaitMs=elapsed,
                    quoteAgeMs=round(time.time()*1000-snapshot['quote']['utcTime']*1000, 3), **timing)
            except QuoteWait as error:
                self._parallel_note(state, account, 'waiting-quote', str(error), readWaitMs=elapsed,
                    quoteAgeMs=error.details['ageMs'], **timing)
            except Exception as error:
                # A failed read has no mutation outcome to replay. It never
                # releases an existing pending reservation or position lock.
                if self.entry_diagnostics:
                    self.entry_diagnostics.emit('validation-error',account=account,entry_id=state['id'],symbol=state.get('symbol'),
                        stage='snapshot-validation',duration_ms=elapsed,error=error)
                self._parallel_note(state, account, 'waiting-read', str(error)[:256], readWaitMs=elapsed, **timing)
        return ready, evidence

    def _parallel_pending(self, state, account):
        return [child for child in state['children'] if child['type'] == 'limit' and account in pending_accounts(child)]

    def _parallel_cancel(self, payload, state, child, accounts):
        started = time.monotonic()
        self._hybrid_limit(payload, state, child, cancel=True, accounts=accounts)
        for account in accounts:
            self._parallel_note(state, account, 'ready', 'Exact cancellation/fill proof received',
                                lastCancelMs=round((time.monotonic()-started)*1000, 3))

    @staticmethod
    def _retryable_market_protection_refusal(payload, child, result):
        # A definite refusal of a NEW opening is different from failure to
        # protect an existing position. Never relax the latter's halt.
        details = result.get('entryDiagnostics', {})
        local = (result.get('retcode') is None and isinstance(details, dict)
            and details.get('rejectionStage') == 'final-native-protection-guard'
            and details.get('mutationStarted') is False)
        return (child['type']=='market' and fill_priority(payload) in ('risk','volume')
            and result.get('status')=='rejected' and (result.get('retcode')==10016 or local)
            and result.get('providerOrderId') in (None,0,'0','')
            and result.get('filledVolume',0) in (None,0,0.))

    @staticmethod
    def _optional_support_refusal(payload, state, child, account, result):
        return (child['type']=='limit' and child.get('support') is True
            and (fill_priority(payload) in ('risk','volume')
                 or state['accounts'][account].get('supportOptionalAfterRefusal') is True)
            and result.get('status')=='rejected' and result.get('retcode')==10016
            and result.get('providerOrderId') in (None,0,'0','')
            and result.get('filledVolume',0) in (None,0,0.))

    @staticmethod
    def _parallel_price_refusal(result):
        # Native retcodes take precedence over localized broker comments.
        code = result.get('retcode')
        if code is not None:
            return code in (10004, 10015, 10020, 10021, 10024)
        if result.get('entryDiagnostics', {}).get('mutationStarted') is False:
            return True
        reason = str(result.get('reason', '')).lower()
        return any(text in reason for text in ('chunk tick is stale', 'no fresh chunk quote',
            'price moved outside', 'limit requires', 'broker quote is stale'))

    @staticmethod
    def _parallel_retry_ready(state, account, kind, quote=None, volume=None):
        last = state['accounts'][account].get('priceRejections', {}).get(kind)
        if last and kind == 'market' and last.get('fastQuoteProof'):
            rejected, latest = last['fastQuoteProof'], state['accounts'][account].get('latestQuoteProof', {})
            captured, tick = latest.get('capturedAtMs'), latest.get('tickUtcMs')
            if (isinstance(captured, (int, float)) and isinstance(tick, (int, float))
                    and math.isfinite(captured) and math.isfinite(tick)
                    and captured > rejected['checkedAtMs']
                    and quote is not None and (quote != rejected['quote'] or tick > rejected['tickUtcMs'])):
                return True
        if (last and last.get('waitForQuoteChange')
                and (quote is None or last.get('quote') == quote)
                and (volume is None or last.get('volume') == volume)
                and time.time()*1000-last['at']<last.get('recheckMs',1000)):
            return False
        # A structurally unavailable support price cannot improve on an
        # unchanged Bid/Ask. Keep that wait without consuming child IDs; the
        # separate market reservation may still proceed after its own check.
        if (last and kind == 'limit' and last.get('quote') == quote and last.get('volume') == volume
                and (state.get('initialSizingOnly') is not True or time.time()*1000-last['at']<1000)):
            return False
        return not last or time.time()*1000-last['at'] >= last.get('retryMs', 250)

    def _parallel_batch(self, payload, state, kind, plans):
        """One durable batch reserves ready accounts, then native sends overlap."""
        selected = {}
        for account, plan in plans.items():
            price, volume, bid, ask, reserved = plan
            volume = self._priority_volume(payload,state,account,kind,price,volume,{account:reserved})
            if volume <= 0:
                self._parallel_note(state,account,'waiting-risk','Remaining risk does not permit the broker minimum; waiting for a permitted price')
                continue
            if time.monotonic() < self.entry_clocks.get((state['id'],account),{}).get('transportRetryAt',0):
                continue
            if not self._parallel_retry_ready(state, account, kind, [bid, ask], volume):
                self._parallel_note(state, account, 'waiting-price', 'Waiting for a fresh permitted price check')
                continue
            selected[account] = (price, volume, bid, ask, reserved)
        if not selected:
            return False
        prices = {account: row[0] for account, row in selected.items()}
        volumes = {account: row[1] for account, row in selected.items()}
        reserved = {account: row[4] for account, row in selected.items()}
        self._hybrid_send(payload, state, kind, prices, volumes,
            self._hybrid_guards(payload, state, prices, volumes, reserved=reserved, support=kind == 'limit'),
            support=kind == 'limit')
        child = state['children'][-1]
        results = self._limit_results(state, child)
        for account, (_, volume, bid, ask, _) in selected.items():
            result = results.get(account, {})
            rejections = state['accounts'][account].setdefault('priceRejections', {})
            if result.get('status') == 'rejected':
                reason = str(result.get('reason', child['result'].get('reason', 'Account order rejected')))[:256]
                if result.get('dispatchNotSent') is True:
                    self._parallel_note(state,account,'waiting-read','Account worker busy before send; waiting for a fresh account snapshot')
                    self.entry_clocks[(state['id'],account)]['transportRetryAt']=time.monotonic()+.05
                    continue
                # New captured policies may continue automatically after an
                # exact, unfilled support refusal. Legacy plans retain their
                # explicit recovery contract. Each market part still passes
                # its own native price, SL/TP, risk and broker checks.
                optional_support = self._optional_support_refusal(payload, state, child, account, result)
                retry_market = self._retryable_market_protection_refusal(payload, child, result)
                transient = self._parallel_price_refusal(result) or optional_support or retry_market
                if transient:
                    rejections[kind] = dict(quote=[bid, ask], volume=volume, at=time.time()*1000,
                        retryMs=500 if result.get('retcode') == 10024 else 250)
                    proof = result.get('entryDiagnostics', {})
                    if (kind == 'market' and result.get('retcode') is None
                            and result.get('providerOrderId') in (None, 0, '0', '')
                            and result.get('filledVolume', 0) in (None, 0, 0.)
                            and isinstance(proof, dict) and proof.get('mutationStarted') is False
                            and proof.get('rejectionStage') == 'native-entry-price-guard'
                            and proof.get('account') == account and proof.get('symbol') == payload['symbol']
                            and proof.get('side') == payload['side'] and proof.get('kind') == 'market'
                            and all(isinstance(proof.get(key), (int, float)) and not isinstance(proof[key], bool)
                                and math.isfinite(proof[key]) and proof[key] > 0
                                for key in ('bid', 'ask', 'tickUtcMs', 'checkedAtMs'))):
                        rejections[kind]['fastQuoteProof'] = dict(quote=[proof['bid'], proof['ask']],
                            tickUtcMs=proof['tickUtcMs'], checkedAtMs=proof['checkedAtMs'])
                    if retry_market:
                        rejections[kind]['waitForQuoteChange'] = True
                        rejections[kind]['recheckMs'] = 1000
                    self._parallel_note(state, account, 'waiting-price', reason)
                else:
                    state['accounts'][account]['entryHalted'] = reason
                    if kind=='limit' and result.get('retcode')==10016 and result.get('providerOrderId') in (None,0,'0','') and result.get('filledVolume',0) in (None,0,0.):
                        state['accounts'][account].update(limitRetryAvailable=True,limitRefusalChild=child['id'])
                    self._parallel_note(state, account, 'stopped', reason)
                    # The halt must survive restart before any later tranche.
                    self._save(state)
            elif result.get('status') == 'ok':
                rejections.pop(kind, None)
                self._parallel_note(state, account, 'limit' if kind == 'limit' else 'filled',
                                    'Broker confirmed the account order')
            else:
                raise ValueError('Account submission is unknown; no replay')
        return True

    def _tick_parallel_hybrid(self, payload, state):
        """Batched ready-account rounds; slow quote reads never join the batch."""
        started = time.monotonic()
        try:
            for account, details in state['accounts'].items():
                if details.get('commonStopUncertain') and details.get('commonStopRequest') and details.get('commonStopRecoveryAllowed'):
                    self._hybrid_sync_stop(payload,state,accounts={account})
            if state.get('reconciliation', {}).get('status') == 'checking':
                self._reconcile_tick(payload, state)
                return
            if state.get('protectionPending') or state.get('closurePending') or state.get('protectionPause'):
                return
            if any(child['state'] in ('sending', 'unknown') for child in state['children']):
                raise ValueError('Submission remains unconfirmed; no replay')
            if state.get('hybridRemainderStop') or self._stopping(state['id']) or time.time()*1000 >= payload['closeAtUtc']:
                safe = self._hybrid_cancel_limits(payload, state)
                self._phase(state, 'stopped' if safe else 'unknown', 'Further entry stopped; positions retained')
                return
            lower, upper = market_price_bounds(payload)
            direction = 1 if payload['side'] == 'buy' else -1
            for _ in range(HYBRID_ROUNDS_PER_TICK):
                if self._stopping(state['id']) or self._protecting(state['id']):
                    return
                quotes, evidence = self._parallel_quotes(payload, state)
                if not quotes:
                    break
                # Consume fills and cancellations before releasing any volume.
                for child in list(state['children']):
                    selected = pending_accounts(child).intersection(quotes) if child['type'] == 'limit' else set()
                    if not selected:
                        continue
                    due = time.monotonic() >= self.pending_poll_at.get(child['id'], 0)
                    inside = {account for account in selected if lower-1e-8 <= quotes[account][1 if direction == 1 else 0] <= upper+1e-8
                              and self._parallel_retry_ready(state, account, 'market', list(quotes[account]))}
                    relocate = {account for account in selected if direction*(quotes[account][1 if direction == 1 else 0]
                                -(upper if direction == 1 else lower)) > 1e-8
                                and child.get('edgePlaced', {}).get(account) != (upper if direction == 1 else lower)}
                    halt = {account for account in selected if state['accounts'][account].get('entryHalted')}
                    if inside or relocate or halt:
                        self._parallel_cancel(payload, state, child, inside | relocate | halt)
                    elif due:
                        before = {account: child.get('positions', {}).get(account, {}).get('volume', 0.) for account in selected}
                        self._hybrid_limit(payload, state, child, accounts=selected)
                        changed = {account for account in selected if child.get('positions', {}).get(account, {}).get('volume', 0.) > before[account]+1e-8}
                        if changed.intersection(pending_accounts(child)):
                            self._parallel_cancel(payload, state, child, changed.intersection(pending_accounts(child)))
                    self.pending_poll_at[child['id']] = time.monotonic()+.25
                for account in quotes:
                    proof=getattr(self,'cancel_proofs',{}).pop((state['id'],account),None)
                    if (proof and str(proof.get('account'))==account and proof.get('server')==self.bridge.config['server']
                            and 0 <= time.time()*1000-proof.get('checkedAt',0) <= 1000):
                        evidence[account]=proof
                # Fills consumed above can introduce a ticket absent from the
                # earlier immutable watch. Such an account needs a fresh proof.
                for account in list(evidence):
                    if evidence[account] is not None:
                        tickets = {str(row.get('ticket')) for row in evidence[account].get('orders', []) if row.get('__isPosition')}
                        if any(account in child.get('positions', {}) and child['positions'][account]['ticket'] not in tickets
                               for child in state['children']):
                            evidence[account] = None
                self._positions(payload, state, accounts=set(quotes), evidence=evidence)
                if state.get('hybridRemainderStop') or self._hybrid_fill_violation(state):
                    raise ValueError(state.get('hybridRemainderStop', 'Actual fill exceeded captured bounds'))
                limits, markets, edges = {}, {}, {}
                for account, (bid, ask) in quotes.items():
                    details = state['accounts'][account]
                    pending = self._parallel_pending(state, account)
                    if details.get('entryHalted'):
                        if not pending and not self._independent_sync_stop(payload, state, account):
                            continue
                        self._parallel_note(state, account, 'stopped', details['entryHalted'])
                        continue
                    closing = bid if direction == 1 else ask
                    if any(account in child.get('positions', {}) and (
                            direction*(closing-child['positions'][account]['sl']) <= 0
                            or child['positions'][account].get('tp') and direction*(closing-child['positions'][account]['tp']) >= 0)
                           for child in state['children']):
                        raise ValueError('Captured SL or TP reached; remainder stopped')
                    if pending:
                        quote = ask if direction == 1 else bid
                        wait_reason = ('Return limit reserved; market quote outside captured zone'
                            if not lower-1e-8 <= quote <= upper+1e-8 else
                            'Return limit reserved; waiting for account retry'
                            if not self._parallel_retry_ready(state, account, 'market', [bid, ask]) else
                            'Return limit reserved; waiting for exact pending result')
                        self._parallel_note(state, account, 'limit', wait_reason)
                        continue
                    if not self._independent_sync_stop(payload, state, account):
                        continue
                    residual = rounded(details['targetVolume']-details['filledVolume'])
                    if residual <= 1e-8:
                        self._parallel_note(state, account, 'completed', 'Target and common stop confirmed')
                        continue
                    quote = ask if direction == 1 else bid
                    inside = lower-1e-8 <= quote <= upper+1e-8
                    adverse = direction*(quote-(upper if direction == 1 else lower)) > 1e-8
                    spec = details['spec']
                    if inside:
                        if not self._parallel_retry_ready(state, account, 'market', [bid, ask]):
                            self._parallel_note(state, account, 'waiting-price', 'Waiting for a fresh permitted market check; support retained')
                            continue
                        volume = min(residual, details['parts'][0], spec['maxQty'])
                        volume = float((Decimal(str(volume))/Decimal(str(spec['quantityStep']))+Decimal('1e-9')).to_integral_value(rounding=ROUND_FLOOR)*Decimal(str(spec['quantityStep'])))
                        if 1e-8 < residual-volume < spec['minQty']-1e-8:
                            volume = rounded(residual-spec['minQty'])
                        if volume < spec['minQty']-1e-8:
                            raise ValueError('Remaining market volume violates broker minimum')
                        markets[account] = (quote, volume, bid, ask, 0.)
                        support = min(rounded(residual-volume), spec['maxQty'])
                        if support >= spec['minQty']-1e-8:
                            distance = self._hybrid_distance(payload, state, account)
                            worst = upper if direction == 1 else lower
                            risk = (0. if payload.get('initialSizingOnly') is True else
                                volume*max(0., distance if distance is not None else direction*(worst-self._hybrid_stop(payload,state,account,quote)))*details['pointValue'])
                            limits[account] = (max(lower, bid) if direction == 1 else min(upper, ask), support, bid, ask, risk)
                    elif adverse:
                        edge = upper if direction == 1 else lower
                        limits[account] = (edge, min(residual, spec['maxQty']), bid, ask, 0.)
                        edges[account] = edge
                    else:
                        self._parallel_note(state, account, 'waiting-price', 'No valid limit inside the captured zone')
                progress = self._parallel_batch(payload, state, 'limit', limits)
                if limits and progress:
                    child = state['children'][-1]
                    marked = {account: edge for account, edge in edges.items() if account in pending_accounts(child)}
                    if marked:
                        child.setdefault('edgePlaced', {}).update(marked)
                        self._save(state)
                # Every market carries the exact accepted support reservation.
                for account, plan in list(markets.items()):
                    if state['accounts'][account].get('entryHalted'):
                        del markets[account]
                        continue
                    pending = self._parallel_pending(state, account)
                    reserved = (0. if payload.get('initialSizingOnly') is True else sum(
                        max(0., child['volumes'][account]-child.get('positions', {}).get(account, {}).get('volume', 0.))
                        *max(0., direction*(child['prices'][account]-child['stops'][account]))*state['accounts'][account]['pointValue'] for child in pending))
                    markets[account] = (*plan[:4], reserved)
                progress |= self._parallel_batch(payload, state, 'market', markets)
                if state['phase'] not in ACTIVE or not progress or time.monotonic()-started >= .25:
                    break
            complete = all(row['filledVolume'] >= row['targetVolume']-1e-8 and row.get('entryStatus') == 'completed' for row in state['accounts'].values())
            ended = (all(row.get('entryStatus') == 'completed' or row.get('entryHalted') and row.get('entryStatus') == 'stopped'
                         for row in state['accounts'].values())
                     and not any(self._parallel_pending(state, account) for account in state['accounts']))
            self._phase(state, 'completed' if complete else 'stopped' if ended else 'running',
                'Target and protection confirmed' if complete else 'Account entry ended' if ended else 'Parallel account entry is active')
            self._remember(state)
        except CommonStopWait as error:
            self._phase(state,'paused',str(error))
        except Exception as error:
            if self.entry_diagnostics:
                self.entry_diagnostics.emit('entry-error',account=getattr(self,'lane_account',self.primary),
                    entry_id=state['id'],symbol=payload.get('symbol'),stage='account-entry-cycle',
                    duration_ms=(time.monotonic()-started)*1000,error=error)
            if any(child['state'] in ('sending', 'unknown') or child['state']=='pending' and child.get('cancellations')
                   for child in state['children']) and self._begin_reconciliation(payload, state, error):
                return
            try:
                safe = self._hybrid_cancel_limits(payload, state)
            except QuoteWait as deferred:
                state['quoteWait'] = deferred.details
                self._phase(state, 'paused', str(deferred))
                return
            except Exception:
                safe = False
            self._phase(state, 'stopped' if safe else 'unknown', str(error))
        finally:
            state['lastCycleMs'] = round((time.monotonic()-started)*1000, 3)
            if state.get('phase') in ('completed', 'stopped'):
                for key in [key for key in self.snapshot_jobs if key[0] == state['id']]:
                    _, future = self.snapshot_jobs.pop(key)
                    if not future.cancel() and not future.done():
                        self.retired_snapshot_jobs.add(future)
                for key in [key for key in self.entry_clocks if key[0] == state['id']]:
                    self.entry_clocks.pop(key)

    def _tick_hybrid(self, payload, state):
        if parallel_accounts(payload):
            return self._tick_parallel_hybrid(payload, state)
        if independent_accounts(payload):
            return self._tick_independent_hybrid(payload, state)
        started = time.perf_counter()
        waiting = False
        try:
            if state.get('reconciliation', {}).get('status') == 'checking':
                self._reconcile_tick(payload, state)
                return
            if state.get('protectionPending') or state.get('closurePending') or state.get('protectionPause'):
                return
            if state['phase'] == 'sending' or any(child['state'] in ('sending', 'unknown') for child in state['children']):
                raise ValueError('Hybrid submission is unconfirmed; automatic repeat is blocked')
            # Broker rounding or Max lot may require extra rounds. Complete the
            # admitted bounded plan without a timer between tranches; each
            # round still gets a fresh terminal price and risk guard.
            for _ in range(HYBRID_ROUNDS_PER_TICK):
                if self._protecting(state['id']):
                    return
                if state.get('hybridRemainderStop') or self._stopping(state['id']) or time.time()*1000 >= payload['closeAtUtc']:
                    if state.get('protectionOnlyWait'):
                        # This is a quote-only protective wait. A stale tick
                        # does not create another rejected native SL command.
                        for account, details in state['accounts'].items():
                            try:
                                _, _, _, point = self._entry_snapshot(account, payload['symbol'])
                            except QuoteWait as quote_wait:
                                quote_wait.details['stage'] = 'common-stop'
                                raise
                            if abs(point-details['pointValue']) > 1e-8:
                                raise ValueError('Instrument USD point value changed')
                    safe = self._hybrid_cancel_limits(payload, state)
                    self._phase(state, 'stopped' if safe else 'unknown', 'Hybrid remainder stopped; existing positions retained' if safe else
                                state.get('reason', 'Hybrid cancellation or common stop unconfirmed'))
                    return
                self._positions(payload, state)
                quotes, snapshots = {}, {}
                waiting = True
                for account, details in state['accounts'].items():
                    data, bid, ask, point = self._entry_snapshot(account, payload['symbol'])
                    if abs(point-details['pointValue']) > 1e-8:
                        raise ValueError('Instrument USD point value changed')
                    quotes[account] = ask if payload['side'] == 'buy' else bid
                    snapshots[account] = (bid, ask)
                waiting = False
                if state.get('quoteWait', {}).get('status') == 'waiting':
                    state['quoteWait'].update(status='resumed', resumedAtMs=time.time()*1000)
                lower, upper = market_price_bounds(payload)
                direction = 1 if payload['side'] == 'buy' else -1
                for child in state['children']:
                    for account, position in child.get('positions', {}).items():
                        closing_quote = snapshots[account][0 if direction == 1 else 1]
                        if (direction*(closing_quote-position['sl']) <= 0
                                or position.get('tp') and direction*(closing_quote-position['tp']) >= 0):
                            raise ValueError('Captured SL or TP reached; further hybrid entry stopped')
                active = {account: row for account, row in state['accounts'].items()
                          if row['filledVolume'] < row['targetVolume']-1e-8}
                inside = all(lower-1e-8 <= quotes[account] <= upper+1e-8 for account in active)
                outside = all(direction*(quotes[account]-(upper if direction == 1 else lower)) > 1e-8 for account in active)
                pending = [child for child in state['children'] if child['type'] == 'limit' and child['state'] == 'pending']
                if pending:
                    # Re-entry quotes are still evaluated at normal cadence;
                    # an unchanged outside limit needs only bounded status reads.
                    now = time.monotonic()
                    if not inside and now < self.pending_poll_at.get(state['id'], 0):
                        return
                    self.pending_poll_at[state['id']] = now + .25
                    before = sum(row['filledVolume'] for row in state['accounts'].values())
                    for child in pending:
                        self._hybrid_limit(payload, state, child, cancel=inside or child.get('support', False))
                    changed = sum(row['filledVolume'] for row in state['accounts'].values()) > before+1e-8
                    if changed or inside or any(child.get('support') for child in pending):
                        if not self._hybrid_cancel_limits(payload, state):
                            raise ValueError(state.get('reason', 'Hybrid cancellation or common stop unconfirmed'))
                    elif any(child['state'] == 'pending' for child in pending):
                        return
                self._hybrid_sync_stop(payload, state)
                if self._hybrid_fill_violation(state):
                    if not self._hybrid_cancel_limits(payload, state):
                        raise ValueError(state.get('reason', 'Out-of-zone fill cancellation unconfirmed'))
                    self._phase(state, 'stopped', 'Actual hybrid fill left the captured zone; all known fills retained and remainder stopped')
                    return
                if all(row['filledVolume'] >= row['targetVolume']-1e-8 for row in state['accounts'].values()):
                    self._phase(state, 'completed', 'Hybrid target confirmed with a common stop on every account')
                    return
                # A limit may have completed only one account in this round.
                # Every new child reserves exactly its unfinished accounts;
                # completed positions remain subject to ownership/SL checks.
                active = {account: row for account, row in state['accounts'].items()
                          if row['filledVolume'] < row['targetVolume']-1e-8}
                quotes = {account: quotes[account] for account in active}
                inside = all(lower-1e-8 <= value <= upper+1e-8 for value in quotes.values())
                outside = all(direction*(value-(upper if direction == 1 else lower)) > 1e-8 for value in quotes.values())
                if not inside and not outside:
                    self._phase(state, 'paused' if any(row['filledVolume'] for row in state['accounts'].values()) else 'armed', 'Waiting for every account to enter the captured side zone')
                    return
                volumes = {}
                for account, details in active.items():
                    spec = details['spec']; step = spec['quantityStep']
                    residual = rounded(details['targetVolume']-details['filledVolume'])
                    volume = min(residual, details['parts'][0], spec['maxQty'])
                    if 1e-8 < residual-volume < spec['minQty']-1e-8:
                        volume = rounded(volume-spec['minQty']+(residual-volume))
                    volume = float((Decimal(str(volume))/Decimal(str(step))+Decimal('1e-9')).to_integral_value(rounding=ROUND_FLOOR)*Decimal(str(step)))
                    if volume < spec['minQty']-1e-8:
                        raise ValueError('Hybrid residual cannot satisfy the broker minimum lot on every account')
                    volumes[account] = volume
                if outside:
                    volumes = {account: min(rounded(details['targetVolume']-details['filledVolume']), details['spec']['maxQty']) for account, details in active.items()}
                    prices = {account: upper if direction == 1 else lower for account in active}
                    self._hybrid_send(payload, state, 'limit', prices, volumes, self._hybrid_guards(payload, state, prices, volumes))
                    return
                distance = entry_stop_distance(payload)
                support_volumes = {account: rounded(details['targetVolume']-details['filledVolume']-volumes[account]) for account, details in active.items()}
                support_volumes = {account: min(volume, state['accounts'][account]['spec']['maxQty']) for account, volume in support_volumes.items()}
                support_prices = {account: max(lower, bid) if direction == 1 else min(upper, ask) for account, (bid, ask) in snapshots.items() if account in active}
                support_allowed = all(support_volumes[account] >= details['spec']['minQty']-1e-8
                    and direction*(quotes[account]-support_prices[account]) > 1e-8
                    and direction*(support_prices[account]-self._hybrid_stop(payload,state,account,support_prices[account])) > 1e-8
                    and (not self._hybrid_tp(payload,state,account) or direction*(self._hybrid_tp(payload,state,account)-support_prices[account]) > 1e-8)
                    for account, details in active.items())
                reserved = {}
                if support_allowed:
                    market_risk = {account: volumes[account]*max(0., self._hybrid_distance(payload,state,account) if self._hybrid_distance(payload,state,account) is not None else direction*((upper if direction == 1 else lower)-self._hybrid_stop(payload,state,account,quotes[account])))*details['pointValue'] for account, details in active.items()}
                    sent = self._hybrid_send(payload, state, 'limit', support_prices, support_volumes,
                        self._hybrid_guards(payload, state, support_prices, support_volumes, market_risk, support=True), support=True)
                    if sent:
                        support_child = state['children'][-1]
                        reserved = {account: volume*max(0., direction*(support_child['prices'][account]-support_child['stops'][account]))*state['accounts'][account]['pointValue'] for account, volume in support_volumes.items()}
                    elif state['phase'] == 'paused':
                        # A definite native freshness rejection must trigger
                        # a new two-account snapshot, rather than sending the
                        # reserved market part using this earlier round.
                        return
                if self._protecting(state['id']):
                    if not self._hybrid_cancel_limits(payload, state):
                        raise ValueError('Pending protection command could not cancel support')
                    return
                self._hybrid_send(payload, state, 'market', quotes, volumes,
                    self._hybrid_guards(payload, state, quotes, volumes, reserved))
                terminal = state['phase'] not in ACTIVE
                reason = state.get('reason', '')
                if not self._hybrid_cancel_limits(payload, state):
                    raise ValueError(state.get('reason', 'Hybrid limit cancellation or common stop unconfirmed'))
                if self._hybrid_fill_violation(state):
                    self._phase(state, 'stopped', 'Actual hybrid fill left the captured zone; all known fills retained and remainder stopped')
                    return
                if terminal and state['phase'] != 'completed':
                    self._phase(state, state['phase'], reason)
                    return
                if all(row['filledVolume'] >= row['targetVolume']-1e-8 for row in state['accounts'].values()):
                    self._phase(state, 'completed', 'Hybrid target confirmed with a common stop on every account')
                    return
                self._phase(state, 'running', 'Hybrid part confirmed; proceeding without an artificial pause')
        except Exception as error:
            if (not isinstance(error, QuoteWait)
                    and any(child['state'] in ('sending','unknown') for child in state['children'])
                    and self._begin_reconciliation(payload, state, error)):
                return
            try:
                retained = (isinstance(error, QuoteWait) and error.details.get('stage') == 'runtime'
                            and self._hybrid_wait_limits(payload, state))
                safe = retained or self._hybrid_cancel_limits(payload, state,
                    sync_stop=not isinstance(error, QuoteWait))
            except QuoteWait as deferred:
                error, safe = deferred, True
            except Exception as observation_error:
                # A failed identity/protection read is not a quote-only wait.
                # Preserve the existing cancellation and uncertainty policy.
                error = observation_error
                waiting = False
                safe = self._hybrid_cancel_limits(payload, state, sync_stop=False)
            protective_wait = (isinstance(error, QuoteWait) and error.details.get('stage') == 'common-stop'
                and state.get('protectionOnlyWait') and state.get('hybridRemainderStop') and safe
                and all(child['state'] in ('confirmed', 'cancelled', 'rejected') for child in state['children'])
                and not any(details.get('commonStopUncertain') for details in state['accounts'].values()))
            uncertain = (any(child['state'] in ('sending', 'unknown') for child in state['children'])
                or any(details.get('commonStopUncertain') or details.get('riskRepairRequired') and not protective_wait
                       for details in state['accounts'].values()) or not safe)
            quote_changed = isinstance(error, QuoteWait) and state.get('quoteWait', {}).get('status') != 'waiting'
            if quote_changed:
                state['quoteWait'] = error.details
            waiting |= isinstance(error, QuoteWait)
            stopped = bool(state.get('hybridRemainderStop')) or self._stopping(state['id']) or time.time()*1000 >= payload['closeAtUtc']
            resolved_target = (str(error) == 'Limit remainder cancellation or opening identity is unconfirmed'
                and safe and not uncertain and not waiting and not stopped and not self._hybrid_fill_violation(state)
                and all(abs(row['filledVolume']-row['targetVolume']) <= 1e-8 for row in state['accounts'].values())
                and all(child['state'] in ('confirmed', 'cancelled', 'rejected') for child in state['children']))
            if resolved_target:
                try:
                    # A resolved cancel race must not hide a subsequent manual
                    # SL change, a closed/reduced position or a failed read.
                    self._positions(payload, state)
                except Exception as check_error:
                    error, uncertain, resolved_target = check_error, True, False
                else:
                    state['resolvedRemainderWarning'] = str(error)[:512]
            if uncertain and not isinstance(error, QuoteWait) and self._begin_reconciliation(payload, state, error):
                return
            self._phase(state, 'unknown' if uncertain else 'paused' if protective_wait else 'stopped' if stopped else 'paused' if waiting else 'stopped',
                HYBRID_STOP_QUOTE_WAIT if protective_wait else
                'Hybrid target confirmed with a common stop on every account' if resolved_target else
                'Hybrid remainder stopped; existing positions retained' if stopped and not uncertain else str(error), force=quote_changed)
        finally:
            state['lastCycleMs'] = round((time.perf_counter()-started)*1000, 3)

    def _tick_entry(self, payload, state):
        if payload.get('executionPolicy') == 'hybrid-market-limit-v1':
            return self._tick_hybrid(payload, state)
        pending = next((child for child in state['children'] if child['state']=='pending'), None)
        if pending is not None:
            self._tick_limit(payload, state, pending)
            return
        if self._stopping(state['id']):
            self._phase(state, 'stopped', 'Further entry stopped by user; existing positions remain protected')
            return
        if state['phase'] == 'sending' or any(child['state'] == 'sending' for child in state['children']):
            self._phase(state, 'unknown', 'A tranche was already claimed; automatic repeat is blocked')
            return
        if time.time() * 1000 >= payload['closeAtUtc']:
            self._phase(state, 'stopped', 'Automatic exit time reached; further entry stopped')
            return
        try:
            self._positions(payload, state)
        except Exception as error:
            self._phase(state, 'stopped', str(error))
            return
        direction = 1 if payload['side'] == 'buy' else -1
        quotes = {}; guards = {}; index = state['nextChunk']
        try:
            for account, details in state['accounts'].items():
                self._account(account)
                _, bid, ask, point = self._snapshot(account, payload['symbol'])
                if abs(point - details['pointValue']) > 1e-8:
                    raise ValueError('Instrument USD point value changed; entry is paused')
                quotes[account] = ask if direction == 1 else bid
        except Exception as error:
            self._phase(state, 'paused', str(error))
            return
        price = quotes[self.primary]
        zone = payload.get('entryMode') in ('zone', 'boundary', 'side-zone')
        hybrid = payload.get('executionPolicy') == 'market-limit'
        limit = False
        distance = entry_stop_distance(payload)
        if zone:
            lower, upper = market_price_bounds(payload)
            protections = [(account, child['positions'][account]) for child in state['children']
                           if child['state'] == 'confirmed' for account in state['accounts']]
            if state['nextChunk'] and any(direction * (quotes[account] - position.get('sl', payload['sl'])) <= 0
                or position.get('tp') and direction * (quotes[account] - position['tp']) >= 0 for account, position in protections):
                self._phase(state, 'stopped', 'Fixed stop or take-profit price reached; further entry stopped')
                return
            edge = upper if direction == 1 else lower
            limit = hybrid and all(direction*(value-edge)>1e-8 for value in quotes.values())
            if not limit and any(value < lower - 1e-8 or value > upper + 1e-8 for value in quotes.values()):
                self._phase(state, 'paused' if state['nextChunk'] else 'armed',
                    'Waiting for every account to reach market entry boundary' if payload.get('entryMode')=='boundary' else 'Waiting for every account to enter the market zone')
                return
        if not state['triggered']:
            crossed = direction * (state['previousPrice'] - payload['trigger']) < 0 <= direction * (price - payload['trigger'])
            if not zone and not crossed:
                self._phase(state, 'armed', 'Waiting for breakout')
                return
            state['triggered'] = True
        pause_reason = None
        for account, details in state['accounts'].items():
            price = edge if limit else quotes[account]
            # The side-zone is an absolute inclusive band. Keep the saved gap
            # as a separate guard against adverse movement after this snapshot;
            # anchoring it at LT would incorrectly truncate a wider play zone.
            anchor = price if payload.get('entryMode') == 'side-zone' else payload['trigger'] if zone else details['anchor'] if details['anchor'] is not None else payload['trigger']
            initial_r = direction * (anchor - payload['sl'])
            if initial_r <= 0 or (not zone and direction * (price - anchor) > 2 * initial_r + 1e-8):
                self._phase(state, 'stopped', 'Price exceeded 2R from the first entry; further entry permanently stopped')
                return
            stop = rounded(price - direction * distance) if distance is not None else payload['sl']
            if stop <= 0 or direction * (price - stop) <= 0 or (payload.get('tp') and direction * (price - payload['tp']) >= 0):
                self._phase(state, 'stopped', 'Fixed stop or take-profit price reached; further entry stopped')
                return
            volume = details['parts'][index]
            remaining = details['riskUsd'] - details['riskUsed']
            risk_price = stop + direction * remaining / (volume * details['pointValue'])
            if distance is not None:
                risk_price = upper if direction == 1 else lower
            guards[account] = dict(anchor=anchor, maxGap=payload['zoneRadius'] if payload.get('entryMode') == 'zone' else payload['maxGap'], maxRiskPrice=risk_price,
                                   positions=[dict(ticket=child['positions'][account]['ticket'],
                                                   volume=child['positions'][account]['volume'],
                                                   sl=child['positions'][account].get('sl', payload['sl']),
                                                   tp=child['positions'][account].get('tp', payload.get('tp', 0)))
                                              for child in state['children'] if child['state'] == 'confirmed'])
            if zone:
                guards[account].update(priceFloor=lower, priceCeiling=upper)
            if distance is not None:
                guards[account].update(stopDistance=distance, riskBudget=remaining)
            if direction * (price - anchor) > guards[account]['maxGap'] + 1e-8:
                pause_reason = 'Account ' + account + ': price gap is too large; waiting for price to return'
            elif volume * direction * (price - stop) * details['pointValue'] > remaining + 1e-8:
                pause_reason = 'Account ' + account + ': next tranche exceeds remaining risk; waiting for price to return'
            if details['filledVolume'] + volume > details['targetVolume'] + 1e-8:
                self._phase(state, 'stopped', 'Planned account volume would be exceeded')
                return
        if pause_reason:
            self._phase(state, 'paused', pause_reason)
            return
        if self._stopping(state['id']):
            self._phase(state, 'stopped', 'Further entry stopped by user; existing positions remain protected')
            return
        request = self._child(payload, state, index, edge if limit else quotes[self.primary])
        if limit:
            request['type'] = 'limit'
        volumes = {account: details['parts'][index] for account, details in state['accounts'].items()}
        # Persist the unique child claim BEFORE entering any submission path.
        child = dict(id=request['id'], index=index + 1, state='sending', type=request['type'], price=request['price'], volumes=volumes, guards=copy.deepcopy(guards),
                     claimedAt=time.time() * 1000, positions={})
        state['children'].append(child)
        self._phase(state, 'sending', 'Tranche submission recorded; waiting for every account')
        try:
            if len(state['accounts']) > 1:
                response = self.bridge.group_execute('place', request, volumes=volumes, guards=guards)
            else:
                request['entryGuard'] = guards[self.primary]
                response = self.bridge.execute('place', request)
            result = response['result']
            child['result'] = copy.deepcopy(result)
            if result.get('status') == 'rejected':
                child['state'] = 'rejected'
                self._phase(state, 'stopped', result.get('reason', 'Tranche rejected; further entry stopped'))
                return
            if limit:
                results = self._limit_results(state, child)
                if (result.get('status')!='ok' or set(results)!=set(state['accounts']) or any(
                        row.get('status')!='ok' or not re.fullmatch(r'[1-9][0-9]{0,19}', str(row.get('providerOrderId', '')))
                        for row in results.values())):
                    child['state'] = 'unknown'
                    self._phase(state, 'unknown', 'Постановка лимитки не подтверждена на каждом счёте; повторный вход заблокирован')
                    return
                child['state'] = 'pending'
                self._phase(state, 'running', 'Лимитная заявка ждёт исполнения; дополнительный рынок заблокирован')
                return
            self._confirm(payload, state, child, result)
        except Exception as error:
            child['state'] = 'unknown'
            self._phase(state, 'unknown', str(error) + '; no automatic repeat')

    def tick(self):
        # HTTP arming/stopping and this one coordinator serialize parent state;
        # the quote collector does not acquire this lock.
        if not self.lock.acquire(blocking=False):
            return
        try:
            rows = self._active_rows()
            for payload, state in rows:
                payload, state = json.loads(payload), json.loads(state)
                if state['phase'] in ACTIVE:
                    try:
                        if payload.get('accountExecution') == 'independent-v4':
                            self._schedule_lanes(payload, state)
                        else:
                            self._tick_entry(payload, state)
                    except Exception as error:
                        if self.entry_diagnostics:
                            self.entry_diagnostics.emit('coordinator-error',account=self.primary,entry_id=state['id'],
                                symbol=payload.get('symbol'),stage='entry-cycle',error=error)
                        raise
        finally:
            self.lock.release()

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.stop_event.clear()
        def run():
            while not self.stop_event.is_set():
                started = time.monotonic()
                try:
                    self.tick()
                except Exception as error:
                    # A persisted sending claim is never replayed by this loop.
                    # Any subsequent evaluation of it is blocked below.
                    if self.entry_diagnostics:
                        self.entry_diagnostics.emit('coordinator-error',account=self.primary,stage='entry-cycle',
                            duration_ms=(time.monotonic()-started)*1000,error=error)
                self.wake_event.wait(max(.001, .05 - (time.monotonic() - started)))
                self.wake_event.clear()
        self.thread = threading.Thread(target=run, name='iscc-chunk-entry', daemon=True)
        self.thread.start()

    def close(self):
        self.admission_executor.shutdown(wait=True)
        if self.progress_observer:
            self.progress_observer.close()
        self.stop_event.set()
        if self.thread and self.thread is not threading.current_thread():
            self.thread.join(timeout=5)
        lane_futures = list(self.lane_jobs.values())
        if lane_futures and wait(lane_futures, timeout=5).not_done:
            raise RuntimeError('Account entry lanes are still draining; storage must remain open')
        futures = [future for job in self.reconcile_jobs.values() for future in job['futures'].values()]
        futures.extend(job[1] for job in self.snapshot_jobs.values())
        futures.extend(self.retired_snapshot_jobs)
        for lane in self.lanes.values():
            futures.extend(job[1] for job in lane.snapshot_jobs.values())
            futures.extend(lane.retired_snapshot_jobs)
            futures.extend(future for job in lane.reconcile_jobs.values() for future in job['futures'].values())
        for future in futures:
            future.cancel()
        if futures and wait(futures, timeout=5).not_done:
            raise RuntimeError('Native reconciliation is still draining; account storage must remain open')
        if self.storage is not self.bridge:
            self.storage.db.close()
            self.storage = self.bridge


class AccountEntryLane(ChunkEntries):
    """One account owns its mutable copy; only merge points touch the parent.

    Every pre-send claim and broker proof commits through the existing parent
    ledger. No new parent, broker retry, independent risk budget or ownership
    namespace is introduced. Parent controls first flag, then drain all lanes.
    """
    def __init__(self, owner, account, slot):
        self.__dict__ = owner.__dict__.copy()
        self.owner, self.lane_account, self.lane_slot = owner, account, slot
        self.lock = threading.RLock()
        self.snapshot_jobs, self.reconcile_jobs = {}, {}
        self.pending_poll_at, self.reconcile_poll_at, self.entry_clocks = {}, {}, {}
        self.retired_snapshot_jobs = set()
        self.cancel_proofs = {}
        self.read_ready = threading.Event()

    def _stopping(self, identifier):
        return identifier in self.owner.parent_halts or super()._stopping(identifier)

    def _begin_reconciliation(self, payload, state, error):
        if not super()._begin_reconciliation(payload, state, error):
            return False
        # The paused claim is durable before dispatch. Only this account's
        # read runs here; proof processing and generation checks stay in the
        # lane. Do not wait for another coordinator turn to start the read.
        self.reconcile_poll_at.pop(state['id'], None)
        self._reconcile_tick(payload, state)
        return True

    def _read_reconciliation(self, account, items):
        if account != self.lane_account:
            raise ValueError('Reconciliation cannot cross an account lane')
        # Execution is waiting on this proof, unlike a passive observer. Use
        # the normal account queue: protective commands keep precedence and
        # an in-flight native call is never interrupted or replayed.
        diagnostic=self.entry_diagnostics
        token=diagnostic.begin('exact-reconciliation',account=account,
            entry_id=getattr(self,'diagnostic_entry_id',None),symbol=getattr(self,'diagnostic_symbol',None),
            reason='Checking exact broker outcome without replay') if diagnostic else None
        try:
            result=(self.bridge.entry_reconcile(items) if account==self.primary else
                self.bridge.linked[account].call('entry_reconcile',items))
        except Exception as error:
            contention=getattr(error,'rpc_diagnostics',{})
            if not isinstance(contention,dict):contention={}
            if diagnostic:diagnostic.finish(token,error=error,
                blockedBy=contention.get('blockedBy'),heldMs=contention.get('heldMs'))
            raise
        else:
            if diagnostic:diagnostic.finish(token)
            return result

    def run(self, payload, state):
        self.read_ready.clear()
        self.current_state = state
        self.diagnostic_entry_id,self.diagnostic_symbol=state['id'],payload['symbol']
        revision = getattr(self, 'command_revision', 0)
        try:
            self._tick_parallel_hybrid(payload, state)
        finally:
            if getattr(self, 'receipt_flush_pending', False):
                self._save(state)
            self.current_state = None
        # An unfinished read already has its own completion notification.
        # Do not wake the coordinator merely to poll that same read again.
        return getattr(self, 'command_revision', 0) != revision or state['phase'] not in ACTIVE

    def _phase(self, state, phase, reason, force=False):
        if phase in ('paused','unknown','stopped'):
            state['accounts'][self.lane_account].update(entryStatus='checking' if phase=='paused' else phase,
                entryReason=str(state['accounts'][self.lane_account].get('entryHalted') or reason)[:256])
        if phase == 'running' and reason == 'Hybrid limit remainder is reserved':
            # The exact accepted ticket is already durable in the native
            # request journal. Commit this receipt together with the very next
            # market/cancel claim; a crash before that claim remains unknown.
            state.update(phase=phase, reason=reason)
            self.receipt_flush_pending = True
            self._remember(state)
            return
        super()._phase(state, phase, reason, force)

    def _call(self, account, method, *args, **kwargs):
        started = time.monotonic()
        diagnostic=self.entry_diagnostics
        token=diagnostic.begin('account-rpc:'+method,account=account,
            entry_id=getattr(self,'diagnostic_entry_id',None),symbol=getattr(self,'diagnostic_symbol',None),
            reason='Waiting for this account MT5 response',announce=method in ('execute','entry_cancel_limit','entry_common_stop')) if diagnostic else None
        if method in ('execute','entry_common_stop'):
            self.command_revision = getattr(self, 'command_revision', 0)+1
        try:
            result=super()._call(account, method, *args, **kwargs)
        except Exception as error:
            contention=getattr(error,'rpc_diagnostics',{})
            if not isinstance(contention,dict):contention={}
            if diagnostic:diagnostic.finish(token,error=error,
                blockedBy=contention.get('blockedBy'),heldMs=contention.get('heldMs'))
            raise
        else:
            if diagnostic:
                rpc=result.get('rpcTiming',{}) if isinstance(result,dict) else {}
                if not isinstance(rpc,dict):rpc={}
                diagnostic.finish(token,workerLockWaitMs=rpc.get('lockWaitMs'),workerRpcMs=rpc.get('totalMs'))
            return result
        finally:
            # entry_context runs in a read-only task; never mutate lane state
            # from that task. Its immutable timings are consumed separately.
            state = getattr(self, 'current_state', None)
            if state is not None and method != 'entry_context':
                timing = state['accounts'][account].setdefault('entryTiming', {})
                trace = timing.setdefault('recentStages', [])
                trace.append(dict(phase='native:'+method, durationMs=round((time.monotonic()-started)*1000, 3)))
                del trace[:-48]

    def _remember(self, state):
        self._merge(state, durable=False)

    def _save(self, state, receipt=None):
        if receipt is not None:
            raise ValueError('Whole-parent controls must drain account lanes first')
        self._merge(state, durable=True)
        self.receipt_flush_pending = False

    def _merge(self, state, durable):
        with self.owner.lock:
            found = self.owner._load(state['id'])
            if found is None:
                raise ValueError('Independent parent disappeared')
            parent = found[2]
            before = copy.deepcopy(parent)
            account = self.lane_account
            # Another lane may have newer read-only diagnostics than the last
            # durable trade claim. Carry them into the next normal commit,
            # without an extra disk write or a native diagnostic request.
            with self.owner.public_lock:
                published = self.owner.public_records.get(state['id'])
                notes = copy.deepcopy(published[1].get('accounts', {})) if published else {}
            for peer, details in parent['accounts'].items():
                note = notes.get(peer, {})
                if (peer != account and parent.get('lanePhases', {}).get(peer) in ACTIVE
                        and note.get('lastEntryCheckAt', 0) >= details.get('lastEntryCheckAt', 0)):
                    for key in ('entryStatus','entryReason','lastEntryCheckAt','entryTiming'):
                        if key in note:details[key] = note[key]
            parent['accounts'][account] = copy.deepcopy(state['accounts'][account])
            parent['children'] = [child for child in parent['children'] if account not in child.get('volumes', {})]
            parent['children'].extend(copy.deepcopy(state['children']))
            parent['children'].sort(key=lambda child:child['index'])
            parent.setdefault('lanePhases', {})[account] = state['phase']
            if state.get('reconciliation'):
                parent.setdefault('laneReconciliation', {})[account] = copy.deepcopy(state['reconciliation'])
            if state.get('hybridRemainderStop'):
                if parent.get('hybridRemainderStop'):
                    pass  # A local result cannot replace a whole-parent control.
                elif self.owner._stopping(state['id']):
                    parent['hybridRemainderStop'] = 'Further entry stopped by user; existing positions remain protected'
                else:
                    parent['accounts'][account]['laneStopReason'] = state['hybridRemainderStop']
            elif (state['phase'] == 'stopped' and not self.owner._stopping(state['id'])
                    and not parent.get('hybridRemainderStop')):
                parent['accounts'][account]['laneStopReason'] = state.get('reason', 'Account entry stopped')
            parent['stopRevision'] = max(parent.get('stopRevision', 0), state.get('stopRevision', 0))
            parent['nextChunk'] = sum(child['type'] == 'market' and child['state'] == 'confirmed' for child in parent['children'])
            phases = [parent.get('lanePhases', {}).get(key, 'armed') for key in parent['accounts']]
            parent['phase'] = ('running' if any(phase in ACTIVE for phase in phases) else
                'unknown' if 'unknown' in phases else 'completed' if all(phase == 'completed' for phase in phases) else 'stopped')
            parent['reason'] = 'Independent account execution; exact account outcomes retained'
            if durable:
                def critical(value):
                    if isinstance(value, dict):
                        return {key:({account:('active' if phase in ACTIVE else phase) for account,phase in item.items()}
                            if key == 'lanePhases' else critical(item)) for key,item in value.items()
                            if key not in ('updatedAt','entryTiming','lastEntryCheckAt','entryStatus','entryReason')}
                    if isinstance(value, list):
                        return [critical(item) for item in value]
                    return value
                if critical(before) != critical(parent):
                    self.owner._save(parent)
                else:
                    self.owner._remember(parent)
            else:
                # Volatile diagnostics cannot replace a newer durable child.
                self.owner._remember(parent)


class AccountPlanBridge:
    """An account's planning ledger never shares a writer with another account.

    Native requests still use the original worker ledger. This file contains
    only acquisition intents and proofs, and has no MT5 connection of its own.
    """
    def __init__(self, owner, account, path):
        self.owner, self.account_id = owner, account
        self.entry_diagnostics=getattr(owner.bridge,'entry_diagnostics',None)
        self.config = dict(owner.bridge.config, account=int(account), linkedAccounts=[], entryRiskLimitUsd=0)
        self.linked = {}
        self.lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False, timeout=5)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        # Whole-parent controls inspect these names for identity collisions;
        # actual native receipts remain in the worker's original database.
        self.db.execute('CREATE TABLE IF NOT EXISTS requests (id TEXT PRIMARY KEY, digest TEXT, result TEXT)')
        self.db.execute('CREATE TABLE IF NOT EXISTS groups (id TEXT PRIMARY KEY, digest TEXT, payload TEXT, result TEXT)')
        self.db.commit()
        self.executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix='iscc-entry-'+account)

    def warm_group_executor(self):
        return self.executor

    def validate_deadline(self, payload):
        return self.owner.bridge.validate_deadline(payload)

    def __getattr__(self, method):
        if method.startswith('_'):
            raise AttributeError(method)
        def call(*args, **kwargs):
            if kwargs:
                raise ValueError('Account native calls require positional arguments')
            worker = (getattr(self.owner.bridge,'primary_worker',None) if self.account_id==self.owner.primary
                      else self.owner.bridge.linked[self.account_id])
            if worker is not None:
                result=worker.call(method,*args)
                observe=getattr(self.owner.bridge,'observe_native',None)
                if callable(observe) and method in ('entry_management','entry_context'):
                    observe(self.account_id,result)
                return result
            with self.owner.bridge.lock:  # Offline tests and legacy native bridge.
                return getattr(self.owner.bridge,method)(*args)
        return call


class AccountPlanEntries(ChunkEntries):
    def __init__(self, owner, account, bridge):
        self.coordinator, self.account_id = owner, account
        self.ownership_callback = owner._receive_owners
        self.accepted_intents = set()
        self.ready_to_publish = False
        super().__init__(bridge)
        self.ready_to_publish = True

    def _expiry(self, payload):
        # The short network admission deadline was checked before the root
        # intent was durably accepted. A slow *read* cannot revoke that intent.
        # Every native child still gets its own fresh 3-second expiry.
        if payload.get('id') in self.accepted_intents:
            if (self.coordinator._stopping(payload['id'])
                    or time.time()*1000 >= payload['closeAtUtc']):
                raise ValueError('Accepted entry stopped before account admission')
            return
        return super()._expiry(payload)

    def _stopping(self, identifier):
        return self.coordinator._stopping(identifier) or super()._stopping(identifier)

    def _protecting(self, identifier):
        return self.coordinator._protecting(identifier) or super()._protecting(identifier)

    def _remember(self, state):
        # A single-account engine has no peer projection to merge. Its native
        # scheduler reads the durable ledger; only the UI needs a public view.
        if self.ready_to_publish:
            self.coordinator._publish_account(self.account_id, state)

    def _schedule_lanes(self, payload, state):
        key=(state['id'],self.account_id)
        if key not in self.lanes:
            self.lanes[key]=SingleAccountLane(self,self.account_id,payload['accountLaneSlot'])
        return super()._schedule_lanes(payload,state)


class SingleAccountLane(AccountEntryLane):
    def _merge(self, state, durable):
        # No other account can modify this parent. Parent controls raise their
        # pause flag and drain this lane before writing, as in the v4 engine.
        state['lanePhases']={self.lane_account:state['phase']}
        if durable:
            self.owner._save(state)
        else:
            # Accepted support receipts can be published before the next
            # durable market/cancel claim. Publish their ownership delta too.
            self.owner._index_owners(state)
            self.owner._remember(state)


class IsolatedEntries(ChunkEntries):
    """v5 parents are a projection of durable, independently scheduled accounts.

    The proven v4 state machine and native receipts run once per account. Root
    journals are touched on admission and explicit whole-parent controls only;
    neither a peer's broker call nor its fsync is in an account's send path.
    Older captured protocols continue through their original engine unchanged.
    """
    def __init__(self, bridge):
        self.shards = {}
        self.shard_registry_lock = threading.Lock()
        self.shard_locks = {}
        self.publication_slots = {}
        self.publication_refresh_lock = threading.RLock()
        self.publication_wake = threading.Event()
        self.publication_stop = threading.Event()
        self.publication_thread = None
        self.projection_lock = threading.RLock()
        self.projections = {}
        self.account_states = {}
        self.dirty_projections = set()
        self.admitting = {}
        self.admission_retries = {}
        self.admission_lock = threading.Lock()
        self.admission_cancelled = set()
        self.checkpoint_jobs = {}
        self.checkpoint_executor = ThreadPoolExecutor(max_workers=1,thread_name_prefix='iscc-entry-checkpoint')
        self.checkpoint_signatures = {}
        self.account_stop_executor = ThreadPoolExecutor(max_workers=17,thread_name_prefix='iscc-account-stop')
        self.account_stop_jobs = {}
        self.restoring = True
        super().__init__(bridge)
        self.database_path = pathlib.Path(bridge.db.execute('PRAGMA database_list').fetchone()[2])
        for state in list(self.projections.values()):
            self._restore_root(state)
        self.restoring = False
        self.owner_delta_ready = True
        self._trim_projections()

    def _receive_owners(self, identifier, pairs):
        with self.owner_lock:
            for pair in pairs:self.owner_index.setdefault(pair,set()).add(identifier)
            self.owner_misses.clear()

    def account_readiness(self):
        primary_worker=getattr(self.bridge,'primary_worker',None)
        if primary_worker is None:return super().account_readiness()
        workers={self.primary:primary_worker,**self.bridge.linked}
        states={account:dict(ready=worker.available() if callable(getattr(worker,'available',None)) else True,
                            nativeProofRequired=True) for account,worker in workers.items()}
        return dict(ready=all(row['ready'] for row in states.values()),accounts=list(workers),accountStates=states)

    def _index_committed(self, state):
        if state.get('accountExecution')!='independent-v5' or not getattr(self,'owner_delta_ready',False):
            return super()._index_committed(state)
        # Account journals have already published all new ownership pairs.
        # Rebuilding the UI projection must not rescan every existing child.
        if state['phase'] in ACTIVE:self.active_ids.add(state['id'])
        else:self.active_ids.discard(state['id'])

    def _restore_root(self, state):
        identifier=state['id']
        self.projections[identifier]=copy.deepcopy(state)
        for account in state['accounts']:
            engine=self._shard(account)
            found=engine._load(identifier)
            if found:
                self._index_owners(found[2])
                self.account_states[(identifier,account)]=copy.deepcopy(found[2])
            else:
                current=self.projections[identifier]
                current['accounts'][account].update(entryStatus='stopped',entryReason='Receiver restarted before account admission; no automatic resumption')
                current['accounts'][account].pop('admissionWaiting',None)
                current.setdefault('lanePhases',{})[account]='stopped'
        self._publish_root(identifier,checkpoint=False)

    def _trim_projections(self):
        evicted=[]
        with self.projection_lock:
            finals=sorted((state for state in self.projections.values() if state['phase'] not in ACTIVE
                and state['phase']!='unknown'),key=lambda state:state.get('updatedAt',0),reverse=True)
            for state in finals[30:]:
                identifier=state['id'];job=self.checkpoint_jobs.get(identifier)
                if identifier in self.dirty_projections or job and not job.done():continue
                self.projections.pop(identifier,None)
                evicted.append(identifier)
                self.checkpoint_jobs.pop(identifier,None);self.checkpoint_signatures.pop(identifier,None)
                for account in state['accounts']:
                    self.account_states.pop((identifier,account),None)
        # Do not take an actor lock while holding the projection lock. An
        # active actor publishes in the reverse direction. Cache trimming is
        # optional and yields immediately to any trading/control operation.
        if not evicted:return
        for engine in tuple(self.shards.values()):
            if not engine.lock.acquire(blocking=False):continue
            try:
                for identifier in evicted:
                    key=(identifier,engine.primary);future=engine.lane_jobs.get(key);lane=engine.lanes.get(key)
                    if identifier in engine.active_ids or not future or not future.done() or lane is None:continue
                    reads=[row[1] for row in lane.snapshot_jobs.values()]+list(lane.retired_snapshot_jobs)
                    reads += [job for row in lane.reconcile_jobs.values() for job in row['futures'].values()]
                    if any(not job.done() for job in reads):continue
                    engine.lane_jobs.pop(key,None);engine.lanes.pop(key,None)
            finally:engine.lock.release()

    def _shard(self, account):
        with self.shard_registry_lock:
            lock=self.shard_locks.setdefault(account,threading.Lock())
            self.publication_slots.setdefault(account,SimpleNamespace(lock=threading.Lock(),states={}))
        with lock:
            if account not in self.shards:
                database = self.database_path
                if not database.name:
                    raise ValueError('Independent entry engines require durable file storage')
                bridge = AccountPlanBridge(self,account,database.with_name('entry-plans-'+account+'.sqlite3'))
                try:engine=AccountPlanEntries(self,account,bridge)
                except Exception:
                    bridge.executor.shutdown(wait=True);bridge.db.close();raise
                self.shards[account] = engine
                if self.thread and self.thread.is_alive():engine.start()
        return self.shards[account]

    def _stopping(self, identifier):
        return self.stop_event.is_set() or super()._stopping(identifier)

    @staticmethod
    def _temporary_admission_error(error):
        # Admission performs reads/checks only. Never classify a configuration,
        # identity, lot or stop validation failure as a transport retry.
        return (type(error).__name__=='WorkerDispatchNotSent' or isinstance(error,(OSError,TimeoutError))
            or str(error) in ('MT5 is disconnected','MT5 account unavailable')
            or str(error).startswith('Linked MT5 response unknown;'))

    def _launch_admission(self, identifier, account, engine, local, attempt=0):
        with self.admission_lock:
            return self._launch_admission_locked(identifier,account,engine,local,attempt)

    def _launch_admission_locked(self, identifier, account, engine, local, attempt):
        key=(identifier,account)
        if key in self.admission_cancelled or self._stopping(identifier) or time.time()*1000>=local['closeAtUtc']:return
        existing=self.admitting.get(key)
        if existing and not existing.done():return
        def admit():
            selected=engine
            diagnostic=self.entry_diagnostics
            token=diagnostic.begin('account-admission',account=account,entry_id=identifier,
                symbol=local['symbol'],reason='Checking captured plan and this account',announce=True) if diagnostic else None
            try:
                # Cold storage initialization belongs only to this account.
                # The root intent is already durable and its peer can run.
                if selected is None:selected=self._shard(account)
                selected.accepted_intents.add(identifier)
                selected.arm(local)
                if self.thread and self.thread.is_alive():selected.start()
                selected.wake_event.set()
                with self.projection_lock:self.admission_retries.pop(key,None)
            except Exception as error:
                if diagnostic:diagnostic.finish(token,error=error)
                token=None
                retry=(self._temporary_admission_error(error) and not self._stopping(identifier)
                       and time.time()*1000<local['closeAtUtc'])
                with self.projection_lock:
                    parent=copy.deepcopy(self.projections[identifier])
                    details=parent['accounts'][account]
                    details.update(entryStatus='checking' if retry else 'stopped',entryReason=str(error)[:256],
                                   admissionWaiting=retry)
                    parent.setdefault('lanePhases',{})[account]='paused' if retry else 'stopped'
                    parent['updatedAt']=time.time()*1000
                    self.projections[identifier]=parent
                    if retry:
                        self.admission_retries[key]=(selected,copy.deepcopy(local),time.monotonic()+min(.5,.05*2**min(attempt,4)),attempt+1)
                    else:self.admission_retries.pop(key,None)
                    self._publish_root(identifier)
                # Failure is before any order intent. This durable admission
                # status never enters the healthy peer's planning/send path.
                with self.storage.lock:
                    self.storage.db.execute('UPDATE chunk_entries SET state=? WHERE id=?',
                        (json.dumps(parent,allow_nan=False),identifier));self.storage.db.commit()
            finally:
                if diagnostic:diagnostic.finish(token)
                if selected is not None:selected.accepted_intents.discard(identifier)
                self.wake_event.set()
        self.admitting[key]=self.admission_executor.submit(admit)

    def _retry_admissions(self):
        if not self.admission_retries:return
        with self.projection_lock:pending=list(self.admission_retries.items())
        for (identifier,account),(engine,local,due,attempt) in pending:
            if (identifier,account) in self.admission_cancelled or self._stopping(identifier) or time.time()*1000>=local['closeAtUtc']:
                with self.projection_lock:self.admission_retries.pop((identifier,account),None)
                if not self._stopping(identifier):
                    with self.projection_lock:
                        parent=self.projections.get(identifier)
                        if parent:
                            parent['accounts'][account].update(entryStatus='stopped',admissionWaiting=False,
                                entryReason='Captured automatic-exit deadline reached before account admission')
                            parent['lanePhases'][account]='stopped'
                            self._publish_root(identifier)
                continue
            if time.monotonic()>=due:self._launch_admission(identifier,account,engine,local,attempt)

    def _publish_account(self, account, state):
        # The actor copies only its own state and replaces one latest slot.
        # It never waits for a peer, the panel, root serialization or fsync.
        snapshot=copy.deepcopy(state)
        slot=self.publication_slots[account]
        with slot.lock:slot.states[state['id']]=snapshot
        self.publication_wake.set()

    def _refresh_projections(self):
        with self.publication_refresh_lock:
            pending=[]
            for account,slot in tuple(self.publication_slots.items()):
                with slot.lock:
                    states=slot.states;slot.states={}
                pending.extend((account,state) for state in states.values())
            with self.projection_lock:
                for account,state in pending:
                    if state['id'] in self.projections:
                        self.account_states[(state['id'],account)]=state
                        self.dirty_projections.add(state['id'])
                identifiers=list(self.dirty_projections)
                self.dirty_projections.clear()
            for identifier in identifiers:self._publish_root(identifier)
            if len(self.projections)>30:self._trim_projections()

    def _start_publication(self):
        if self.publication_thread and self.publication_thread.is_alive():return
        def run():
            while not self.publication_stop.is_set():
                self.publication_wake.wait()
                self.publication_wake.clear()
                if self.publication_stop.is_set():break
                try:
                    # Active projections are built only when a status reader
                    # asks for them. A notification must not serialize every
                    # intermediate child in parallel with native execution.
                    final=False
                    for slot in tuple(self.publication_slots.values()):
                        with slot.lock:final=final or any(state['phase'] not in ACTIVE for state in slot.states.values())
                    if final:self._refresh_projections()
                    with self.events:
                        self.event_revision+=1;self.events.notify_all()
                except Exception as error:
                    if self.entry_diagnostics:self.entry_diagnostics.emit('publication-error',
                        account=self.primary,stage='entry-projection',error=error)
        self.publication_thread=threading.Thread(target=run,name='iscc-entry-publication',daemon=True)
        self.publication_thread.start()

    def list(self):
        self._refresh_projections()
        return super().list()

    def _publish_root(self, identifier, checkpoint=True):
        with self.projection_lock:
            parent = copy.deepcopy(self.projections[identifier])
            for account in parent['accounts']:
                local = self.account_states.get((identifier,account))
                if local is None:
                    continue
                multiplier = parent['accounts'][account]['multiplier']
                parent['accounts'][account] = dict(copy.deepcopy(local['accounts'][account]),multiplier=multiplier)
                parent['children'] = [child for child in parent['children'] if account not in child.get('volumes',{})]
                parent['children'].extend(copy.deepcopy(local['children']))
                parent.setdefault('lanePhases',{})[account] = local['phase']
                reconciliation=local.get('reconciliation') or local.get('laneReconciliation',{}).get(account)
                if reconciliation:
                    parent.setdefault('laneReconciliation',{})[account] = copy.deepcopy(reconciliation)
                if local.get('hybridRemainderStop'):
                    parent['accounts'][account]['laneStopReason'] = local['hybridRemainderStop']
                parent['stopRevision'] = max(parent.get('stopRevision',0),local.get('stopRevision',0))
            parent['children'].sort(key=lambda row:row['index'])
            phases = [parent.get('lanePhases',{}).get(account,'armed') for account in parent['accounts']]
            parent['phase'] = ('running' if any(phase in ACTIVE for phase in phases) else 'unknown' if 'unknown' in phases
                               else 'completed' if all(phase=='completed' for phase in phases) else 'stopped')
            parent['nextChunk'] = sum(child['type']=='market' and child['state']=='confirmed' for child in parent['children'])
            parent['updatedAt'] = max([parent.get('updatedAt',0)]+[
                self.account_states[(identifier,account)].get('updatedAt',0) for account in parent['accounts']
                if (identifier,account) in self.account_states])
            parent['reason'] = 'Independent account engines; exact account outcomes retained'
            if parent.get('protectionPending') or parent.get('closurePending'):
                parent.update(phase='unknown',reason='Interrupted parent control; inspect its exact receipt')
            self.projections[identifier] = parent
            self._index_committed(parent)
            ChunkEntries._remember(self,parent)
            if checkpoint and parent['phase'] not in ACTIVE and not self.restoring and not getattr(self,'closed',False):
                signature=digest(parent)
                if self.checkpoint_signatures.get(identifier)!=signature:
                    self.checkpoint_signatures[identifier]=signature
                    def checkpoint():
                        # Rebuildable projection: never before a native send.
                        with self.lock:
                            with self.projection_lock:
                                current=copy.deepcopy(self.projections[identifier])
                            with self.storage.lock:
                                self.storage.db.execute('UPDATE chunk_entries SET state=? WHERE id=?',
                                    (json.dumps(current,allow_nan=False),identifier))
                                self.storage.db.commit()
                    self.checkpoint_jobs[identifier]=self.checkpoint_executor.submit(checkpoint)

    def _load(self, identifier):
        self._refresh_projections()
        found = super()._load(identifier)
        if found and found[1].get('accountExecution')=='independent-v5':
            with self.projection_lock:
                if identifier not in self.projections:self._restore_root(found[2])
                return found[:2]+(copy.deepcopy(self.projections.get(identifier,found[2])),)
        return found

    def _remember(self, state):
        result = super()._remember(state)
        if state.get('accountExecution')=='independent-v5':
            with self.projection_lock:
                self.projections[state['id']]=copy.deepcopy(state)
        return result

    def _begin_account_stops(self, identifier, entry_id, payload):
        found = self._load(entry_id)
        if not found or found[1].get('accountExecution')!='independent-v5':
            return []
        futures = []
        for account in found[2]['accounts']:
            key = (entry_id, account)
            with self.projection_lock:
                self.admission_cancelled.add(key)
                self.admission_retries.pop(key,None)
            def stop_account(account=account,key=key):
                self.entry_controls.drain(entry_id,account)
                with self.admission_lock:admission = self.admitting.get(key)
                if admission:
                    wait([admission])
                engine = self.shards.get(account)
                if engine is None:return
                engine._drain_lanes(entry_id)
                command = dict(id='account-stop:'+hashlib.sha256((identifier+':'+account).encode()).hexdigest(),
                    entryId=entry_id,cancelContinueIds=[],expiresAt=time.time()*1000+3000)
                with engine.lock:
                    if not engine._load(entry_id):return
                    return engine._stop_locked(command['id'],digest(command),entry_id,command)
            job_key = (identifier,account)
            with self.stop_requests_lock:
                job = self.account_stop_jobs.get(job_key)
                if job is None:
                    job = self.account_stop_executor.submit(stop_account)
                    self.account_stop_jobs[job_key] = job
            futures.append(job)
        return futures

    def _stop_locked(self, identifier, fingerprint, entry_id, payload):
        with self.projection_lock:
            for key in list(self.admission_retries):
                if key[0]==entry_id:
                    self.admission_cancelled.add(key);self.admission_retries.pop(key,None)
        result=super()._stop_locked(identifier,fingerprint,entry_id,payload)
        # Use the just-committed control state. Refreshing older account slots
        # here can replace the stop and lose userStopped before shard saves.
        found=ChunkEntries._load(self,entry_id)
        if found and found[1].get('accountExecution')=='independent-v5':
            self._save(found[2])
        return result

    def _active_rows(self):
        return [(payload,state) for payload,state in super()._active_rows()
                if json.loads(payload).get('accountExecution')!='independent-v5']

    def _save(self, state, receipt=None):
        if state.get('accountExecution')=='independent-v5':
            # Only explicit controls call this method for v5. They first raise
            # a shared stop/protection flag and drain all account commands.
            for account in state['accounts']:
                engine = self.shards.get(account)
                found = engine._load(state['id']) if engine else None
                if not found:
                    continue
                local = copy.deepcopy(state)
                local['accountExecution'] = 'independent-v4'
                local['accounts'] = {account:copy.deepcopy(state['accounts'][account])}
                local['children'] = [copy.deepcopy(row) for row in state['children'] if account in row.get('volumes',{})]
                local['phase'] = state.get('lanePhases',{}).get(account,state['phase']) if state['phase'] in ACTIVE else state['phase']
                local['lanePhases'] = {account:local['phase']}
                engine._save(local)
            with self.projection_lock:
                self.projections[state['id']] = copy.deepcopy(state)
        return super()._save(state,receipt)

    def _drain_lanes(self, identifier):
        with self.admission_lock:
            pending = [future for (parent,_),future in self.admitting.items() if parent==identifier]
        if pending and wait(pending,timeout=5).not_done:
            raise ValueError('Account admission is still draining; entry permission remains stopped')
        for engine in tuple(self.shards.values()):
            engine._drain_lanes(identifier)
        return super()._drain_lanes(identifier)

    def arm(self, payload, received=None):
        if payload.get('accountExecution')!='independent-v5':
            with self.lock:
                if not self._load(str(payload.get('id',''))) and any(
                        row.get('accountExecution')=='independent-v5' and row['phase'] in ACTIVE
                        and row.get('symbol')==payload.get('symbol') and row.get('side')==payload.get('side') for row in self.list()):
                    raise ValueError('An independent entry is already active in this direction')
            return super().arm(payload,received)
        identifier = str(payload.get('id',''))
        if not re.fullmatch(r'[A-Za-z0-9_.:-]{1,100}',identifier):
            raise ValueError('Invalid chunk entry ID')
        with self.lock:
            found = self._load(identifier)
            if found:
                if found[0]!=digest(payload):
                    raise ValueError('Chunk entry ID reused with different parameters')
                return public_record(found[2])
            cancelled = self._cancelled(identifier)
            if cancelled is not None:
                return public_record(cancelled)
            self._expiry(payload)
            self.bridge.validate_deadline(payload)
            if (payload.get('executionPolicy')!='hybrid-market-limit-v1' or payload.get('entryMode')!='side-zone'
                    or payload.get('symbol') not in self.bridge.config['symbols'] or payload.get('side') not in ('buy','sell')):
                raise ValueError('Unsupported independent entry engine contract')
            for key in ('trigger','volume','chunkVolume','riskUsd','sl','tickSize','stopOffsetPts','chunkPercent'):
                number(payload.get(key),key)
            if payload['chunkVolume']>payload['volume'] or payload['chunkPercent']>100:
                raise ValueError('Invalid independent entry size')
            for cap in (self.bridge.config.get('entryRiskLimitUsd',0),payload.get('entryRiskLimitUsd',0)):
                cap=number(cap,'Entry risk limit',positive=False)
                if cap<0 or cap>0 and payload['riskUsd']>cap+1e-8:
                    raise ValueError('New entry exceeds its admission risk limit')
            followers = self.bridge.requested_followers(payload) if payload.get('mirrorAccounts') or payload.get('mirrorAccount') else {}
            accounts = {self.primary:1.,**followers}
            current = [row for row in self.list() if row['phase'] in ACTIVE]
            if len(current)>=4 or any(row['symbol']==payload['symbol'] and row['side']==payload['side'] for row in current):
                raise ValueError('An entry is already active in this direction, or the active entry limit was reached')
            state = {key:copy.deepcopy(payload[key]) for key in PUBLIC_FIELDS if key in payload}
            state.update(id=identifier,accountExecution='independent-v5',initialSizingOnly=True,phase='armed',
                         reason='Account admission in progress',children=[],nextChunk=0,childCount=0,
                         createdAt=time.time()*1000,updatedAt=time.time()*1000,lanePhases={account:'armed' for account in accounts},
                         accounts={account:dict(multiplier=multiplier,targetVolume=rounded(payload['volume']*multiplier),
                             filledVolume=0.,riskUsd=rounded(payload['riskUsd']*multiplier),riskUsed=0.,entryStatus='checking')
                             for account,multiplier in accounts.items()})
            with self.storage.lock:
                self.storage.db.execute('INSERT INTO chunk_entries VALUES (?,?,?,?)',
                    (identifier,digest(payload),json.dumps(payload,allow_nan=False),json.dumps(state,allow_nan=False)))
                self.storage.db.commit()
            self.projections[identifier] = copy.deepcopy(state)
            self._index_committed(state);self._remember(state)
            for slot,(account,multiplier) in enumerate(accounts.items()):
                engine = self.shards.get(account)
                local = copy.deepcopy(payload)
                for key in ('mirrorAccounts','mirrorAccount','requiredCapabilities','admissionContract','entryRiskLimitUsd'):
                    local.pop(key,None)
                local.update(accountExecution='independent-v4',initialSizingOnly=True,accountLaneSlot=slot,
                             volume=rounded(payload['volume']*multiplier),chunkVolume=rounded(payload['chunkVolume']*multiplier),
                             riskUsd=rounded(payload['riskUsd']*multiplier))
                self._launch_admission(identifier,account,engine,local)
            return public_record(state)

    def continue_account(self, request):
        found = self._load(request.get('entryId',''))
        if not found or found[1].get('accountExecution')!='independent-v5':
            return super().continue_account(request)
        account = str(request.get('account',''))
        if account not in found[2]['accounts']:
            raise ValueError('Continuation account does not belong to this parent')
        with self.entry_controls.continuing(request):
            with self.projection_lock:pending=self.admission_retries.get((found[2]['id'],account))
            if pending:
                self._expiry(request)
                self.entry_controls.check(request)
                self._launch_admission(found[2]['id'],account,pending[0],pending[1],pending[3])
                return self.get(found[2]['id'])
            self.shards[account]._continue_account(request)
            return self.get(found[2]['id'])

    def tick(self):
        self._retry_admissions()
        super().tick()
        if not self.thread:
            for engine in tuple(self.shards.values()):
                engine.tick()  # Deterministic offline/manual runner only.

    def start(self):
        self._start_publication()
        super().start()
        for engine in tuple(self.shards.values()):
            engine.start()

    def close(self):
        if getattr(self,'closed',False):
            return
        self.closed=True
        self.stop_event.set()
        for engine in tuple(self.shards.values()):
            engine.stop_event.set()
        self.publication_stop.set();self.publication_wake.set()
        if self.publication_thread:
            self.publication_thread.join(timeout=5)
            if self.publication_thread.is_alive():raise RuntimeError('Entry publication is still draining; storage must remain open')
        # A legacy lane connection can also be the checkpoint destination.
        # Drain its writers before ChunkEntries.close closes that connection.
        self.checkpoint_executor.shutdown(wait=True)
        self.account_stop_executor.shutdown(wait=True)
        super().close()
        for engine in tuple(self.shards.values()):
            engine.close()
            engine.bridge.executor.shutdown(wait=True)
            engine.bridge.db.close()
        self._refresh_projections()

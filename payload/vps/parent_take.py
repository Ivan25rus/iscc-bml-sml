"""Durable partial take for an exact parent, independently triggered per account.

Only explicit arming creates permission. A firing claim is committed before
closing, never replayed after interruption, and uses the existing owned-parent
partial-close engine for native remainder proof and acquisition cancellation.
"""
import copy
import hashlib
import json
import math
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from parent_close_plan import partial_close_plan

CAPABILITY='entry-parent-partial-take-v1'


class ParentTakeRules:
    def __init__(self, entries):
        self.entries=entries;self.storage=entries.storage
        self.control=threading.RLock();self.jobs={};self.records={};self.stop=threading.Event();self.wake=threading.Event();self.thread=None
        self.executor=ThreadPoolExecutor(max_workers=17,thread_name_prefix='iscc-parent-take')
        with self.storage.lock:
            self.storage.db.execute('CREATE TABLE IF NOT EXISTS parent_take_rules (id TEXT PRIMARY KEY,payload TEXT NOT NULL)')
            self.storage.db.execute('CREATE TABLE IF NOT EXISTS parent_take_commands (id TEXT PRIMARY KEY,digest TEXT NOT NULL,result TEXT NOT NULL)')
            for identifier,text in self.storage.db.execute('SELECT id,payload FROM parent_take_rules').fetchall():
                rule=json.loads(text);self.records[identifier]=copy.deepcopy(rule);changed=False
                for state in rule['accounts'].values():
                    if state['state']=='firing':state.update(state='unknown',reason='Interrupted take; no automatic replay');changed=True
                if changed:self._write(rule);self.records[identifier]=copy.deepcopy(rule)
            self.storage.db.commit()

    def _write(self, rule):
        self.storage.db.execute('INSERT OR REPLACE INTO parent_take_rules VALUES (?,?)',(rule['id'],json.dumps(rule,allow_nan=False)))

    def list(self):
        with self.storage.lock:
            return copy.deepcopy(sorted(self.records.values(),key=lambda r:r['createdAt'],reverse=True))

    def receipt(self, identifier):
        with self.storage.lock:
            row=self.storage.db.execute('SELECT result FROM parent_take_commands WHERE id=?',(identifier,)).fetchone()
            return json.loads(row[0]) if row else None

    @staticmethod
    def _number(value):
        if type(value) not in (int,float) or not math.isfinite(value) or value<=0:raise ValueError('Invalid partial take price or percentage')
        return value

    def command(self, command):
        from chunk_entry import digest
        identifier,entry_id=command.get('id'),command.get('entryId')
        if (not isinstance(identifier,str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,100}',identifier)
                or not isinstance(entry_id,str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,100}',entry_id)):
            raise ValueError('Invalid partial take identity')
        operation=command.get('operation')
        required={'id','entryId','symbol','operation','expiresAt'}
        if operation=='arm':required|={'targetPrice','closePercent'}
        elif operation=='cancel':required|={'ruleId'}
        else:raise ValueError('Invalid partial take operation')
        if set(command)!=required:raise ValueError('Invalid partial take command')
        fingerprint=digest(command)
        with self.control:
            with self.storage.lock:
                saved=self.storage.db.execute('SELECT digest,result FROM parent_take_commands WHERE id=?',(identifier,)).fetchone()
            if saved:
                if saved[0]!=fingerprint:raise ValueError('Partial take command ID reused')
                return dict(result=json.loads(saved[1]))
            self.entries._expiry(command)
            found=self.entries._load(entry_id)
            if not found or found[1].get('accountExecution')!='independent-v5' or found[1]['symbol']!=command['symbol']:
                raise ValueError('Exact independent parent not confirmed')
            _,payload,parent=found
            if operation=='cancel':
                rule=next((r for r in self.list() if r['id']==command['ruleId'] and r['entryId']==entry_id),None)
                if not rule:raise ValueError('Partial take does not belong to this parent')
                if any(s['state'] in ('firing','unknown') for s in rule['accounts'].values()):
                    raise ValueError('A partial take outcome is unresolved; cancellation cannot revoke a submitted close')
                for state in rule['accounts'].values():
                    if state['state']=='armed':state.update(state='cancelled',reason='Cancelled by user')
            else:
                price=self._number(command['targetPrice']);percent=self._number(command['closePercent'])
                if percent>=100:raise ValueError('Partial take percent must be below 100')
                if sum(any(s['state'] in ('armed','firing','unknown') for s in r['accounts'].values()) for r in self.list())>=32:
                    raise ValueError('Too many unresolved partial takes')
                if any(r['entryId']==entry_id and any(s['state'] in ('armed','firing','unknown') for s in r['accounts'].values()) for r in self.list()):
                    raise ValueError('Cancel or reconcile the existing partial take before arming a new one')
                if parent.get('protectionPending') or parent.get('closurePending'):raise ValueError('Parent control is unresolved')
                # These checks never send a close. All captured accounts must
                # confirm the price, owned lots and a valid rounded allocation.
                for account in parent['accounts']:
                    engine=self.entries.shards.get(account);local=engine._load(entry_id) if engine else None
                    if not local:raise ValueError('Account parent is not admitted: '+account)
                    if local[2].get('closurePending') or local[2].get('protectionPending') or local[2]['phase']=='unknown':
                        raise ValueError('Account control is unresolved: '+account)
                    rows=engine._owned_close_view(local[1],local[2],account)
                    plan=partial_close_plan(rows,local[2]['accounts'][account]['spec'],percent)
                    if not plan['volume']:raise ValueError('No owned position on account '+account)
                    data=engine._call(account,'snapshot',payload['symbol'])
                    quote=self._quote(data,account,payload['symbol'])
                    step=self._number(data['metadata']['tickSize'])
                    if abs(price/step-round(price/step))>1e-6:raise ValueError('Partial take price does not match native tick')
                    buy=payload['side']=='buy'
                    if (quote['bid']>=price if buy else quote['ask']<=price):raise ValueError('Partial take price already reached on '+account)
                    for row in rows:
                        if row['status']!='open':continue
                        if row['tp'] and (price>=row['tp'] if buy else price<=row['tp']):
                            raise ValueError('Partial take must precede the ordinary TP on '+account)
                        if row['sl'] and (price<=row['sl'] if buy else price>=row['sl']):
                            raise ValueError('Partial take must stay beyond the stop on '+account)
                rule=dict(id=identifier,entryId=entry_id,symbol=payload['symbol'],side=payload['side'],
                    targetPrice=price,closePercent=percent,createdAt=time.time()*1000,
                    accounts={account:dict(state='armed') for account in parent['accounts']})
            result=dict(id=identifier,entryId=entry_id,status='ok',operation=operation,rule=copy.deepcopy(rule))
            self.entries._expiry(command)
            with self.storage.lock:
                if operation=='cancel':
                    rule=copy.deepcopy(self.records[command['ruleId']])
                    if any(s['state'] in ('firing','unknown') for s in rule['accounts'].values()):
                        raise ValueError('A partial take is already firing; cancellation cannot revoke its close')
                    for state in rule['accounts'].values():
                        if state['state']=='armed':state.update(state='cancelled',reason='Cancelled by user')
                    result['rule']=copy.deepcopy(rule)
                self._write(rule)
                self.storage.db.execute('INSERT INTO parent_take_commands VALUES (?,?,?)',(identifier,fingerprint,json.dumps(result)))
                self.storage.db.commit()
                self.records[rule['id']]=copy.deepcopy(rule)
                retired=[r for r in self.list() if all(s['state'] not in ('armed','firing','unknown') for s in r['accounts'].values())]
                for old in retired[100:]:
                    self.storage.db.execute('DELETE FROM parent_take_rules WHERE id=?',(old['id'],));self.records.pop(old['id'],None)
                self.storage.db.commit()
            self.wake.set()
            return dict(result=result)

    def _quote(self, data, account, symbol):
        quote=data.get('quote',{});stamp=quote.get('utcTime')
        if (str(data.get('account'))!=account or data.get('server')!=self.entries.bridge.config['server']
                or data.get('symbol')!=symbol or type(stamp) not in (int,float) or not math.isfinite(stamp)
                or not -0.25<=time.time()-stamp<=1
                or any(type(quote.get(k)) not in (int,float) or not math.isfinite(quote[k]) or quote[k]<=0 for k in ('bid','ask'))
                or quote['ask']<quote['bid']):raise ValueError('No fresh exact account quote')
        return quote

    def _state(self, identifier, account, state, **values):
        with self.storage.lock:
            rule=json.loads(self.storage.db.execute('SELECT payload FROM parent_take_rules WHERE id=?',(identifier,)).fetchone()[0])
            current=rule['accounts'][account]
            if state=='firing' and current['state']!='armed':return False
            if state!='firing' and current['state']!='firing':return False
            current.update(state=state,**values);self._write(rule);self.storage.db.commit()
            self.records[rule['id']]=copy.deepcopy(rule);return True

    def _evaluate(self, rule, account):
        engine=self.entries.shards.get(account)
        if engine is None:return
        try:
            data=engine._call(account,'snapshot',rule['symbol']);quote=self._quote(data,account,rule['symbol'])
            if (quote['bid']<rule['targetPrice'] if rule['side']=='buy' else quote['ask']>rule['targetPrice']):return
        except (OSError,ValueError,KeyError,TypeError,RuntimeError):return  # Wait for a fresh quote; no close intent exists yet.
        if not self._state(rule['id'],account,'firing',claimedAt=time.time()*1000):return
        close_id='partial-take:'+hashlib.sha256((rule['id']+':'+account).encode()).hexdigest()
        try:
            result=engine.close_position(dict(id=close_id,entryId=rule['entryId'],symbol=rule['symbol'],
                closePercent=rule['closePercent'],expiresAt=time.time()*1000+2000))['result']
            outcome=result.get('accountResults',{}).get(account,{})
            self._state(rule['id'],account,'completed' if result['status']=='ok' else result['status'],commandId=close_id,
                filledVolume=outcome.get('filledVolume'),remainingVolume=outcome.get('remainingVolume'),reason=result.get('reason',''))
        except Exception as error:
            receipt=engine._closure_receipt(close_id)
            status=receipt[1]['status'] if receipt else 'rejected' if isinstance(error,ValueError) else 'unknown'
            self._state(rule['id'],account,status,commandId=close_id,reason=str(error)[:256])

    def tick(self):
        if self.stop.is_set():return
        with self.control:
            for rule in self.list():
                for account,state in rule['accounts'].items():
                    key=(rule['id'],account)
                    if state['state']=='armed' and (key not in self.jobs or self.jobs[key].done()):
                        self.jobs[key]=self.executor.submit(self._evaluate,rule,account)
            self.jobs={key:job for key,job in self.jobs.items() if not job.done()}

    def start(self):
        if self.thread and self.thread.is_alive():return
        def run():
            while not self.stop.is_set():
                self.tick()
                active=any(s['state']=='armed' for r in self.list() for s in r['accounts'].values())
                self.wake.wait(.25 if active else 30);self.wake.clear()
        self.thread=threading.Thread(target=run,name='iscc-parent-take',daemon=True);self.thread.start()

    def close(self):
        self.stop.set();self.wake.set()
        if self.thread:self.thread.join(timeout=5)
        self.executor.shutdown(wait=True)

"""Account-local controls must progress despite a peer's preflight or send."""
import copy
import hashlib
import json
import pathlib
import sys
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
import test_entry_independence as fixture
from chunk_entry import AccountPlanEntries, IsolatedEntries, digest
from mt5_bridge import CoordinatorBridge


class AccountControlTests(unittest.TestCase):
    def model(self):
        case=fixture.EntryIndependenceTests()
        self.addCleanup(case.doCleanups)
        f,root=case.model()
        f.engine.arm(f.payload)
        fixture.support.IsolatedEngineTests.until(self,f,lambda state:state['phase']=='completed')
        return f,root

    def command(self,f,identifier='control',**values):
        return dict(id=identifier,entryId=f.payload['id'],symbol=f.payload['symbol'],account='900000002',
                    mirrorAccounts=[dict(account='900000001')],operation='modify',sl=24990.,
                    expiresAt=time.time()*1000+3000,**values)

    def legacy_direction(self, symbol):
        case=fixture.EntryIndependenceTests();self.addCleanup(case.doCleanups)
        f,root=case.model();account='900000001';native=f.natives[1];worker=f.bridges[1]
        for bridge in (root,*f.bridges):
            if symbol not in bridge.config['symbols']:bridge.config['symbols'].append(symbol)
        payload=dict(id='historical-'+symbol,symbol=symbol,side='sell',accountExecution='independent-v4',
                     executionPolicy='hybrid-market-limit-v1')
        positions=[('71',.8,7644.33)] if symbol=='SPX500' else [('71',.2,28971.83),('72',.2,28970.20)]
        details=dict(multiplier=1,filledVolume=sum(volume for _,volume,_ in positions),targetVolume=.8,
                     commonStopUncertain=True,riskRepairRequired=symbol=='SPX500',entryStatus='unknown')
        parent=dict(payload,phase='unknown',accounts={'900000002':dict(multiplier=1,filledVolume=0,targetVolume=.8),account:details},
                    lanePhases={'900000002':'stopped',account:'unknown'},children=[],stopRevision=3)
        for index,(ticket,volume,price) in enumerate(positions,1):
            market=symbol=='USTEC';number=int(ticket)
            parent['children'].append(dict(id=payload['id']+':h'+str(index),index=index,state='confirmed',type='market' if market else 'limit',
                volumes={account:volume},positions={account:dict(ticket=ticket,openingTicket=ticket,volume=volume,fillPrice=price)},
                result=dict(accountResults={account:dict(status='ok',providerOrderId=ticket)})))
            native.history_orders.append(SimpleNamespace(ticket=number,symbol=symbol,position_id=number,type=1 if market else 3,
                state=4,volume_initial=volume,sl=price+4,tp=0.))
            native.history_deals.extend([SimpleNamespace(ticket=number*10+1,order=number,position_id=number,symbol=symbol,
                entry=0,volume=volume,price=price,type=1,reason=0),SimpleNamespace(ticket=number*10+2,order=number*100,position_id=number,
                symbol=symbol,entry=1,volume=volume,price=price+4,type=0,reason=4)])
        if symbol=='USTEC':
            parent['children'].append(dict(id=payload['id']+':h3',index=3,state='cancelled',type='limit',
                volumes={account:.6},positions={},result=dict(accountResults={account:dict(status='ok',providerOrderId='73')})))
            parent['children'].append(dict(id=payload['id']+':h4',index=4,state='rejected',type='market',
                volumes={account:.2},positions={},result=dict(accountResults={account:dict(status='rejected')})))
            native.history_orders.append(SimpleNamespace(ticket=73,symbol=symbol,position_id=0,type=3,state=2))
        stop_id='hybrid-sl:'+hashlib.sha256((payload['id']+':1:'+account).encode()).hexdigest()
        receipt=dict(account=account,symbol=symbol,status='rejected',reason='Common stop exceeds the reserved risk budget'
            if symbol=='SPX500' else 'Captured entry position closed or changed')
        worker.db.execute('INSERT INTO entry_common_stops VALUES (?,?,?)',(stop_id,'original',json.dumps(receipt)))
        worker.db.commit()
        self.save_legacy_direction(root,payload,parent)
        return f,root,payload,parent,stop_id

    @staticmethod
    def save_legacy_direction(root,payload,parent):
        with root.lock:
            root.db.execute('INSERT OR REPLACE INTO chunk_entries VALUES (?,?,?,?)',
                            (payload['id'],digest(payload),json.dumps(payload),json.dumps(parent)))
            root.db.commit()

    def test_exact_legacy_direction_read_clears_only_the_proven_account_without_a_trade(self):
        for symbol in ('SPX500','USTEC'):
            with self.subTest(symbol=symbol):
                f,root,payload,parent,stop_id=self.legacy_direction(symbol)
                self.assertTrue(AccountPlanEntries._holds_account_direction(parent,'900000001'))
                raw=f.bridges[1].db.execute('SELECT * FROM entry_common_stops WHERE id=?',(stop_id,)).fetchone()
                exact=f.engine.get(payload['id'])
                self.assertTrue(exact['accounts']['900000001']['positionsClosed'])
                saved=f.engine._load(payload['id'])[2]
                self.assertFalse(AccountPlanEntries._holds_account_direction(saved,'900000001'))
                self.assertEqual(saved['accounts']['900000002'],parent['accounts']['900000002'])
                self.assertEqual(saved['children'],parent['children'])
                self.assertEqual(saved['lanePhases'],parent['lanePhases'])
                self.assertEqual(f.bridges[1].db.execute('SELECT * FROM entry_common_stops WHERE id=?',(stop_id,)).fetchone(),raw)
                self.assertEqual([n.sent for n in f.natives],[[],[]])

    def test_legacy_direction_preserves_the_hold_for_incomplete_native_or_journal_proof(self):
        problems={'missing exit':'exact-full-exit-unconfirmed','uncertain stop':'native-common-stop-unconfirmed',
                  'older uncertain stop':'native-common-stop-unconfirmed','unknown child':'settled-opening-journal-unconfirmed',
                  'live captured limit':'owned-order-or-position-still-present','wrong exit identity':'exact-full-exit-unconfirmed',
                  'duplicate captured ownership':'captured-ownership-unconfirmed'}
        for problem, failure in problems.items():
            with self.subTest(problem=problem):
                f,root,payload,parent,stop_id=self.legacy_direction('USTEC')
                native=f.natives[1];worker=f.bridges[1]
                if problem=='missing exit':native.history_deals.pop()
                if problem=='wrong exit identity':native.history_deals[0].price+=1
                if problem=='uncertain stop':
                    worker.db.execute('UPDATE entry_common_stops SET result=? WHERE id=?',(json.dumps(dict(status='unknown')),stop_id))
                    worker.db.commit()
                if problem=='older uncertain stop':
                    latest='hybrid-sl:'+hashlib.sha256((payload['id']+':2:900000001').encode()).hexdigest()
                    worker.db.execute('INSERT INTO entry_common_stops SELECT ?,digest,result FROM entry_common_stops WHERE id=?',(latest,stop_id))
                    worker.db.execute('UPDATE entry_common_stops SET result=? WHERE id=?',(json.dumps(dict(status='unknown')),stop_id))
                    worker.db.commit()
                if problem=='unknown child':
                    parent['children'][-1]['state']='unknown';self.save_legacy_direction(root,payload,parent)
                if problem=='live captured limit':
                    native.orders.append(SimpleNamespace(ticket=73,identifier=73,symbol='USTEC',type=3,
                        volume_current=.6,price_open=29000.,sl=29004.,tp=0.,position_id=0,comment='old'))
                if problem=='duplicate captured ownership':
                    self.save_legacy_direction(root,dict(payload,id='other-owner'),dict(parent,id='other-owner'))
                exact=f.engine.get(payload['id'])
                self.assertTrue(exact['accounts']['900000001']['commonStopUncertain'])
                self.assertEqual(exact['accounts']['900000001']['legacyDirectionCheck'],failure)
                self.assertEqual(f.engine._load(payload['id'])[2],parent)
                self.assertEqual([n.sent for n in f.natives],[[],[]])

    def test_exact_legacy_direction_can_admit_only_a_fresh_new_intent(self):
        f,_,payload,parent,_=self.legacy_direction('USTEC')
        f.payload.update(side='sell',sl=25016.)
        for native in f.natives:
            native.side='sell';native.tick.bid,native.tick.ask=24998.,24998.2
        initial=f.engine.arm(f.payload)
        self.assertEqual(initial['lanePhases']['900000001'],'armed')
        result=fixture.support.IsolatedEngineTests.until(self,f,lambda state:state['phase']=='completed')
        self.assertEqual(result['accounts']['900000001']['filledVolume'],1.)
        saved=f.engine._load(payload['id'])[2]
        self.assertTrue(saved['accounts']['900000001']['positionsClosed'])
        self.assertEqual(saved['children'],parent['children'])
        self.assertTrue(all(row.comment=='cid:'+f.payload['cid'] for n in f.natives for row in n.positions))

    def test_controls_skip_only_the_account_stopped_before_admission(self):
        case=fixture.EntryIndependenceTests()
        self.addCleanup(case.doCleanups)
        f,root=case.model()
        account='900000001'
        original=AccountPlanEntries._direction_rows
        def direction_rows(storage,payload,legacy_only=False):
            if legacy_only:
                return [({},dict(phase='running',accounts={account:{}}))]
            return original(storage,payload,legacy_only)
        with patch.object(AccountPlanEntries,'_direction_rows',side_effect=direction_rows):
            f.engine.arm(f.payload)
        entry=fixture.support.IsolatedEngineTests.until(self,f,lambda state:state['lanePhases']['900000002']=='completed')
        self.assertTrue(f.engine.account_was_not_admitted(entry,account))
        self.assertFalse(f.engine.account_was_not_admitted(entry,'900000002'))
        self.assertFalse(f.natives[1].sent)

        command=self.command(f,identifier='skip-unadmitted-tp')
        command.pop('sl');command['tp']=25040.
        response=f.engine.protection(command)['result']
        self.assertEqual(response['status'],'ok',response)
        self.assertTrue(response['accountResults'][account]['noPosition'])
        self.assertTrue(all(row.tp==25040. for row in f.natives[0].positions))
        self.assertEqual(root.receipt(command['id'])['result'],response)

        close=dict(id='partial-with-unadmitted-peer',entryId=f.payload['id'],symbol=f.payload['symbol'],
                   closePercent=50,expiresAt=time.time()*1000+3000)
        partial=f.engine.close_position(close)['result']
        self.assertEqual(partial['status'],'ok',partial)
        self.assertEqual(partial['accountResults'][account]['remainingVolume'],0)
        self.assertAlmostEqual(sum(row.volume for row in f.natives[0].positions),.5)
        close.pop('closePercent');close.update(id='close-with-unadmitted-peer',expiresAt=time.time()*1000+3000)
        full=f.engine.close_position(close)
        self.assertEqual(full['result']['status'],'ok',full)
        self.assertEqual(full['entry']['positionLifecycle'],'closed')
        self.assertFalse(f.natives[0].positions)
        self.assertFalse(f.natives[1].sent)

    def test_protection_skips_empty_admitted_mirror_and_preserves_its_working_limit(self):
        account = '900000001'
        for pending in (False, True):
            with self.subTest(pending=pending):
                case = fixture.EntryIndependenceTests()
                self.addCleanup(case.doCleanups)
                f, root = case.model()
                native = f.natives[1]
                if pending:
                    f.quote(native,25008.)
                else:
                    native.tick.time -= 3
                f.engine.arm(f.payload)
                fixture.support.IsolatedEngineTests.until(self,f,lambda state:state['lanePhases']['900000002']=='completed'
                    and state['accounts'][account]['filledVolume']==0 and bool(native.orders)==pending
                    and f.engine.shards[account]._load(f.payload['id']) is not None)
                self.assertFalse(native.positions)
                self.assertFalse(f.engine.account_was_not_admitted(f.state(),account))
                orders = copy.deepcopy(native.orders)
                sends = list(native.sent)
                command = self.command(f,'protect-no-fill');command.pop('sl');command['tp']=25040.
                result = f.engine.protection(command)['result']
                self.assertEqual(result['status'],'ok',result)
                mirror = result['accountResults'][account]
                self.assertTrue(mirror['noPosition'])
                self.assertEqual('working limit unchanged' in mirror['reason'],pending)
                self.assertTrue(all(row.tp==25040. for row in f.natives[0].positions))
                self.assertEqual(native.orders,orders)
                self.assertEqual(native.sent,sends)
                local = f.engine.shards[account]._protection_receipt(mirror['controlId'])[1]
                self.assertTrue(local['accountResults'][account]['noPosition'])
                self.assertEqual(root.receipt(command['id'])['result'],result)

    def test_protection_skips_exited_mirror_only_after_exact_history(self):
        f, root = self.model()
        account, native = '900000001', f.natives[1]
        for position in list(native.positions):
            order = next(row for row in native.history_orders if row.ticket==position.ticket)
            order.type, order.volume_initial, order.sl, order.tp = position.type, position.volume, position.sl, position.tp
            incoming = next(row for row in native.history_deals if row.position_id==position.identifier)
            incoming.ticket, incoming.order = position.ticket*100+1, order.ticket
            incoming.type, incoming.price = position.type, position.price_open
            f.native_exit(native,position,position.volume)
            exit = native.history_deals[-1]
            exit.ticket, exit.order = position.ticket*100+2, position.ticket+100
            exit.type, exit.price = 1-position.type, position.sl
        native.positions.append(native.position_from(91919,dict(symbol=f.payload['symbol'],type=0,volume=.1,
            price=25002.,sl=24980.,tp=25050.,comment='unrelated')))
        old_native = copy.deepcopy(native.positions)
        old_sends = list(native.sent)
        missing_exit = native.history_deals.pop()
        rejected = f.engine.protection(self.command(f,'not-proven-exited'))['result']
        self.assertEqual(rejected['accountResults'][account]['status'],'rejected',rejected)
        self.assertEqual(rejected['accountResults']['900000002']['status'],'ok')
        self.assertNotIn('noPosition',rejected['accountResults'][account])
        native.history_deals.append(missing_exit)

        result = f.engine.protection(self.command(f,'proven-exited'))['result']
        self.assertEqual(result['status'],'ok',result)
        self.assertTrue(result['accountResults'][account]['noPosition'])
        self.assertEqual(result['accountResults'][account]['reason'],'Exact broker full exits confirmed')
        self.assertEqual(root.receipt('proven-exited')['result'],result)
        command = self.command(f,'selected-exited')
        command.update(account=account,mirrorAccounts=[])
        selected = f.engine.protection(command)['result']
        self.assertEqual(selected['accountResults'][account]['status'],'rejected',selected)
        self.assertNotIn('noPosition',selected['accountResults'][account])
        self.assertEqual(native.positions,old_native)
        self.assertEqual(native.sent,old_sends)

    def test_protection_applies_to_every_live_part_of_a_partially_filled_mirror(self):
        case = fixture.EntryIndependenceTests()
        self.addCleanup(case.doCleanups)
        f, _ = case.model()
        account, native = '900000001', f.natives[1]
        def stop_after_two_parts(broker):
            if len(f.markets(broker)) == 2:
                f.quote(broker,25008.)
        native.after_market = stop_after_two_parts
        f.engine.arm(f.payload)
        fixture.support.IsolatedEngineTests.until(self,f,lambda state:state['lanePhases']['900000002']=='completed'
            and 0<state['accounts'][account]['filledVolume']<1 and bool(native.orders))
        self.assertEqual(len(native.positions),2)
        native.after_market = None
        command = self.command(f,'protect-live-parts');command.pop('sl');command['tp']=25040.
        result = f.engine.protection(command)['result']
        self.assertEqual(result['status'],'ok',result)
        self.assertNotIn('noPosition',result['accountResults'][account])
        self.assertEqual({row['ticket'] for row in result['accountResults'][account]['positions']},
                         {str(row.ticket) for row in native.positions})
        self.assertEqual(len(native.positions),2)
        self.assertTrue(all(row.tp==25040. for row in native.positions))

    def test_protection_requires_actual_position_for_a_remapped_pending_fill(self):
        case = fixture.EntryIndependenceTests()
        self.addCleanup(case.doCleanups)
        f, _ = case.model()
        account, native = '900000001', f.natives[1]
        f.quote(native,25008.)
        f.engine.arm(f.payload)
        fixture.support.IsolatedEngineTests.until(self,f,lambda state:state['lanePhases']['900000002']=='completed'
            and state['accounts'][account]['filledVolume']==0 and bool(native.orders))
        order = native.orders[0]
        order.volume_current -= .25
        order.position_id = order.ticket+40
        sends = list(native.sent)
        command = self.command(f,'unproved-remap');command.pop('sl');command['tp']=25040.
        result = f.engine.protection(command)['result']
        self.assertEqual(result['accountResults'][account]['status'],'rejected',result)
        self.assertEqual(native.sent,sends)

        actual = native.position_from(order.ticket+20,dict(symbol=order.symbol,type=0,volume=.25,
            price=order.price_open,sl=order.sl,tp=order.tp,comment=order.comment))
        actual.identifier = order.position_id
        native.positions.append(actual)
        native.history_deals.append(SimpleNamespace(symbol=order.symbol,position_id=actual.identifier,entry=0,volume=.25,reason=0))
        send = native.order_send
        def retain_opening_link(request):
            response = send(request)
            if request['action']==native.TRADE_ACTION_REMOVE and request['order']==order.ticket:
                native.history_orders[-1].position_id = actual.identifier
            return response
        native.order_send = retain_opening_link
        command['id'] = 'proven-remap'
        result = f.engine.protection(command)['result']
        self.assertEqual(result['status'],'ok',result)
        self.assertNotIn('noPosition',result['accountResults'][account])
        self.assertEqual(result['accountResults'][account]['positions'][0]['ticket'],str(actual.ticket))
        self.assertEqual(actual.tp,25040.)

    def test_slow_account_preflight_does_not_delay_peer_modification_or_root_reads(self):
        for slow in ('900000002','900000001'):
            with self.subTest(account=slow):
                f,root=self.model();entered,release=threading.Event(),threading.Event()
                self.addCleanup(release.set)
                worker=root.primary_worker if slow=='900000002' else root.linked[slow]
                def before(method,args):
                    if method=='entry_protection_plan':entered.set();release.wait()
                worker.before=before
                healthy=f.natives[1] if slow=='900000002' else f.natives[0]
                with ThreadPoolExecutor(max_workers=1) as executor:
                    job=executor.submit(f.engine.protection,self.command(f))
                    try:
                        self.assertTrue(entered.wait(10 if sys.platform=='win32' else 2))
                        deadline=time.monotonic()+(10 if sys.platform=='win32' else 3)
                        healthy_account='900000001' if slow=='900000002' else '900000002'
                        while time.monotonic()<deadline and root.receipt('control')['result']['accountResults'][healthy_account]['status']!='ok':time.sleep(.002)
                        self.assertTrue(all(row.sl==24990. for row in healthy.positions))
                        self.assertEqual(root.receipt('control')['result']['accountResults'][healthy_account]['status'],'ok')
                    finally:release.set()
                    self.assertEqual(job.result()['result']['status'],'ok')

    def test_unknown_peer_does_not_block_new_levels_or_closing_healthy_account(self):
        f,root=self.model()
        f.natives[1].sl_failure=dict(at=f.natives[1].protection_sends+1,apply=True,unknown=True)
        first=f.engine.protection(self.command(f))
        self.assertEqual(first['result']['status'],'unknown')
        self.assertEqual(first['result']['accountResults']['900000002']['status'],'ok')
        self.assertEqual(first['entry']['lanePhases']['900000002'],'completed')
        counts=[len(native.sent) for native in f.natives]
        self.assertEqual(f.engine.protection(self.command(f,identifier='control'))['result'],first['result'])
        self.assertEqual([len(native.sent) for native in f.natives],counts)
        next_command=self.command(f,identifier='new-levels');next_command['sl']=24991.
        second=f.engine.protection(next_command)
        self.assertEqual(second['result']['status'],'rejected')
        self.assertEqual(second['result']['accountResults']['900000002']['status'],'ok')
        self.assertEqual(second['result']['accountResults']['900000001']['status'],'rejected')
        self.assertTrue(all(row.sl==24991. for row in f.natives[0].positions))
        self.assertEqual(len(f.natives[1].sent),counts[1])
        close=dict(id='close-healthy',entryId=f.payload['id'],symbol=f.payload['symbol'],expiresAt=time.time()*1000+3000)
        result=f.engine.close_position(close)['result']
        self.assertEqual(result['accountResults']['900000002']['status'],'ok')
        self.assertEqual(result['accountResults']['900000001']['status'],'rejected')
        self.assertFalse(f.natives[0].positions)
        self.assertTrue(f.natives[1].positions)

    def test_known_refusal_and_new_levels_are_independent_and_survive_restart(self):
        f,root=self.model()
        def refused(request):
            return dict(account='900000001',symbol=f.payload['symbol'],status='rejected',
                        retryableQuoteWait=True,mutationStarted=False,reason='Waiting for quote')
        with patch.object(f.bridges[1],'entry_common_stop',side_effect=refused):
            result=f.engine.protection(self.command(f))['result']
        self.assertEqual(result['status'],'rejected')
        self.assertTrue(result['partialProtection'])
        next_command=self.command(f,identifier='new-levels');next_command['sl']=24991.
        result=f.engine.protection(next_command)['result']
        self.assertEqual(result['status'],'ok',result)
        self.assertTrue(all(row.sl==24991. for native in f.natives for row in native.positions))
        counts=[len(native.sent) for native in f.natives]
        f.engine.close();f.engine=IsolatedEntries(root);root.chunk_entries=f.engine
        self.addCleanup(f.engine.close)
        self.assertEqual(f.engine.protection(next_command)['result']['status'],'ok')
        self.assertEqual([len(native.sent) for native in f.natives],counts)
        changed=copy.deepcopy(next_command);changed['tp']=25040.
        with self.assertRaisesRegex(ValueError,'reused'):f.engine.protection(changed)

    def test_unknown_receipt_is_assembled_from_account_journals_without_replay(self):
        f,root=self.model();command=self.command(f)
        self.assertEqual(f.engine.protection(command)['result']['status'],'ok')
        with root.lock:
            result=f.engine._protection_receipt(command['id'])[1]
            result.update(status='unknown',accountResults={a:dict(status='unknown') for a in result['accountResults']})
            root.db.execute('UPDATE chunk_entry_protections SET result=? WHERE id=?',(json.dumps(result),command['id']));root.db.commit()
        counts=[len(native.sent) for native in f.natives]
        self.assertEqual(root.receipt(command['id'])['result']['status'],'ok')
        self.assertEqual([len(native.sent) for native in f.natives],counts)

    def test_timed_out_stop_recovers_without_resending_and_three_accounts_can_reenter(self):
        for recover_before_exit in (True,False):
            with self.subTest(recover_before_exit=recover_before_exit):
                self.check_timed_out_stop_reentry(recover_before_exit)

    def check_timed_out_stop_reentry(self, recover_before_exit):
        f=fixture.support.fixture.HybridProtectionTests();f.setUp();self.addCleanup(f.doCleanups)
        accounts=('900000003','900000001','900000004')
        f.make(accounts=3,account_ids=tuple(map(int,accounts)))
        root=CoordinatorBridge(f.primary.config,f.paths[0],fixture.RPC(f.primary))
        root.linked={account:fixture.RPC(bridge) for account,bridge in zip(accounts[1:],f.bridges[1:])}
        self.addCleanup(root.db.close);self.addCleanup(root.close_group_executor);self.addCleanup(root.chunk_entries.close)
        f.engine=root.chunk_entries
        f.payload.update(accountExecution='independent-v5',chunkPercent=25,chunkVolume=.25,fillPriority='risk')
        for native in f.natives:native.info.volume_step=native.info.volume_min=.01
        f.engine.arm(f.payload)
        fixture.support.IsolatedEngineTests.until(self,f,lambda state:state['phase']=='completed')
        lane=f.engine.shards['900000004']
        native_stop=f.bridges[-1].entry_common_stop
        def lose_reply(request):
            self.assertEqual(native_stop(request)['status'],'ok')
            raise RuntimeError('Linked MT5 response unknown; inspect receipt')
        command=dict(id='lost-stop-reply',entryId=f.payload['id'],symbol='USTEC',account=accounts[0],
            mirrorAccounts=[dict(account=account) for account in accounts[1:]],operation='modify',sl=24990.,
            expiresAt=time.time()*1000+3000)
        with patch.object(lane,'_reconcile_timed_out_protection',return_value=False), \
                patch.object(f.bridges[-1],'entry_common_stop',side_effect=lose_reply) as sent:
            response=f.engine.protection(command)
            self.assertEqual(sent.call_count,1)
        self.assertEqual(response['result']['accountResults']['900000004']['status'],'unknown')
        self.assertIn('protectionPending',lane._load(f.payload['id'])[2])
        counts=[len(native.sent) for native in f.natives]
        if recover_before_exit:
            position=f.natives[-1].positions[0];position.sl=24989.
            self.assertEqual(root.receipt(command['id'])['result']['status'],'unknown')
            self.assertIn('protectionPending',lane._load(f.payload['id'])[2])
            position.sl=24990.
            recovered=root.receipt(command['id'])['result']
            self.assertEqual(recovered['status'],'ok',recovered)
            self.assertNotIn('protectionPending',lane._load(f.payload['id'])[2])
            self.assertEqual(recovered['accountResults']['900000004']['sl'],24990.)
            self.assertTrue(all(row.tp==0. for row in f.natives[-1].positions))
        old_tickets=[{position.ticket for position in native.positions} for native in f.natives]
        for native in f.natives:
            for index,position in enumerate(native.positions):
                order=next(row for row in native.history_orders if row.ticket==position.ticket)
                order.type=0;order.volume_initial=position.volume;order.sl=position.sl;order.tp=position.tp
                deal=next(row for row in native.history_deals if row.position_id==position.identifier)
                deal.ticket=position.ticket*100+1;deal.order=order.ticket;deal.type=0;deal.price=position.price_open
                native.history_deals.append(SimpleNamespace(ticket=position.ticket*100+2,order=999999+index,
                    position_id=position.identifier,symbol='USTEC',entry=1,volume=position.volume,
                    price=position.sl,type=1,reason=4))
            native.positions.clear()
        self.assertEqual([len(native.sent) for native in f.natives],counts)
        old_id=f.payload['id']
        if not recover_before_exit:
            f.engine.close()
            f.engine=IsolatedEntries(root);root.chunk_entries=f.engine
            self.addCleanup(f.engine.close)
            lane=f.engine.shards['900000004']
            self.assertIn('protectionPending',lane._load(old_id)[2])
        f.payload.update(id='after-broker-stop',cid='after-broker-stop')
        f.engine.arm(f.payload)
        next_entry=fixture.support.IsolatedEngineTests.until(self,f,lambda state:state['phase']=='completed')
        self.assertEqual({account:row['filledVolume'] for account,row in next_entry['accounts'].items()},dict.fromkeys(accounts,1.))
        self.assertNotIn('protectionPending',lane._load(old_id)[2])
        for native,count,tickets in zip(f.natives,counts,old_tickets):
            self.assertFalse(any(row.get('position') in tickets for row in native.sent[count:]))

    def archived_spx_close(self, live_order=False, missing_exit=False):
        f=fixture.support.fixture.HybridProtectionTests();f.setUp();self.addCleanup(f.doCleanups)
        accounts=('900000003','900000001','900000004')
        f.make(accounts=3,account_ids=tuple(map(int,accounts)))
        for bridge,native in zip(f.bridges,f.natives):
            bridge.config['symbols'].append('SPX500')
            native.info.volume_step=native.info.volume_min=.01
            f.quote(native,7638.)
        root=CoordinatorBridge(f.primary.config,f.paths[0],fixture.RPC(f.primary))
        root.linked={account:fixture.RPC(bridge) for account,bridge in zip(accounts[1:],f.bridges[1:])}
        self.addCleanup(root.db.close);self.addCleanup(root.close_group_executor)
        self.addCleanup(root.chunk_entries.close)
        f.engine=root.chunk_entries
        f.payload.update(symbol='SPX500',trigger=7637.,level=7637.,sl=7621.,accountExecution='independent-v5',
            chunkPercent=25,chunkVolume=.25,fillPriority='risk',mirrorAccounts=[dict(account='900000001',multiplier=1.)])
        f.engine.arm(f.payload)
        fixture.support.IsolatedEngineTests.until(self,f,lambda state:state['phase']=='completed')
        old_id=f.payload['id'];old=f.engine._load(old_id)[2]
        old['accounts']['900000002']=old['accounts'].pop('900000003')
        old.update(phase='unknown',lanePhases={'900000002':'unknown','900000001':'unknown'},closurePending='archived-spx-close')
        for child in old['children']:
            for key in ('volumes','positions'):
                rows=child.get(key,{})
                if '900000003' in rows:rows['900000002']=rows.pop('900000003')
            results=child.get('result',{}).get('accountResults',{})
            if '900000003' in results:results['900000002']=results.pop('900000003')
            for position in child.get('positions',{}).values():position.pop('openingTicket',None)
        for native in f.natives[:2]:
            for index,position in enumerate(native.positions):
                order=next(row for row in native.history_orders if row.ticket==position.ticket)
                order.type=0;order.volume_initial=position.volume;order.sl=position.sl;order.tp=position.tp
                deal=next(row for row in native.history_deals if row.position_id==position.identifier)
                deal.ticket=position.ticket*100+1;deal.order=order.ticket;deal.type=0;deal.price=position.price_open
                if missing_exit and native is f.natives[1] and index==0:continue
                native.history_deals.append(SimpleNamespace(ticket=position.ticket*100+2,order=999999+index,
                    position_id=position.identifier,symbol='SPX500',entry=1,volume=position.volume,
                    price=position.sl,type=1,reason=4))
            native.positions.clear()
        close=dict(id='archived-spx-close',entryId=old_id,symbol='SPX500',expiresAt=time.time()*1000+3000)
        receipt=dict(id=close['id'],entryId=old_id,status='unknown',reason='Historical whole-parent close not confirmed',accountResults={})
        retired=f.engine._shard('900000002')
        former=f.engine.shards['900000003']._load(old_id)
        with retired.storage.lock:
            retired.storage.db.execute('INSERT INTO chunk_entries VALUES (?,?,?,?)',
                (old_id,former[0],json.dumps(former[1]),json.dumps(former[2])))
            retired.storage.db.commit()
        with root.lock:
            root.db.execute('INSERT INTO chunk_entry_closures VALUES (?,?,?)',(close['id'],digest(close),json.dumps(receipt)))
            root.db.commit()
        f.engine._save(old)
        if live_order:
            child=next(row for row in old['children'] if row['type']=='limit' and '900000001' in row['volumes'])
            ticket=int(child['result']['accountResults']['900000001']['providerOrderId']);native=f.natives[1]
            native.orders.append(SimpleNamespace(ticket=ticket,identifier=ticket,symbol='SPX500',type=native.ORDER_TYPE_BUY_LIMIT,
                volume_current=.1,price_open=7637.,sl=7621.,tp=0.,position_id=0,comment='cid:protection-one'))
        for native in f.natives:f.quote(native,7638.)
        return f,root,old_id,close['id'],receipt,accounts

    def test_archived_spx_close_with_retired_peer_releases_900000001_before_new_entry(self):
        for startup_read in (False,True):
            with self.subTest(startup_read=startup_read):
                f,root,old_id,close_id,receipt,accounts=self.archived_spx_close()
                counts=[len(native.sent) for native in f.natives]
                if startup_read:
                    f.engine.close();f.engine=IsolatedEntries(root);root.chunk_entries=f.engine
                    self.addCleanup(f.engine.close)
                    root.entry_plans()
                    fixture.EntryIndependenceTests.wait(self,
                        lambda:f.engine.shards['900000001']._load(old_id)[2].get('positionsClosed'))
                    self.assertEqual([len(native.sent) for native in f.natives],counts)
                f.payload.update(id='new-spx-after-close',cid='new-spx-after-close',
                    mirrorAccounts=[dict(account=account,multiplier=1.) for account in accounts[1:]])
                f.engine.arm(f.payload)
                next_entry=fixture.support.IsolatedEngineTests.until(self,f,lambda state:state['phase']=='completed')
                self.assertEqual({account:row['filledVolume'] for account,row in next_entry['accounts'].items()},dict.fromkeys(accounts,1.))
                self.assertTrue(f.engine.shards['900000001']._load(old_id)[2].get('positionsClosed'))
                self.assertEqual(f.engine._closure_receipt(close_id)[1],receipt)
                self.assertNotIn('900000002',root.linked)
                self.assertEqual(f.engine.shards['900000002']._load(old_id)[2]['closurePending'],close_id)
                for native,count in zip(f.natives,counts):
                    self.assertFalse(any(row.get('comment')=='ISCC close' for row in native.sent[count:]))

    def test_archived_spx_live_order_or_missing_exit_still_blocks_affected_account(self):
        for live_order in (False,True):
            with self.subTest(live_order=live_order):
                f,root,old_id,close_id,receipt,accounts=self.archived_spx_close(live_order=live_order,missing_exit=not live_order)
                count=len(f.natives[1].sent)
                f.engine.get(old_id)
                f.payload.update(id='new-spx-with-unresolved',cid='new-spx-with-unresolved',
                    mirrorAccounts=[dict(account=account,multiplier=1.) for account in accounts[1:]])
                f.engine.arm(f.payload)
                state=fixture.support.IsolatedEngineTests.until(self,f,lambda state:state['lanePhases']['900000001']=='stopped'
                    and all(state['lanePhases'][account]=='completed' for account in ('900000003','900000004')))
                self.assertEqual(state['accounts']['900000001']['filledVolume'],0.)
                self.assertEqual(f.engine.shards['900000001']._load(old_id)[2].get('closurePending'),close_id)
                self.assertEqual(f.engine._closure_receipt(close_id)[1],receipt)
                self.assertEqual(len(f.natives[1].sent),count)
                self.assertEqual(len(f.natives[1].orders),int(live_order))

    def test_definite_non_quote_refusal_is_not_retried_and_new_user_levels_remain_allowed(self):
        f,root=self.model()
        with patch.object(f.bridges[1],'entry_common_stop',return_value=dict(account='900000001',symbol=f.payload['symbol'],
                status='rejected',mutationStarted=False,finalPreMutationRejection=True,reason='Stops invalid after price moved')) as call:
            result=f.engine.protection(self.command(f))['result']
        self.assertEqual(call.call_count,1)
        self.assertEqual(result['status'],'rejected',result)
        self.assertEqual(result['accountResults']['900000002']['status'],'ok')
        self.assertEqual(result['accountResults']['900000001']['status'],'rejected')
        self.assertEqual(f.engine.protection(self.command(f,identifier='new-intent'))['result']['status'],'ok')

    def test_fractional_tick_user_levels_are_normalized_by_each_broker_and_saved_as_actual_overrides(self):
        f,root=self.model()
        f.natives[0].info.trade_tick_size=.01
        # These are the same half-cent fractions as the two live rejections;
        # weighted entries and user offsets need not land on the broker grid.
        command=self.command(f,tp=25040.005);command['sl']=24990.005
        response=f.engine.protection(command)
        self.assertEqual(response['result']['status'],'ok',response)
        for account,native in zip(('900000002','900000001'),f.natives):
            stop,take=(24990.01,25040.01) if account=='900000002' else (24990.1,25040.1)
            self.assertTrue(all(row.sl==stop and row.tp==take for row in native.positions))
            local=f.engine.shards[account]._load(f.payload['id'])[2]
            self.assertEqual(local['accounts'][account]['protectionOverride'],dict(sl=stop,tp=take))
        counts=[len(native.sent) for native in f.natives]
        self.assertEqual(f.engine.protection(command)['result']['status'],'ok')
        self.assertEqual([len(native.sent) for native in f.natives],counts)

    def test_fractional_sell_stop_rounds_down_without_adding_risk_and_zero_take_is_preserved(self):
        import test_hybrid_protection as support
        case=support.HybridProtectionTests();case.setUp();self.addCleanup(case.doCleanups)
        case.make('sell',2);case.entry()
        for native in case.natives:native.info.trade_tick_size=.01
        response=case.engine.protection(case.command(sl=25010.005,tp=0,mirrorAccounts=[dict(account='900000001')]))
        self.assertEqual(response['result']['status'],'ok',response)
        self.assertTrue(all(row.sl==25010.0 and row.tp==0 for native in case.natives for row in native.positions))

    def test_whole_exit_is_proven_only_after_both_independent_accounts_are_closed(self):
        f,root=self.model()
        command=dict(id='close-both',entryId=f.payload['id'],symbol=f.payload['symbol'],expiresAt=time.time()*1000+3000)
        result=f.engine.close_position(command)
        self.assertEqual(result['result']['status'],'ok',result)
        self.assertTrue(result['entry']['positionsClosed'])
        self.assertFalse(result['entry']['hasOpenPositions'])
        self.assertTrue(result['entry']['positionsConfirmed'])
        self.assertTrue(all(not native.positions for native in f.natives))


if __name__=='__main__':unittest.main()

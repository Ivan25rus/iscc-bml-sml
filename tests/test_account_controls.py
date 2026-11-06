"""Account-local controls must progress despite a peer's preflight or send."""
import copy
import pathlib
import sys
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
import test_entry_independence as fixture
from chunk_entry import IsolatedEntries


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
            import json
            result=f.engine._protection_receipt(command['id'])[1]
            result.update(status='unknown',accountResults={a:dict(status='unknown') for a in result['accountResults']})
            root.db.execute('UPDATE chunk_entry_protections SET result=? WHERE id=?',(json.dumps(result),command['id']));root.db.commit()
        counts=[len(native.sent) for native in f.natives]
        self.assertEqual(root.receipt(command['id'])['result']['status'],'ok')
        self.assertEqual([len(native.sent) for native in f.natives],counts)

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

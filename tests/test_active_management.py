"""Compact UI evidence retains every guard and live target; fake SDK only."""
import copy
import http.client
import json
import pathlib
import socket
import tempfile
import threading
import time
import unittest
from concurrent.futures import Future
from unittest.mock import patch
from test_mt5_bridge import FakeMT5,Bridge
from mt5_bridge import format_management_snapshot,QuotePublisher,create_server
import test_entry_check_batch as fixtures


class ActiveManagementTests(unittest.TestCase):
    def setUp(self):
        directory=tempfile.TemporaryDirectory();self.addCleanup(directory.cleanup)
        self.native=FakeMT5()
        self.bridge=Bridge(self.native,dict(account=900000002,server='Example-MT5',symbols=['SPX500','USTEC'],tradingEnabled=True),pathlib.Path(directory.name)/'fixture.db')
        self.addCleanup(self.bridge.db.close)

    def add(self,table,identifier,state,ticket='999',result='ok',symbol='USTEC'):
        payload=dict(ticket=ticket,symbol=symbol,actions={},detail='historical details '*30)
        self.bridge.db.execute('INSERT INTO '+table+' VALUES (?,?,?,?)',(identifier,json.dumps(payload),state,json.dumps(dict(status=result))))

    def test_older_active_unknown_and_unrecognized_states_survive_more_than_100_final_rows(self):
        for table in ('close_rules','time_rules'):
            for state in ('armed','opening','firing','unknown','future-state'):
                self.add(table,state,state,result='unknown' if state!='armed' else 'ok')
            for i in range(120):self.add(table,'old-'+str(i),'completed')
            compact=self.bridge._management_rule_rows(table,[],True)
            self.assertEqual({row[0] for row in compact},{'armed','opening','firing','unknown','future-state'})
            full=self.bridge._management_rule_rows(table,[],False)
            self.assertEqual(len(full),100);self.assertTrue(all(row[0].startswith('old-') for row in full))

    def test_only_unrelated_definite_history_is_omitted_and_all_live_target_history_remains(self):
        self.add('time_rules','live','completed',ticket='71')
        self.add('time_rules','other-symbol','completed',ticket='71',symbol='SPX500')
        self.add('time_rules','unknown-final','completed',result='unknown')
        self.add('time_rules','missing-outcome','completed',result=None)
        self.add('time_rules','cancelled','cancelled',result='rejected')
        self.add('time_rules','rejected','rejected',result='rejected')
        self.assertEqual({row[0] for row in self.bridge._management_rule_rows('time_rules',[dict(ticket='71',symbol='USTEC')],True)},
            {'live','unknown-final','missing-outcome'})

    def test_compact_keeps_native_identity_position_watch_and_receipts_with_no_extra_or_missing_checks(self):
        self.add('time_rules','history','completed');self.add('close_rules','uncertain','unknown',ticket='71',result='unknown')
        self.bridge.db.execute('INSERT INTO requests VALUES (?,?,?)',('exact','digest',json.dumps(dict(status='unknown',operation='close',ticket='71'))))
        self.bridge.db.commit()
        before={table:self.bridge.db.execute('SELECT * FROM '+table).fetchall() for table in ('requests','time_rules','close_rules')}
        watch=[dict(ticket='71',symbol='USTEC')]
        counts=[]
        for compact in (False,True):
            with patch.object(self.native,'account_info',wraps=self.native.account_info) as account,patch.object(self.native,'terminal_info',wraps=self.native.terminal_info) as terminal,\
                 patch.object(self.native,'positions_get',wraps=self.native.positions_get) as positions,patch.object(self.native,'orders_get',wraps=self.native.orders_get) as orders:
                state=format_management_snapshot(self.bridge.management_snapshot(['exact'],watch,compact),'900000002')
                counts.append([account.call_count,terminal.call_count,positions.call_count,orders.call_count])
            if compact:current=state
            else:full=state
        # The exact receipt performs its own unchanged identity verification.
        self.assertEqual(counts,[[2,2,1,1],[2,2,1,1]])
        for key in ('orders','openingStates','receipts','account','server','accountInfo'):
            self.assertEqual(current.get(key),full.get(key))
        self.assertEqual(current['timeRules'],[]);self.assertEqual(current['rules'][0]['state'],'unknown')
        self.assertEqual(current['managementView'],'active');self.assertTrue(current['ruleHistoryAvailable'])
        self.assertEqual(before,{table:self.bridge.db.execute('SELECT * FROM '+table).fetchall() for table in before})
        self.native.account.login=9
        with self.assertRaisesRegex(ValueError,'mismatch'):self.bridge.management_snapshot(compact=True)
        self.assertEqual(self.native.sent,[])

    def test_invalid_final_payload_still_fails_instead_of_being_hidden(self):
        self.bridge.db.execute('INSERT INTO time_rules VALUES (?,?,?,?)',('bad','{invalid','completed','{"status":"ok"}'))
        raw=self.bridge.management_snapshot(compact=True)
        with self.assertRaises(ValueError):format_management_snapshot(raw,'900000002')

    def test_coordinator_cache_keeps_full_and_active_evidence_separate(self):
        case=fixtures.EntryCheckBatchTests();self.addCleanup(case.doCleanups)
        _,f,root=case.model();seen=[]
        for worker in (root.primary_worker,*root.linked.values()):worker.before=lambda method,args:seen.append((method,args))
        root.management_accounts(compact=True)
        for _,job in list(root.management_jobs.values()):job.result(timeout=2)
        active=root.management_accounts(compact=True)
        self.assertTrue(all(row['evidenceReady'] and row['managementView']=='active' for row in active.values()))
        for _,job in list(root.management_jobs.values()):job.result(timeout=2)
        for worker in (root.primary_worker,*root.linked.values()):
            worker.before=lambda method,args:time.sleep(.1) if method=='management_snapshot' else None
        cold=root.management_accounts()
        self.assertTrue(all(not row['evidenceReady'] and row['connected'] is None for row in cold.values()))
        for _,job in list(root.management_jobs.values()):job.result(timeout=2)
        full=root.management_accounts()
        self.assertTrue(all(row['evidenceReady'] and 'managementView' not in row for row in full.values()))
        self.assertTrue(any(method=='management_snapshot' and args[-1] is True for method,args in seen))
        self.assertFalse(any(native.sent for native in f.natives))

    def test_quote_native_stages_are_measured_without_additional_reads(self):
        with patch.object(self.native,'account_info',wraps=self.native.account_info) as account,patch.object(self.native,'terminal_info',wraps=self.native.terminal_info) as terminal,\
             patch.object(self.native,'symbol_info_tick',wraps=self.native.symbol_info_tick) as tick:
            frame=self.bridge.quote_frame()
        self.assertEqual([account.call_count,terminal.call_count,tick.call_count],[1,1,2])
        self.assertTrue(all(frame['quoteTiming'][key]>=0 for key in ('nativeMs','accountInfoMs','terminalInfoMs')))
        self.assertEqual(frame['intervalMs'],100);self.assertEqual(self.native.sent,[])

    def test_quote_publisher_keeps_bounded_stage_counters_and_only_sends_slow_details(self):
        for native_ms,rpc_ms in ((1,2),(300,450)):
            publisher=QuotePublisher(self.bridge)
            with patch.object(self.bridge,'quote_frame',return_value=dict(account=900000002,server='Example-MT5',
                    snapshots={},capturedAt=time.time()*1000,intervalMs=100,
                    quoteTiming=dict(nativeMs=native_ms,rpcMs=rpc_ms,lockWaitMs=0,sendMs=0))):
                try:
                    sequence=publisher.subscribe();_,raw=publisher.next_frame(sequence);frame=json.loads(raw)
                    self.assertGreaterEqual(frame['publishedAt'],frame['capturedAt'])
                    self.assertEqual('quoteTiming' in frame,native_ms>=250)
                finally:publisher.close()
        operations=self.bridge.read_timings.snapshot()['operations']
        self.assertEqual(operations['quoteStage:nativeMs']['maxMs'],300)
        self.assertEqual(operations['quoteReplyOverhead']['maxMs'],150)
        self.assertEqual(self.native.sent,[])

    def test_http_view_is_explicit_validated_and_legacy_full_remains_available(self):
        case=fixtures.EntryCheckBatchTests();self.addCleanup(case.doCleanups)
        _,f,root=case.model();root.config['token']='fixture-only-token'
        server=create_server(root,('127.0.0.1',0));threading.Thread(target=server.serve_forever,daemon=True).start()
        self.addCleanup(server.server_close);self.addCleanup(server.shutdown)
        def get(route):
            client=http.client.HTTPConnection(*server.server_address,timeout=3)
            try:
                client.request('GET',route,headers={'Authorization':'Bearer fixture-only-token'})
                response=client.getresponse();return response.status,json.loads(response.read())
            finally:client.close()
        for route in ('/management?view=active','/management?view=full','/management'):
            status,value=get(route);self.assertEqual(status,200)
            self.assertEqual(str(value['account']),'900000002')
            self.assertIn('management-active-v1',value['capabilities'])
        for route in ('/management?view=bad','/management?view=','/management?view=active&view=full','/accounts/900000001/management?view=active'):
            self.assertEqual(get(route)[0],400)
        self.assertFalse(any(native.sent for native in f.natives))

    def test_quote_socket_buffer_retains_os_default_without_changing_any_socket_or_collecting_more_quotes(self):
        publisher=QuotePublisher(self.bridge)
        class Socket:
            calls=[]
            def setsockopt(self,*args):raise AssertionError('Native send-buffer policy must stay unchanged')
            def getsockopt(self,*args):self.calls.append(args);return 65536
        connection=Socket();publisher.configure_socket(connection)
        self.assertEqual(connection.calls,[(socket.SOL_SOCKET,socket.SO_SNDBUF)])
        state=publisher.snapshot();self.assertEqual(state['socketSendBufferBytes'],65536)
        self.assertEqual(state['socketBufferPolicy'],'os-default');self.assertIsNone(state['requestedSocketSendBufferBytes'])
        self.assertEqual(state['captures'],0);self.assertIsNone(publisher.thread)
        self.assertEqual(self.native.sent,[])

    def test_unsupported_quote_buffer_is_diagnosed_without_disconnect_or_native_reads(self):
        publisher=QuotePublisher(self.bridge)
        class Socket:
            def getsockopt(self,*args):raise OSError('unsupported option')
        publisher.configure_socket(Socket());self.assertEqual(publisher.snapshot()['socketBufferErrors'],1)
        self.assertFalse(publisher.failed);self.assertIsNone(publisher.thread)


if __name__=='__main__':unittest.main()

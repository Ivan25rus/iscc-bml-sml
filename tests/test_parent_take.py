"""Partial take persists without mutating native TP, with synthetic MT5 only."""
import copy
import http.client
import json
import pathlib
import sys
import threading
import time
import unittest
from unittest.mock import patch
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
import test_account_controls as controls
import test_entry_independence as independence
import test_isolated_entry_engines as isolated
from chunk_entry import AccountPlanEntries,IsolatedEntries
from mt5_bridge import create_server


class ParentTakeTests(unittest.TestCase):
    def model(self):
        case=controls.AccountControlTests();self.addCleanup(case.doCleanups)
        return case.model()

    def command(self,f,identifier='take-one',**values):
        return dict(id=identifier,entryId=f.payload['id'],symbol='USTEC',operation='arm',
            targetPrice=25010.,closePercent=50,expiresAt=time.time()*1000+3000,**values)

    def evaluate(self,f):
        f.engine.partial_takes.tick()
        for job in list(f.engine.partial_takes.jobs.values()):job.result(timeout=30)
        return f.engine.partial_takes.list()[0]

    def test_arm_leaves_native_sl_tp_unchanged_and_each_account_waits_for_own_quote(self):
        f,root=self.model();takes=f.engine.partial_takes
        levels=[[(p.ticket,p.sl,p.tp,p.volume) for p in native.positions] for native in f.natives]
        sent=[len(n.sent) for n in f.natives]
        response=takes.command(self.command(f));self.assertEqual(response['result']['status'],'ok',response)
        self.assertEqual([[(p.ticket,p.sl,p.tp,p.volume) for p in n.positions] for n in f.natives],levels)
        self.assertEqual([len(n.sent) for n in f.natives],sent)
        self.evaluate(f);self.assertEqual([len(n.sent) for n in f.natives],sent)
        f.natives[0].tick.bid=25011.;f.natives[0].tick.ask=25011.2
        rule=self.evaluate(f)
        self.assertEqual(rule['accounts']['900000002']['state'],'completed',rule)
        self.assertEqual(rule['accounts']['900000001']['state'],'armed',rule)
        self.assertEqual(sum(p.volume for p in f.natives[0].positions),.5)
        self.assertEqual(sum(p.volume for p in f.natives[1].positions),1)
        f.natives[1].tick.bid=25011.;f.natives[1].tick.ask=25011.2
        rule=self.evaluate(f);self.assertEqual(rule['accounts']['900000001']['state'],'completed',rule)
        for index,native in enumerate(f.natives):
            self.assertEqual(sum(p.volume for p in native.positions),.5)
            before={ticket:(sl,tp) for ticket,sl,tp,_ in levels[index]}
            self.assertTrue(all((p.sl,p.tp)==before[p.ticket] for p in native.positions))
        counts=[len(n.sent) for n in f.natives];self.evaluate(f)
        self.assertEqual([len(n.sent) for n in f.natives],counts)

    def test_take_keeps_the_account_that_never_entered_cancelled_without_read_or_close(self):
        case=independence.EntryIndependenceTests();self.addCleanup(case.doCleanups)
        f,root=case.model()
        follower='900000001';primary='900000002'
        legacy=dict(phase='running',accounts={follower:{}})
        direction_rows=AccountPlanEntries._direction_rows
        def block_follower(storage,payload,legacy_only=False):
            return iter([(payload,legacy)]) if legacy_only else direction_rows(storage,payload)
        with patch.object(AccountPlanEntries,'_direction_rows',side_effect=block_follower):
            f.engine.arm(f.payload)
        state=isolated.IsolatedEngineTests.until(self,f,lambda s:s['lanePhases'][primary]=='completed')
        self.assertEqual(state['lanePhases'][follower],'stopped')
        follower_calls=[]
        root.linked[follower].before=lambda method,args:follower_calls.append(method)
        takes=f.engine.partial_takes;command=self.command(f)
        sent=[len(n.sent) for n in f.natives]
        response=takes.command(command)
        accounts=response['result']['rule']['accounts']
        self.assertEqual(accounts[primary]['state'],'armed')
        self.assertEqual(accounts[follower],dict(state='cancelled',reason='Account was not admitted to this parent'))
        self.assertEqual(takes.command(command),response)
        self.assertEqual(root.receipt(command['id'])['result'],response['result'])
        self.assertEqual([len(n.sent) for n in f.natives],sent)
        for native in f.natives:native.tick.bid=25011.;native.tick.ask=25011.2
        rule=self.evaluate(f)
        self.assertEqual(rule['accounts'][primary]['state'],'completed',rule)
        self.assertEqual(rule['accounts'][follower],accounts[follower])
        self.assertEqual(sum(p.volume for p in f.natives[0].positions),.5)
        self.assertEqual(len(f.natives[1].sent),sent[1])
        self.assertEqual(follower_calls,[])

    def test_missing_lane_without_pre_admission_evidence_blocks_the_partial_take(self):
        f,_=self.model();takes=f.engine.partial_takes;command=self.command(f)
        sent=[len(n.sent) for n in f.natives]
        with patch.object(f.engine.shards['900000001'],'_load',return_value=None):
            with self.assertRaisesRegex(ValueError,'Account parent is not admitted: 900000001'):
                takes.command(command)
        self.assertEqual(takes.list(),[])
        self.assertIsNone(takes.receipt(command['id']))
        self.assertEqual([len(n.sent) for n in f.natives],sent)

    def test_cancel_and_idempotency_cannot_arm_twice(self):
        f,root=self.model();takes=f.engine.partial_takes;command=self.command(f)
        first=takes.command(command);self.assertEqual(takes.command(command),first)
        changed=dict(command,targetPrice=25011.)
        with self.assertRaisesRegex(ValueError,'reused'):takes.command(changed)
        with self.assertRaisesRegex(ValueError,'existing partial take'):takes.command(self.command(f,'duplicate'))
        cancel=dict(id='cancel-take',entryId=f.payload['id'],symbol='USTEC',operation='cancel',ruleId=command['id'],expiresAt=time.time()*1000+3000)
        self.assertEqual(takes.command(cancel)['result']['status'],'ok')
        for native in f.natives:native.tick.bid=25011.;native.tick.ask=25011.2
        sent=[len(n.sent) for n in f.natives];self.evaluate(f)
        self.assertEqual([len(n.sent) for n in f.natives],sent)
        self.assertEqual(root.receipt(cancel['id'])['result']['operation'],'cancel')

    def test_restart_retains_armed_take_but_a_claimed_unknown_never_replays(self):
        f,root=self.model();takes=f.engine.partial_takes;takes.command(self.command(f))
        self.assertTrue(takes._state('take-one','900000001','firing'))
        f.engine.close();f.engine=IsolatedEntries(root);root.chunk_entries=f.engine;self.addCleanup(f.engine.close)
        state=f.engine.partial_takes.list()[0]['accounts']
        self.assertEqual(state['900000002']['state'],'armed');self.assertEqual(state['900000001']['state'],'unknown')
        for native in f.natives:native.tick.bid=25011.;native.tick.ask=25011.2
        follower_sends=len(f.natives[1].sent);self.evaluate(f)
        self.assertEqual(len(f.natives[1].sent),follower_sends)

    def test_full_tp_must_follow_partial_take_and_already_reached_or_stale_quotes_cannot_arm(self):
        f,root=self.model();takes=f.engine.partial_takes
        f.engine.protection(dict(id='ordinary-tp',entryId=f.payload['id'],symbol='USTEC',account='900000002',
            mirrorAccounts=[dict(account='900000001')],operation='modify',tp=25009.,expiresAt=time.time()*1000+3000))
        sent=[len(n.sent) for n in f.natives]
        with self.assertRaisesRegex(ValueError,'precede the ordinary TP'):takes.command(self.command(f))
        command=self.command(f);command['targetPrice']=25001.
        with self.assertRaisesRegex(ValueError,'already reached'):takes.command(command)
        self.assertEqual([len(n.sent) for n in f.natives],sent)

    def test_unknown_close_is_retained_without_replay_and_healthy_peer_completes(self):
        f,root=self.model();takes=f.engine.partial_takes;takes.command(self.command(f));f.natives[1].close_unknown=True
        for native in f.natives:native.tick.bid=25011.;native.tick.ask=25011.2
        rule=self.evaluate(f)
        self.assertEqual(rule['accounts']['900000002']['state'],'completed',rule)
        self.assertEqual(rule['accounts']['900000001']['state'],'unknown',rule)
        sent=[len(n.sent) for n in f.natives];self.evaluate(f)
        self.assertEqual([len(n.sent) for n in f.natives],sent)

    def test_cancel_wins_before_claim_but_cannot_revoke_a_firing_account(self):
        f,root=self.model();takes=f.engine.partial_takes;takes.command(self.command(f))
        takes._state('take-one','900000002','firing')
        with self.assertRaisesRegex(ValueError,'unresolved'):
            takes.command(dict(id='cancel',entryId=f.payload['id'],symbol='USTEC',operation='cancel',ruleId='take-one',expiresAt=time.time()*1000+3000))

    def test_sell_take_uses_ask_and_slow_peer_cannot_hold_a_healthy_trigger(self):
        f=isolated.IsolatedEngineTests.make(self,'sell');f.engine.arm(f.payload)
        isolated.IsolatedEngineTests.until(self,f,lambda s:s['phase']=='completed')
        takes=f.engine.partial_takes;command=self.command(f);command['targetPrice']=24990.
        takes.command(command)
        for native in f.natives:native.tick.bid=24989.8;native.tick.ask=24990.2
        self.assertTrue(all(s['state']=='armed' for s in self.evaluate(f)['accounts'].values()))
        f.natives[0].tick.ask=24989.9
        entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        original=takes.entries.shards['900000001']._call
        def slow(account,method,*args):
            if method=='snapshot':entered.set();release.wait(3)
            return original(account,method,*args)
        with patch.object(takes.entries.shards['900000001'],'_call',side_effect=slow):
            takes.tick()
            try:
                self.assertTrue(entered.wait(1))
                end=time.monotonic()+3
                while time.monotonic()<end and takes.list()[0]['accounts']['900000002']['state']!='completed':time.sleep(.005)
                self.assertEqual(takes.list()[0]['accounts']['900000002']['state'],'completed')
                self.assertEqual(sum(p.volume for p in f.natives[1].positions),1)
            finally:release.set()
            for job in list(takes.jobs.values()):job.result(timeout=30)

    def test_authenticated_root_route_only_arms_and_receipt_remains_read_only(self):
        f,root=self.model();root.config['token']='parent-take-test'
        server=create_server(root,('127.0.0.1',0));thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        self.addCleanup(server.server_close);self.addCleanup(server.shutdown)
        def request(method,path,body=None,authenticated=True):
            connection=http.client.HTTPConnection('127.0.0.1',server.server_address[1],timeout=5)
            headers={'Authorization':'Bearer parent-take-test'} if authenticated else {}
            connection.request(method,path,json.dumps(body) if body else None,headers)
            response=connection.getresponse();value=json.loads(response.read());connection.close();return response.status,value
        counts=[len(n.sent) for n in f.natives]
        command=self.command(f)
        self.assertEqual(request('POST','/entry-parent-take',command,False)[0],401)
        self.assertEqual(request('POST','/accounts/900000001/entry-parent-take',command)[0],400)
        status,value=request('POST','/entry-parent-take',command)
        self.assertEqual((status,value['result']['status']),(200,'ok'),value)
        self.assertEqual(request('GET','/receipt?id='+command['id'])[1]['result'],value['result'])
        self.assertEqual([len(n.sent) for n in f.natives],counts)


if __name__=='__main__':unittest.main()

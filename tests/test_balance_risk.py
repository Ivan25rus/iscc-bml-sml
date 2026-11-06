"""Synthetic balance sizing examples; no terminals, network or owner settings."""
import copy
from pathlib import Path
import sys
import time
import unittest
from types import SimpleNamespace

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'payload/vps'))
import test_isolated_entry_engines as isolated
from test_hybrid_protection import ProtectionMT5, InProcessWorker
from chunk_entry import balance_risk_sizing, IsolatedEntries
from mt5_bridge import Bridge


class BalanceRiskTests(unittest.TestCase):
    def setUp(self):
        self.payload=dict(accountExecution='independent-v4',initialSizingOnly=True,
            executionPolicy='hybrid-market-limit-v1',stopMode='entry',riskPercent=.1,
            tickSize=.01,stopOffsetPts=1000,chunkPercent=20)
        self.account=SimpleNamespace(balance=50000,currency='USD')
        self.metadata=dict(tickSize=.01,contractSize=1,quantityStep=.1,minQty=.1,maxQty=100)

    def test_new_balance_and_stop_change_the_next_target(self):
        result=balance_risk_sizing(self.payload,self.account,self.metadata)
        self.assertEqual((result['riskUsd'],result['volume'],result['chunkVolume']),(50,5,1))
        self.account.balance=45000
        self.assertEqual(balance_risk_sizing(self.payload,self.account,self.metadata)['volume'],4.5)
        self.payload['stopOffsetPts']=2000
        result=balance_risk_sizing(self.payload,self.account,self.metadata)
        self.assertEqual((result['riskUsd'],result['volume']),(45,2.2))
        self.assertLessEqual(result['volume']*20,result['riskUsd'])

    def test_native_contract_value_step_and_cent_floor_are_used(self):
        result=balance_risk_sizing(self.payload,self.account,dict(self.metadata,contractSize=10,quantityStep=.01,minQty=.01,maxQty=.5))
        self.assertEqual((result['volume'],result['chunkVolume']),(.5,.1))
        self.account.balance=50009.99
        result=balance_risk_sizing(self.payload,self.account,self.metadata)
        self.assertEqual((result['riskUsd'],result['volume']),(50,5))

    def test_invalid_data_never_falls_back_to_scaled_lots(self):
        for balance in (0,-1,None,float('nan'),float('inf'),.01):
            with self.subTest(balance=balance),self.assertRaises(ValueError):
                balance_risk_sizing(self.payload,SimpleNamespace(balance=balance,currency='USD'),self.metadata)
        for change in (dict(riskPercent=0),dict(riskPercent=101),dict(stopMode='bad'),dict(initialSizingOnly=False)):
            with self.assertRaises(ValueError):
                balance_risk_sizing(dict(self.payload,**change),self.account,self.metadata)
        with self.assertRaises(ValueError):
            balance_risk_sizing(self.payload,SimpleNamespace(balance=50000,currency='EUR'),self.metadata)

    def fixture(self):
        helper=isolated.IsolatedEngineTests();self.addCleanup(helper.doCleanups)
        f=helper.make();primary=str(f.natives[0].account.login);follower=str(f.natives[1].account.login)
        f.payload.update(volume=.5,chunkVolume=.1,chunkPercent=20,riskUsd=13,stopOffsetPts=100,sl=24990.,
            mirrorAccounts=[dict(account=follower,multiplier=1,riskPercent=.1)])
        for native,target,budget in zip(f.natives,[.5,5],[13,50]):
            native.info.volume_step=native.info.volume_min=.1;native.account.balance=50000
            native.group_budgets['cid:protection-one']=(target,budget)
        return helper,f,primary,follower

    def test_admission_replay_and_restart_keep_the_captured_balance(self):
        helper,f,primary,follower=self.fixture();original=copy.deepcopy(f.payload)
        f.engine.arm(f.payload);state=helper.until(f,lambda row:row['phase']=='completed')
        self.assertEqual(f.payload,original)
        self.assertEqual(state['accounts'][primary]['targetVolume'],.5)
        self.assertEqual((state['accounts'][follower]['targetVolume'],state['accounts'][follower]['riskUsd']),(5,50))
        self.assertEqual(state['accounts'][follower]['balanceRisk']['balance'],50000)
        f.natives[1].account.balance=45000;sent=len(f.natives[1].sent)
        f.engine.arm(f.payload);self.assertEqual(len(f.natives[1].sent),sent)
        self.assertEqual(f.state()['accounts'][follower]['balanceRisk']['balance'],50000)
        f.engine.close();f.engine=IsolatedEntries(f.primary);f.primary.chunk_entries=f.engine;self.addCleanup(f.engine.close)
        self.assertEqual(f.state()['accounts'][follower]['riskUsd'],50)

    def test_mixed_modes_share_parent_protection_and_partial_close(self):
        helper,f,primary,follower=self.fixture();third_id=800000003;third=str(third_id)
        native=ProtectionMT5(f.now,third_id,'buy')
        bridge=Bridge(native,dict(account=third_id,server=f.bridges[0].config['server'],symbols=['USTEC'],tradingEnabled=True),f.paths[0].with_name('third.sqlite'))
        f.resources.append(bridge);f.natives.append(native);f.primary.linked[third]=InProcessWorker(f.primary,bridge)
        f.payload['mirrorAccounts']=[dict(account=follower,multiplier=1.6),dict(account=third,multiplier=1,riskPercent=.1)]
        for item,target,budget in zip(f.natives,[.5,.8,5],[13,20.8,50]):
            item.account.balance=50000;item.info.volume_step=item.info.volume_min=.1
            item.group_budgets['cid:protection-one']=(target,budget)
        f.engine.arm(f.payload);state=helper.until(f,lambda row:row['phase']=='completed')
        self.assertEqual([state['accounts'][key]['filledVolume'] for key in (primary,follower,third)],[.5,.8,5])
        result=f.engine.protection(f.command(sl=24995.,mirrorAccounts=[dict(account=follower),dict(account=third)]))
        self.assertEqual(result['result']['status'],'ok',result)
        self.assertTrue(all(position.sl==24995. for item in f.natives for position in item.positions))
        result=f.engine.close_position(dict(id='synthetic-half',entryId=f.payload['id'],symbol='USTEC',closePercent=50,expiresAt=time.time()*1000+3000))['result']
        self.assertEqual(result['status'],'ok',result)
        self.assertEqual([result['accountResults'][key]['filledVolume'] for key in (primary,follower,third)],[.2,.4,2.5])

    def test_percent_adds_no_native_account_reads(self):
        helper,f,_primary,_follower=self.fixture();native=f.natives[1];counts=[];read=native.account_info
        def counted():counts.append(1);return read()
        native.account_info=counted
        payload=dict(f.payload,accountExecution='independent-v4',initialSizingOnly=True);payload.pop('mirrorAccounts',None)
        f.bridges[1].entry_admission(payload);baseline=len(counts);counts.clear()
        result=f.bridges[1].entry_admission(dict(payload,riskPercent=.1))
        self.assertEqual(len(counts),baseline);self.assertEqual(result['balanceRiskSizing']['riskUsd'],50)
        self.assertFalse(native.sent)

    def test_fixed_level_stop_uses_adverse_zone_edge_for_buy_and_sell(self):
        for side,stop in [('buy',98),('sell',102)]:
            payload=dict(self.payload,stopMode='level',side=side,entryMode='side-zone',level=100,trigger=100,zoneRadius=1,sl=stop)
            result=balance_risk_sizing(payload,self.account,self.metadata)
            self.assertEqual((result['stopDistance'],result['volume']),(3,16.6))
            with self.assertRaises(ValueError):
                balance_risk_sizing(dict(payload,sl=102 if side=='buy' else 98),self.account,self.metadata)

    def test_level_sizing_keeps_primary_target_independent(self):
        for side in ('buy','sell'):
            helper=isolated.IsolatedEngineTests();self.addCleanup(helper.doCleanups);f=helper.make(side)
            primary=str(f.natives[0].account.login);follower=str(f.natives[1].account.login)
            f.payload.update(stopMode='level',riskUsd=22,volume=.5,chunkVolume=.1,chunkPercent=20,
                mirrorAccounts=[dict(account=follower,multiplier=1,riskPercent=.1)])
            for native,target,budget in zip(f.natives,[.5,2.2],[22,50]):
                native.account.balance=50000;native.info.volume_step=native.info.volume_min=.1
                native.group_budgets['cid:protection-one']=(target,budget)
            f.engine.arm(f.payload);state=helper.until(f,lambda row:row['accounts'][follower].get('balanceRisk') is not None)
            self.assertEqual(state['accounts'][follower]['balanceRisk']['stopDistance'],22)
            self.assertEqual(state['accounts'][follower]['targetVolume'],2.2)
            self.assertEqual(state['accounts'][primary]['targetVolume'],.5)


if __name__=='__main__':unittest.main()

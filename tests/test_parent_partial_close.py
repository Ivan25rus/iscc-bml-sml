"""Whole-parent percentages: synthetic native execution and exact remainders."""
import copy
import itertools
import pathlib
import sys
import time
import unittest

sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
import test_account_controls as controls
from chunk_entry import IsolatedEntries
from parent_close_plan import partial_close_plan


class ParentPartialCloseTests(unittest.TestCase):
    def model(self):
        case=controls.AccountControlTests();self.addCleanup(case.doCleanups)
        return case.model()

    def command(self,f,identifier='parent-partial',percent=50):
        return dict(id=identifier,entryId=f.payload['id'],symbol=f.payload['symbol'],closePercent=percent,
            expiresAt=time.time()*1000+3000)

    def test_all_linked_accounts_close_percentage_of_current_whole_parent(self):
        f,root=self.model()
        before=[sum(row.volume for row in native.positions) for native in f.natives]
        levels=[{row.ticket:(row.sl,row.tp) for row in native.positions} for native in f.natives]
        command=self.command(f)
        response=f.engine.close_position(command)
        self.assertEqual(response['result']['status'],'ok',response)
        for index,(account,native) in enumerate(zip(('900000002','900000001'),f.natives)):
            result=response['result']['accountResults'][account]
            self.assertAlmostEqual(result['filledVolume'],before[index]/2)
            self.assertAlmostEqual(result['remainingVolume'],before[index]/2)
            self.assertAlmostEqual(sum(row.volume for row in native.positions),before[index]/2)
            self.assertTrue(all((row.sl,row.tp)==levels[index][row.ticket] for row in native.positions))
            local=f.engine.shards[account]._load(f.payload['id'])[2]
            self.assertTrue(local['entryReduced']);self.assertEqual(local['phase'],'stopped')
            self.assertTrue(local['accounts'][account]['userStopped'])
        counts=[len(native.sent) for native in f.natives]
        self.assertEqual(f.engine.close_position(command)['result'],response['result'])
        for _ in range(3):f.engine.tick()
        self.assertEqual([len(native.sent) for native in f.natives],counts)
        f.engine.close();f.engine=IsolatedEntries(root);root.chunk_entries=f.engine
        self.addCleanup(f.engine.close)
        self.assertEqual(f.engine.close_position(command)['result'],response['result'])
        self.assertEqual([len(native.sent) for native in f.natives],counts)

    def test_second_user_percentage_uses_remainder_and_full_close_still_works(self):
        f,root=self.model()
        self.assertEqual(f.engine.close_position(self.command(f))['result']['status'],'ok')
        second=f.engine.close_position(self.command(f,'next-partial',50))
        self.assertEqual(second['result']['status'],'ok',second)
        for result in second['result']['accountResults'].values():self.assertEqual(result['remainingVolume'],.25)
        command=self.command(f,'full-exit');command.pop('closePercent')
        self.assertEqual(f.engine.close_position(command)['result']['status'],'ok')
        self.assertTrue(all(not native.positions for native in f.natives))

    def test_unknown_account_never_replays_and_healthy_account_finishes(self):
        f,root=self.model();f.natives[1].close_unknown=True
        command=self.command(f)
        result=f.engine.close_position(command)['result']
        self.assertEqual(result['status'],'unknown',result)
        self.assertEqual(result['accountResults']['900000002']['status'],'ok')
        self.assertEqual(result['accountResults']['900000001']['status'],'unknown')
        counts=[len(native.sent) for native in f.natives]
        self.assertEqual(f.engine.close_position(command)['result'],result)
        self.assertEqual([len(native.sent) for native in f.natives],counts)

    def test_invalid_percentage_cannot_close_anything(self):
        f,root=self.model();counts=[len(native.sent) for native in f.natives]
        for value in [0,100,101,-1,float('nan'),True,'50']:
            with self.subTest(value=value),self.assertRaises(ValueError):
                f.engine.close_position(self.command(f,percent=value))
        self.assertEqual([len(native.sent) for native in f.natives],counts)


class PartialPlanTests(unittest.TestCase):
    def rows(self,volumes):
        return [dict(ticket=str(i+1),status='open',volume=v) for i,v in enumerate(volumes)]

    def test_step_rounding_does_not_depend_on_ticket_partition(self):
        spec=dict(quantityStep=.1,minQty=.1,maxQty=100)
        for volumes in [[.5],[.3,.2],[.1,.1,.1,.1,.1]]:
            result=partial_close_plan(self.rows(volumes),spec,50)
            self.assertEqual(result['volume'],.2)
            self.assertEqual(result['remaining'],.3)
        self.assertEqual(partial_close_plan(self.rows([.3,.3,.2]),spec,50)['volume'],.4)

    def test_minimum_remainders_match_exhaustive_small_allocations(self):
        for minimum in [1,2,3]:
            spec=dict(quantityStep=1,minQty=minimum,maxQty=10)
            for sizes in itertools.product(range(minimum,6),repeat=3):
                choices=[[v for v in range(size+1) if v==0 or v>=minimum and (v==size or size-v>=minimum)] for size in sizes]
                possible={sum(parts) for parts in itertools.product(*choices)}
                for percent in [25,50,75]:
                    target=sum(sizes)*percent//100
                    if 0<target<sum(sizes) and target in possible:
                        plan=partial_close_plan(self.rows(sizes),spec,percent)
                        self.assertEqual(sum(plan['volumes'].values()),target)
                        for index,size in enumerate(sizes):self.assertIn(plan['volumes'][str(index+1)],choices[index])
                    else:
                        with self.assertRaises(ValueError):partial_close_plan(self.rows(sizes),spec,percent)

    def test_huge_units_do_not_require_volume_sized_memory(self):
        plan=partial_close_plan(self.rows([1_000_000.,1_000_000.]),dict(quantityStep=.00001,minQty=.00001,maxQty=1_000_000.),50)
        self.assertEqual(plan['volume'],1_000_000.)


if __name__=='__main__':unittest.main()

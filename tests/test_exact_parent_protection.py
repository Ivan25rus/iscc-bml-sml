import copy
import unittest
import test_asymmetric_parent_protection as asymmetric

class ExactParentProtectionTests(unittest.TestCase):
    def model(self):
        f,root=asymmetric.AsymmetricParentProtectionTests.model(self,'USTEC','buy','900000002')
        command=asymmetric.AsymmetricParentProtectionTests.command(self,f,'900000002','exact-target',tp=25060.)
        p=f.natives[0].positions[0]
        command.update(capturedAccounts=['900000002','900000001'],capturedSide='buy',
            capturedTarget=dict(account='900000002',ticket=str(p.ticket),volume=p.volume,fillPrice=p.price_open,sl=p.sl,tp=p.tp))
        return f,command

    def test_exact_target_protects_all_parts_and_replayed_intent_returns_original_receipt(self):
        f,c=self.model();result=f.engine.protection(c)['result'];self.assertEqual(result['status'],'ok',result)
        counts=[len(n.sent) for n in f.natives]
        self.assertEqual(f.engine.protection(c)['result'],result);self.assertEqual([len(n.sent) for n in f.natives],counts)

    def test_changed_selected_position_prevents_modification_on_either_account(self):
        for field,value in [('volume',.1),('fillPrice',1.),('sl',1.),('tp',1.)]:
            with self.subTest(field=field):
                f,c=self.model();c['capturedTarget'][field]=value;before=[len(n.sent) for n in f.natives]
                with self.assertRaises(ValueError):f.engine.protection(c)
                self.assertEqual([len(n.sent) for n in f.natives],before)

    def test_changed_side_account_set_and_foreign_ticket_fail_before_mutation(self):
        for key,value in [('capturedSide','sell'),('capturedAccounts',['900000002']),('capturedTarget',dict(account='900000002',ticket='9999999',volume=1.,fillPrice=25000.,sl=24984.,tp=25060.))]:
            with self.subTest(key=key):
                f,c=self.model();c[key]=value;before=[len(n.sent) for n in f.natives]
                with self.assertRaises(ValueError):f.engine.protection(c)
                self.assertEqual([len(n.sent) for n in f.natives],before)

if __name__=='__main__':unittest.main()

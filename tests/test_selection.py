import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT=Path(__file__).resolve().parent.parent
spec=importlib.util.spec_from_file_location('selected_prepare',ROOT/'prepare.py')
kit=importlib.util.module_from_spec(spec)
spec.loader.exec_module(kit)


class SelectionTests(unittest.TestCase):
    def test_all_choices_keep_the_same_dependency_contract_in_python_and_javascript(self):
        node=shutil.which('node')
        self.assertIsNotNone(node,'Node is required for the cross-language selection check')
        script="const {ids,resolve}=require('./selection');console.log(JSON.stringify(Array.from({length:255},(_,i)=>resolve(ids.filter((_,j)=>(i+1)&(1<<j))))));"
        actual=json.loads(subprocess.check_output([node,'-e',script],cwd=ROOT,text=True))
        expected=[kit.resolve([name for index,name in enumerate(kit.IDS) if mask&(1<<index)]) for mask in range(1,256)]
        self.assertEqual(expected,actual)

    def test_all_selected_frontends_have_a_complete_import_closure(self):
        manifest=kit.verify(ROOT)
        for mask in range(1,256):
            plan={'frontend':True,'features':kit.resolve([name for i,name in enumerate(kit.IDS) if mask&(1<<i)])['features']}
            files=kit.frontend_files(ROOT,manifest,plan)
            if not kit.resolve(plan['features'])['mt5Core']:
                self.assertEqual(files,set())
            elif 'bml-sml' in plan['features']:
                self.assertIn('payload/src/app/services/levelOrder/chunkAdmission.js',files)

    def test_profiles_stage_selected_sources_without_changing_the_recipient(self):
        with tempfile.TemporaryDirectory() as temporary:
            base=Path(temporary);root=base/'kit';shutil.copytree(ROOT,root,ignore=shutil.ignore_patterns('.git','local','node_modules','__pycache__'))
            project=base/'project';(project/'app').mkdir(parents=True)
            (project/'app/main.js').write_text('recipient main')
            (project/'app/renderer.js').write_text('recipient renderer')
            before={str(file.relative_to(project)):file.read_bytes() for file in project.rglob('*') if file.is_file()}
            for requested in [['levels-to-lt'],['management'],['bml-sml'],kit.IDS]:
                plan={'schema':2,'platform':'windows','frontend':True,'receiver':True,'tradingEnabled':False,'features':requested}
                result=kit.prepare(project,plan,root);work=Path(result['prepared'])
                report=json.loads((work/'review.json').read_text())
                self.assertTrue(report['preserveExistingHostFeatures']);self.assertFalse(report['removeExistingRules'])
                self.assertEqual(report['selection']['features'],kit.resolve(requested)['features'])
                self.assertEqual((work/'payload/vps').exists(),kit.resolve(requested)['mt5Core'])
                self.assertEqual((work/'components/level-renderer/install.py').exists(),'levels' in kit.resolve(requested)['features'])
                if requested==['levels-to-lt']:
                    self.assertFalse((work/'payload').exists())
                    self.assertTrue(json.loads((work/'levels-selection.json').read_text())['publishToLT'])
                if requested==['management']:
                    self.assertFalse((work/'payload/src/app/services/levelOrder/chunkAdmission.js').exists())
                    subprocess.run([shutil.which('node'),'-e',"const {createBmlSml}=require('./module');createBmlSml({features:['management'],getAdapter:()=>{throw Error('Unexpected adapter');}}).arm({}).then(r=>{if(r.status!=='rejected')process.exit(1);});"],cwd=work,check=True)
            self.assertEqual(before,{str(file.relative_to(project)):file.read_bytes() for file in project.rglob('*') if file.is_file()})

    def test_invalid_or_incomplete_plans_fail_without_staging(self):
        valid={'schema':2,'platform':'windows','frontend':True,'receiver':True,'features':['bml-sml'],'tradingEnabled':False}
        for change in [{'features':[]},{'features':['unknown']},{'tradingEnabled':True},{'features':['levels'],'frontend':False},{'schema':True}]:
            with self.assertRaises(ValueError):kit.selection({**valid,**change})

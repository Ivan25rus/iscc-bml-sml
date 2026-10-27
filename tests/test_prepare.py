import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('kit_prepare', Path(__file__).resolve().parent.parent/'prepare.py')
prepare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prepare)


class PreparationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        root = Path(self.temporary.name)
        self.kit, self.app = root/'kit', root/'project'
        self.kit.mkdir(); (self.app/'app/services/levelOrder').mkdir(parents=True)
        (self.app/'app/main.js').write_text('existing main\n')
        (self.app/'app/renderer.js').write_text('existing renderer\n')
        (self.app/'app/services/levelOrder/marketZone.js').write_text('recipient custom code\n')
        self.files = {'payload/src/app/services/levelOrder/marketZone.js':b'candidate\n',
                      'payload/vps/chunk_entry.py':b'# execution candidate\n',
                      'reference/renderer-functions.js.txt':b'merge reference\n'}
        for name,data in self.files.items():
            target=self.kit/name; target.parent.mkdir(parents=True,exist_ok=True); target.write_bytes(data)
        self.manifest={'schema':1,'kind':'iscc-bml-sml','version':'test',
            'files':{name:{'size':len(data),'sha256':hashlib.sha256(data).hexdigest()} for name,data in self.files.items()}}
        self.write_manifest()
        self.plan={'schema':1,'platform':'windows','frontend':True,'receiver':True,'tradingEnabled':False}

    def write_manifest(self):
        (self.kit/'manifest.json').write_text(json.dumps(self.manifest))

    def snapshot(self):
        return {str(file.relative_to(self.app)):file.read_bytes() for file in self.app.rglob('*') if file.is_file()}

    def test_preparation_stages_both_parts_and_preserves_every_application_byte(self):
        before=self.snapshot()
        result=prepare.prepare(self.app,self.plan,self.kit)
        report=json.loads((Path(result['prepared'])/'review.json').read_text())
        self.assertEqual(self.snapshot(),before)
        self.assertFalse(report['readyForAutomaticReplacement'])
        self.assertFalse(report['applicationModified'])
        self.assertEqual({row['status'] for row in report['files']},{'merge-required','new-dependency'})
        self.assertEqual(report['liveOrdersSent'],0)

    def test_component_choice_never_stages_the_unselected_execution_core(self):
        result=prepare.prepare(self.app,{**self.plan,'receiver':False},self.kit)
        self.assertFalse((Path(result['prepared'])/'payload/vps').exists())

    def test_modified_package_is_rejected_before_staging_or_touching_the_project(self):
        before=self.snapshot()
        (self.kit/next(iter(self.files))).write_bytes(b'changed')
        with self.assertRaises(ValueError):prepare.prepare(self.app,self.plan,self.kit)
        self.assertEqual(self.snapshot(),before)
        self.assertFalse((self.kit/'local').exists())

    def test_manifest_traversal_and_symlinked_payload_cannot_be_read_as_kit_files(self):
        self.manifest['files']['../outside']={'size':0,'sha256':hashlib.sha256(b'').hexdigest()}
        self.write_manifest()
        with self.assertRaises(ValueError):prepare.verify(self.kit)
        self.manifest['files'].pop('../outside');self.write_manifest()
        file=self.kit/next(iter(self.files));file.unlink();file.symlink_to(self.app/'app/main.js')
        with self.assertRaises(ValueError):prepare.verify(self.kit)

    def test_plan_cannot_enable_trading_or_smuggle_connection_details(self):
        for change in [{'tradingEnabled':True},{'server':'example.invalid'},{'frontend':False,'receiver':False}]:
            with self.assertRaises(ValueError):prepare.selection({**self.plan,**change})

    def test_unlisted_local_connection_file_is_rejected_before_preparation(self):
        (self.kit/'.env').write_text('unexpected local settings')
        with self.assertRaises(ValueError):prepare.verify(self.kit)


if __name__ == '__main__':
    unittest.main()

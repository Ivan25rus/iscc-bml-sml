import json
import pathlib
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import install

LISTENER = '''function initService(servicesApi) {
  // recipient custom logic stays here
  const tvProxy = servicesApi.tvProxy;
  if (tvProxy) {
    tvProxy.addListener((rec) => {
        Object.entries(rec.sources).forEach(([sourceId, src]) => {
          const sourceKey = sourceId != null && sourceId !== '' ? String(sourceId) : null;
          if (src) customFeature(src);
        });
    });
  }
}
'''
LT = '''function create() {
  function normalize(group) {
    return {
    key,
    enabled: group.enabled !== false,
    };
  }
  async function save(nextConfig = {}) { return nextConfig; }
  return {
    save,
    refreshAll,
  };
}
'''


def plan(**changes):
    value = dict(schema=2, platform='windows', adapterMode='auto', publishToLT=True)
    value.update(changes)
    return value


class InstallerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = pathlib.Path(self.temp.name)
        self.source, self.work = self.base / 'panel', self.base / 'review'
        for name, text in {'app/services/tvListener/manifest.js': LISTENER,
                           'app/services/levelTrack/index.js': LT,
                           'app/unrelated.js': 'custom recipient module\n',
                           'package.json': '{"name":"recipient-panel","dependencies":{}}\n'}.items():
            file = self.source / name
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text(text, encoding='utf-8')

    def snapshot(self):
        return {file.relative_to(self.source).as_posix(): file.read_bytes()
                for file in self.source.rglob('*') if file.is_file()}

    def test_stage_is_read_only_and_apply_preserves_custom_modules_without_backups(self):
        before = self.snapshot()
        result = install.stage(self.source, plan(), self.work)
        self.assertEqual(result['status'], 'ready-to-apply')
        self.assertFalse(result['backupCreated'])
        self.assertEqual(self.snapshot(), before)
        install.change(self.work)
        self.assertIn('recipient custom logic', (self.source / 'app/services/tvListener/manifest.js').read_text())
        self.assertEqual((self.source / 'app/unrelated.js').read_bytes(), before['app/unrelated.js'])
        config = json.loads((self.source / 'app/services/candleLevelRenderer/config/candle-renderer.json').read_text())
        self.assertFalse(config['enabled'])
        self.assertEqual(config['mappings'], [])
        self.assertEqual(config['adapterMode'], 'stdio')
        self.assertFalse((self.work / 'backup').exists())
        report = json.loads((self.work / 'review.json').read_text())
        self.assertFalse(report['backupCreated'])
        self.assertNotIn('existingVersion', report['selection'])
        self.assertNotIn('ownChanges', report['selection'])

    def test_changed_source_blocks_apply_before_any_write(self):
        install.stage(self.source, plan(), self.work)
        target = self.source / 'app/services/tvListener/manifest.js'
        target.write_text(LISTENER + '\n// another local edit\n')
        before = self.snapshot()
        with self.assertRaisesRegex(ValueError, 'source file changed'):
            install.change(self.work)
        self.assertEqual(self.snapshot(), before)

    def test_install_completes_without_questions_or_explicit_apply(self):
        with patch('builtins.input', side_effect=AssertionError('No installation questions')):
            result = install.install(self.source, plan(), self.work)
        self.assertEqual(result['status'], 'applied-to-source')
        self.assertFalse(result['backupCreated'])
        self.assertFalse((self.work / 'backup').exists())

    def test_plan_needs_only_components_and_rejects_invalid_component_choices(self):
        self.assertEqual(set(install.selection(plan())), {'schema', 'platform', 'adapterMode', 'publishToLT'})
        for field, value in [('schema', 1), ('platform', 'unknown'), ('adapterMode', 'unknown'), ('publishToLT', 'yes')]:
            selection = plan()
            selection[field] = value
            with self.assertRaises(ValueError):
                install.selection(selection)

    def test_incompatible_source_or_existing_renderer_is_not_replaced(self):
        file = self.source / 'app/services/tvListener/manifest.js'
        for text in ['recipient-only implementation', LISTENER + "\nrequire('./candleLevels');"]:
            file.write_text(text)
            before = self.snapshot()
            with self.assertRaises(ValueError):
                install.stage(self.source, plan(), self.work)
            self.assertFalse(self.work.exists())
            self.assertEqual(self.snapshot(), before)

    def test_optional_lt_skip_and_existing_dependencies_are_preserved(self):
        selection = plan()
        selection['publishToLT'] = False
        install.stage(self.source, selection, self.work)
        install.change(self.work)
        self.assertEqual((self.source / 'app/services/levelTrack/index.js').read_text(), LT)

    def test_existing_iscc_receiver_is_required_when_selected(self):
        selection = plan()
        selection['adapterMode'] = 'iscc'
        with self.assertRaisesRegex(ValueError, 'no compatible MT5'):
            install.stage(self.source, selection, self.work)
        self.assertFalse(self.work.exists())

    def test_auto_detects_existing_receiver_without_asking(self):
        (self.source / 'app/services/mt5-vps').mkdir()
        result = install.install(self.source, plan(), self.work)
        self.assertEqual(result['adapterMode'], 'iscc')
        config = json.loads((self.source / 'app/services/candleLevelRenderer/config/candle-renderer.json').read_text())
        self.assertEqual(config['adapterMode'], 'iscc')

    def test_write_failure_restores_files_in_memory_without_persistent_backups(self):
        before = self.snapshot()
        install.stage(self.source, plan(), self.work)
        real_atomic = install.atomic
        interrupted = False

        def fail_once(file, data):
            nonlocal interrupted
            if file.name == 'package.json' and not interrupted:
                interrupted = True
                raise OSError('simulated write failure')
            real_atomic(file, data)

        with patch.object(install, 'atomic', side_effect=fail_once):
            with self.assertRaises(OSError):
                install.change(self.work)
        self.assertEqual(self.snapshot(), before)
        self.assertFalse((self.work / 'backup').exists())

    def test_cli_installs_by_default_with_closed_stdin(self):
        selection_file = self.base / 'selection.json'
        selection_file.write_text(json.dumps(plan()))
        result = subprocess.run([sys.executable, str(ROOT / 'install.py'), '--source', str(self.source),
            '--plan', str(selection_file), '--work', str(self.work)], input='', text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)['status'], 'applied-to-source')
        self.assertFalse((self.work / 'backup').exists())

    def test_symlink_source_is_rejected(self):
        file = self.source / 'app/services/tvListener/manifest.js'
        saved = self.base / 'outside.js'
        file.rename(saved)
        file.symlink_to(saved)
        with self.assertRaises(ValueError):
            install.stage(self.source, plan(), self.work)

    def test_private_work_folder_cannot_be_inside_target(self):
        with self.assertRaises(ValueError):
            install.stage(self.source, plan(), self.source / 'local/review')

    def test_tampered_reviewed_overlay_blocks_all_writes(self):
        install.stage(self.source, plan(), self.work)
        (self.work / 'overlay/package.json').write_text('{}')
        before = self.snapshot()
        with self.assertRaisesRegex(ValueError, 'overlay integrity'):
            install.change(self.work)
        self.assertEqual(self.snapshot(), before)


if __name__ == '__main__':
    unittest.main()

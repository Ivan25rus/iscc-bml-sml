import ast
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'payload/vps'))


class PublicScopeTests(unittest.TestCase):
    def test_configuration_is_disabled_and_contains_no_connection_values(self):
        config = json.loads((ROOT/'config.example.json').read_text())
        self.assertFalse(config['enabled'])
        self.assertFalse(config['tradingEnabled'])
        self.assertEqual(config['followers'], [])
        self.assertEqual(config['mappings'], [])
        self.assertFalse(config['timeExit']['enabled'])

    def test_excluded_features_and_private_file_types_are_not_distributed(self):
        manifest = json.loads((ROOT/'manifest.json').read_text())
        for name in manifest['files']:
            self.assertFalse(set(Path(name).parts) & {'mobile', 'backups', 'local', '.git', 'personalEntryProgress', 'levelAlerts'})
            self.assertNotIn(Path(name).suffix, {'.pem', '.key', '.crt', '.sqlite3', '.log', '.pfx', '.p12'})
        source = (ROOT/'payload/vps/mt5_bridge.py').read_text()
        self.assertNotIn('import mobile_panel', source)
        self.assertNotIn('from lt_level_sync import', source)
        ast.parse(source)

    def test_personal_observer_cannot_start_threads_or_persist_statistics(self):
        from chunk_entry import EntryProgressObserver
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)/'unused.json'
            observer = EntryProgressObserver('900000001', 'Example-MT5', path, lambda:[])
            observer.start()
            observer.offer('900000001', [], 1)
            entry = {'id':'example'}
            self.assertIs(observer.decorate(entry), entry)
            self.assertIsNone(observer.snapshot())
            observer.close()
            self.assertFalse(path.exists())

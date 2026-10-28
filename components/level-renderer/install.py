"""Install the scoped addon after automatic compatibility checks, without backups."""
import argparse
import difflib
import hashlib
import json
import os
import pathlib
import tempfile

ROOT = pathlib.Path(__file__).resolve().parent
HOOK = 'ISCC_CANDLE_RENDERER_ADDON_V1'


def sha(data):
    return hashlib.sha256(data).hexdigest()


def encoded(value):
    return (json.dumps(value, ensure_ascii=False, indent=2) + '\n').encode()


def selection(value):
    required = {'schema', 'platform', 'adapterMode', 'publishToLT'}
    if not isinstance(value, dict) or set(value) != required or value['schema'] != 2:
        raise ValueError('Use a plan from SETUP.html')
    if value['platform'] not in ('windows', 'macos') or value['adapterMode'] not in ('auto', 'iscc', 'stdio'):
        raise ValueError('Choose a supported platform and receiver')
    if type(value['publishToLT']) is not bool:
        raise ValueError('Choose whether to publish to LT')
    return value


def inside(root, name):
    if pathlib.PurePosixPath(name).is_absolute() or '..' in pathlib.PurePosixPath(name).parts:
        raise ValueError('Invalid relative file name')
    target = root / name
    if not target.resolve().is_relative_to(root.resolve()):
        raise ValueError('A link leaves the selected project')
    current = target
    while current != root:
        if current.is_symlink():
            raise ValueError('Source symlinks need separate review')
        current = current.parent
    return target


def once(text, before, after):
    if text.count(before) != 1:
        raise ValueError('Source differs at an integration point; preserve it and adapt manually')
    return text.replace(before, after, 1)


def listener_patch(text):
    if HOOK in text or "require('./candleLevels')" in text or 'startCandleLevels(' in text:
        raise ValueError('This panel already has a candle renderer; compare versions instead of adding a second writer')
    anchor = '  const tvProxy = servicesApi.tvProxy;'
    text = once(text, anchor, anchor + '\n' + '''  // ISCC_CANDLE_RENDERER_ADDON_V1
  const candleRenderer = require('../candleLevelRenderer').attach({
    servicesApi, settings, root: require('../../config/load').USER_ROOT,
    getEnabled: () => cfg.enabled !== false && (typeof tvProxy?.getStatus !== 'function' || tvProxy.getStatus().enabled !== false)
  });
  require('electron').app?.on('before-quit', () => candleRenderer.stop());''')
    text = once(text, '    tvProxy.addListener((rec) => {', '''    tvProxy.addListener((rec) => {
      if (rec?.event === 'candle_bars') { candleRenderer.acceptBars(rec); return; }''')
    anchor = "          const sourceKey = sourceId != null && sourceId !== '' ? String(sourceId) : null;"
    text = once(text, anchor, anchor + "\n          if (candleRenderer.acceptSource(sourceKey, src, { local: rec.event === 'http_request' && rec.dir === 'out' })) return;")
    return text


def lt_patch(text):
    if "group.source === 'tv-mt5-candles'" not in text:
        text = once(text, '    key,\n    enabled: group.enabled !== false,',
                    "    key,\n    ...(group.source === 'tv-mt5-candles' ? { source: group.source } : {}),\n    enabled: group.enabled !== false,")
    if '  function update(transform)' in text:
        return text
    text = once(text, '  async function save(nextConfig = {}) {', '''  // Serialize saves and addon transforms so they read the latest manual edits.
  let candleRendererSaveQueue = Promise.resolve();
  function update(transform) {
    const next = candleRendererSaveQueue.then(() => commit(transform(snapshot().config)));
    candleRendererSaveQueue = next.catch(() => {});
    return next;
  }
  function save(nextConfig = {}) { return update(() => nextConfig); }
  async function commit(nextConfig = {}) {''')
    return once(text, '    save,\n    refreshAll,', '    save,\n    update,\n    refreshAll,')


def verify_package():
    manifest = json.loads((ROOT / 'manifest.json').read_text(encoding='utf-8'))
    for name, record in manifest['files'].items():
        file = inside(ROOT, name)
        if not file.is_file() or sha(file.read_bytes()) != record['sha256']:
            raise ValueError('Addon integrity check failed: ' + name)
    return manifest


def stage(source, plan, work):
    manifest = verify_package()
    plan = selection(plan)
    source, work = pathlib.Path(source).resolve(), pathlib.Path(work).absolute()
    if work.exists() or work.is_symlink():
        raise ValueError('Use a new private work folder')
    if work.resolve().is_relative_to(source):
        raise ValueError('Keep the private work folder outside the target project')
    app = 'app' if (source / 'app/services/tvListener/manifest.js').is_file() else 'src/app'
    listener_name = app + '/services/tvListener/manifest.js'
    listener = inside(source, listener_name)
    if not listener.is_file():
        raise ValueError('Select extracted ISCC source with app/ or src/app/. Packaged apps require separate extraction and rebuild')
    has_receiver = (source / app / 'services/mt5-vps').is_dir()
    adapter_mode = ('iscc' if has_receiver else 'stdio') if plan['adapterMode'] == 'auto' else plan['adapterMode']
    changes = {listener_name: listener_patch(listener.read_text(encoding='utf-8')).encode()}
    if plan['publishToLT']:
        lt_name = app + '/services/levelTrack/index.js'
        changes[lt_name] = lt_patch(inside(source, lt_name).read_text(encoding='utf-8')).encode()
    for name in sorted(manifest['files']):
        if not name.startswith('module/'):
            continue
        file = inside(ROOT, name)
        relative = file.relative_to(ROOT / 'module').as_posix()
        target = app + '/services/candleLevelRenderer/' + relative
        if inside(source, target).exists():
            raise ValueError('An existing addon file requires version comparison, not replacement')
        data = file.read_bytes()
        if relative == 'config/candle-renderer.json':
            config = json.loads(data)
            config.update(adapterMode=adapter_mode, publishToLT=plan['publishToLT'])
            data = encoded(config)
        changes[target] = data
    package = inside(source, 'package.json')
    if not package.is_file():
        raise ValueError('Select the complete source project with package.json')
    metadata = json.loads(package.read_text(encoding='utf-8'))
    if 'ws' not in metadata.get('dependencies', {}):
        metadata.setdefault('dependencies', {})['ws'] = '^8.22.0'
        changes['package.json'] = encoded(metadata)
    if adapter_mode == 'iscc' and not has_receiver:
        raise ValueError('This panel has no compatible MT5 receiver. Choose the candle-only stdio reader')
    records, diff = {}, []
    for name, after in sorted(changes.items()):
        file = inside(source, name)
        before = file.read_bytes() if file.exists() else None
        if before == after:
            continue
        records[name] = {'before': sha(before) if before is not None else None, 'after': sha(after)}
        diff.extend(difflib.unified_diff((before or b'').decode('utf-8').splitlines(keepends=True),
            after.decode('utf-8').splitlines(keepends=True), fromfile='a/' + name, tofile='b/' + name))
    work.mkdir(parents=True, mode=0o700)
    for name in records:
        target = inside(work, 'overlay/' + name)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(changes[name])
    (work / 'changes.patch').write_text(''.join(diff), encoding='utf-8')
    report = {'schema': 2, 'source': str(source), 'selection': plan, 'files': records,
        'detectedVersion': metadata.get('version', 'unknown'), 'adapterMode': adapter_mode,
        'status': 'ready-to-apply', 'backupCreated': False, 'targetMachineVerified': False,
        'notes': ['Compatibility checks passed; independent source changes are retained.',
                  'No source app, receiver, dependencies or terminal has been launched.',
                  'Install one matching indicator; fill your own settings locally; enable after verification.']}
    (work / 'review.json').write_bytes(encoded(report))
    return {'status': report['status'], 'changedFiles': len(records), 'adapterMode': adapter_mode,
            'backupCreated': False, 'privateReview': str(work / 'review.json')}


def atomic(file, data):
    file.parent.mkdir(parents=True, exist_ok=True)
    mode = file.stat().st_mode & 0o777 if file.exists() else 0o644
    descriptor, temporary = tempfile.mkstemp(prefix='.renderer-', dir=file.parent)
    try:
        with os.fdopen(descriptor, 'wb') as handle:
            handle.write(data)
        os.chmod(temporary, mode)
        os.replace(temporary, file)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def change(work):
    work = pathlib.Path(work).resolve()
    report = json.loads((work / 'review.json').read_text(encoding='utf-8'))
    if report.get('schema') != 2:
        raise ValueError('Prepare a new installation plan with this installer')
    source = pathlib.Path(report['source']).resolve()
    prepared = []
    for name, record in report['files'].items():
        target = inside(source, name)
        current = target.read_bytes() if target.is_file() else None
        expected = record['before']
        if (sha(current) if current is not None else None) != expected:
            raise ValueError('A source file changed; preserve it and prepare a new comparison: ' + name)
        desired = inside(work, 'overlay/' + name).read_bytes()
        if sha(desired) != record['after']:
            raise ValueError('Reviewed overlay integrity differs')
        prepared.append((target, current, desired))
    done = []
    try:
        for target, current, desired in prepared:
            atomic(target, desired)
            done.append((target, current))
    except BaseException:
        for target, current in reversed(done):
            target.unlink(missing_ok=True) if current is None else atomic(target, current)
        raise
    return {'status': 'applied-to-source', 'changedFiles': len(done), 'backupCreated': False,
            'adapterMode': report['adapterMode'], 'targetMachineVerified': False}


def install(source, plan, work):
    stage(source, plan, work)
    return change(work)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['install', 'stage', 'apply'], nargs='?', default='install')
    parser.add_argument('--source', type=pathlib.Path)
    parser.add_argument('--plan', type=pathlib.Path)
    parser.add_argument('--work', type=pathlib.Path)
    args = parser.parse_args()
    try:
        if args.action in ('install', 'stage'):
            if not args.source or not args.plan:
                parser.error('install and stage require --source and --plan')
            work = args.work
            if work is None:
                private = ROOT / 'local'
                private.mkdir(mode=0o700, exist_ok=True)
                session = pathlib.Path(tempfile.mkdtemp(prefix='install-', dir=private))
                work = session / 'prepared'
            action = install if args.action == 'install' else stage
            result = action(args.source, json.loads(args.plan.read_text(encoding='utf-8')), work)
        else:
            if not args.work:
                parser.error('apply requires --work')
            result = change(args.work)
    except (OSError, ValueError, KeyError) as error:
        parser.exit(1, str(error) + '\n')
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()

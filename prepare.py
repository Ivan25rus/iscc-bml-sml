"""Verify this kit and stage selected sources for a preserving agent merge."""
import argparse
import hashlib
import json
import pathlib
import tempfile

ROOT = pathlib.Path(__file__).resolve().parent


def verify(root=ROOT):
    root = pathlib.Path(root).resolve()
    manifest = json.loads((root / 'manifest.json').read_text())
    if manifest.get('schema') != 1 or manifest.get('kind') != 'iscc-bml-sml':
        raise ValueError('Unknown BML/SML manifest')
    allowed = set(manifest['files']) | {'manifest.json'}
    for file in root.rglob('*'):
        relative = file.relative_to(root)
        if set(relative.parts) & {'.git', 'local', 'node_modules', '__pycache__'}:
            continue
        if file.is_symlink() or file.is_file() and relative.as_posix() not in allowed:
            raise ValueError('Unlisted file or symlink in kit: ' + relative.as_posix())
    for name, expected in manifest['files'].items():
        relative = pathlib.PurePosixPath(name)
        if relative.is_absolute() or '..' in relative.parts:
            raise ValueError('Unsafe manifest path')
        file = root / name
        if file.is_symlink() or not file.resolve().is_relative_to(root):
            raise ValueError('Symlink or path outside the kit')
        if any(parent.is_symlink() for parent in file.parents if parent != root and parent.is_relative_to(root)):
            raise ValueError('Symlinked source directory')
        data = file.read_bytes()
        if len(data) != expected['size'] or hashlib.sha256(data).hexdigest() != expected['sha256']:
            raise ValueError('Kit file changed: ' + name)
    return manifest


def selection(value):
    if not isinstance(value, dict) or set(value) != {'schema','platform','frontend','receiver','tradingEnabled'}:
        raise ValueError('Unknown selection fields')
    if (value['schema'] != 1 or value['platform'] not in ('windows','macos')
            or type(value['frontend']) is not bool or type(value['receiver']) is not bool
            or value['tradingEnabled'] is not False or not (value['frontend'] or value['receiver'])):
        raise ValueError('Choose components; trading stays disabled during preparation')
    return value


def prepare(source, plan, root=ROOT):
    root = pathlib.Path(root).resolve()
    manifest, plan = verify(root), selection(plan)
    requested = pathlib.Path(source).absolute()
    if requested.is_symlink():
        raise ValueError('Select the real source directory')
    source = requested.resolve()
    candidates = [source / 'app', source / 'src/app']
    candidates = [path for path in candidates if (path / 'main.js').is_file() and (path / 'renderer.js').is_file()]
    if len(candidates) != 1:
        raise ValueError('Expected one source project with app/ or src/app/')
    app = candidates[0]
    if app.is_symlink() or not app.resolve().is_relative_to(source):
        raise ValueError('Application source must remain in its project')
    local = root / 'local'
    if local.is_symlink():
        raise ValueError('local/ must not be a symlink')
    local.mkdir(exist_ok=True)
    work = pathlib.Path(tempfile.mkdtemp(prefix='prepared-', dir=local))
    records = []
    for name, expected in manifest['files'].items():
        target = None
        if name.startswith('payload/src/app/') and plan['frontend']:
            target = app / name.removeprefix('payload/src/app/')
        elif name.startswith('payload/vps/') and plan['receiver']:
            target = source / name.removeprefix('payload/')
        if target is None:
            if name.startswith('reference/') and plan['frontend']:
                output = work / name
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_bytes((root/name).read_bytes())
            continue
        if target.is_symlink() or not target.resolve().is_relative_to(source):
            raise ValueError('Candidate target outside selected project')
        current = hashlib.sha256(target.read_bytes()).hexdigest() if target.is_file() else None
        state = 'unchanged' if current == expected['sha256'] else 'merge-required' if current else 'new-dependency'
        output = work / name
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes((root/name).read_bytes())
        records.append({'source':name, 'target':str(target), 'currentSHA256':current,
                        'candidateSHA256':expected['sha256'], 'status':state})
    report = {'schema':1, 'kitVersion':manifest['version'], 'selection':plan,
        'sourceProject':str(source), 'applicationSources':str(app),
        'readyForAutomaticReplacement':False, 'applicationModified':False,
        'receiverStarted':False, 'liveOrdersSent':0, 'files':records,
        'nextStep':'Merge the selected modules and reference hooks, test and build the recipient application.'}
    (work/'review.json').write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')
    return {'prepared':str(work), 'files':len(records), 'applicationModified':False,
            'recipientIntegrationRequired':True, 'liveOrdersSent':0}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=pathlib.Path)
    parser.add_argument('--plan', type=pathlib.Path)
    parser.add_argument('--verify', action='store_true')
    args = parser.parse_args()
    if args.verify:
        result = verify()
        print(json.dumps({'verified':True,'version':result['version'],'files':len(result['files'])}))
    elif args.source and args.plan:
        print(json.dumps(prepare(args.source, json.loads(args.plan.read_text())), ensure_ascii=False, indent=2))
    else:
        parser.error('Use --verify or --source and --plan')

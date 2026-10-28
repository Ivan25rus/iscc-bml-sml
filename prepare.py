"""Verify this kit and stage selected sources for a preserving agent merge."""
import argparse
import importlib.util
import hashlib
import json
import pathlib
import re
import tempfile

_spec=importlib.util.spec_from_file_location('iscc_feature_selection',pathlib.Path(__file__).with_name('selection.py'))
_selection=importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_selection)
resolve,IDS=_selection.resolve,_selection.IDS

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
    if not isinstance(value, dict):
        raise ValueError('Use a plan from SETUP.html')
    fields={'schema','platform','frontend','receiver','tradingEnabled'}
    schema=value.get('schema')
    if type(schema) is not int or schema not in (1,2) or set(value)!=(fields|({'features'} if schema==2 else set())):
        raise ValueError('Unknown selection fields')
    if (value['platform'] not in ('windows','macos')
            or type(value['frontend']) is not bool or type(value['receiver']) is not bool
            or value['tradingEnabled'] is not False or not (value['frontend'] or value['receiver'])):
        raise ValueError('Choose components; trading stays disabled during preparation')
    # Older plans keep the scope of 0.2.0; levels require a new explicit choice.
    selected=resolve(value['features'] if schema==2 else IDS[:6])
    if 'levels' in selected['features'] and not value['frontend']:
        raise ValueError('The levels addon needs frontend integration')
    return {**value,'schema':2,'features':selected['features']}


ROOTS = {
    'bml-sml':['levelOrder/chunkAdmission.js','mt5-vps/cardRecovery.js','mt5-vps/runtimeActivity.js','tradeRules/index.js'],
    'management':['mt5-vps/management.js'],
    'accounts':['mt5-vps/tradeSettings.js','mt5-vps/accountEnrollment.js'],
    'time-exit':['mt5-vps/tradeSettings.js']}
REQUIRE=re.compile(r'''require\(\s*['"](\.[^'"\r\n]+)['"]\s*\)''')


def frontend_files(root,manifest,plan):
    if not plan['frontend'] or not resolve(plan['features'])['mt5Core']:
        return set()
    prefix='payload/src/app/services/'
    pending=[prefix+name for key in plan['features'] for name in ROOTS.get(key,[])]
    pending += [prefix+'mt5-vps/client.js',prefix+'mt5-vps/buildInfo.js']
    chosen=set()
    while pending:
        name=pending.pop()
        if name in chosen: continue
        if name not in manifest['files']:
            raise ValueError('Missing selected dependency: '+name)
        chosen.add(name)
        for relative in REQUIRE.findall((root/name).read_text(encoding='utf-8')):
            base=(root/name).parent/relative
            candidates=(base,base.with_suffix('.js'),base/'index.js')
            dependency=next((item.resolve().relative_to(root).as_posix() for item in candidates
                if item.resolve().is_relative_to(root) and item.resolve().relative_to(root).as_posix() in manifest['files']),None)
            if dependency is None:
                if relative=='../../config/load': continue # Recipient's existing configuration service.
                raise ValueError('Missing selected import: '+name+' -> '+relative)
            pending.append(dependency)
    if 'bml-sml' in plan['features']:
        chosen.add(prefix+'levelOrder/config/level-order-settings-descriptor.json')
    return chosen


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
    # Resolve the complete dependency closure before creating any output.
    selected=resolve(plan['features'])
    selected_files=frontend_files(root,manifest,plan)
    local = root / 'local'
    if local.is_symlink():
        raise ValueError('local/ must not be a symlink')
    local.mkdir(exist_ok=True)
    work = pathlib.Path(tempfile.mkdtemp(prefix='prepared-', dir=local))
    records = []
    for name, expected in manifest['files'].items():
        target = None
        if name in selected_files:
            target = app / name.removeprefix('payload/src/app/')
        elif name.startswith('payload/vps/') and plan['receiver'] and selected['mt5Core']:
            target = source / name.removeprefix('payload/')
        if target is None:
            if (name.startswith('reference/') and plan['frontend'] and selected['mt5Core']
                    and ('bml-sml' in plan['features'] or name=='reference/mt5-service-manifest.js.txt')
                    or name.startswith('module/') and plan['frontend'] and selected['mt5Core']
                    or name in ('selection.js','features.json') and plan['frontend'] and selected['mt5Core']
                    or name.startswith('components/level-renderer/') and 'levels' in plan['features']):
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
    if 'levels' in plan['features']:
        (work/'levels-selection.json').write_text(json.dumps({'schema':2,'platform':plan['platform'],
            'adapterMode':'auto','publishToLT':'levels-to-lt' in plan['features']},indent=2)+'\n')
    (work/'feature-policy.json').write_text(json.dumps({'schema':1,'features':plan['features'],
        'enabled':False,'tradingEnabled':False},indent=2)+'\n')
    report = {'schema':2, 'kitVersion':manifest['version'], 'selection':plan,
        'mandatorySharedCore':selected['mt5Core'],
        'preserveExistingHostFeatures':True,'removeExistingRules':False,
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

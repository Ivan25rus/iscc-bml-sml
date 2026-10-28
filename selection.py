"""Validate install choices and keep each selected dependency bundle complete."""
import json
from pathlib import Path

CATALOG = json.loads((Path(__file__).resolve().parent/'features.json').read_text(encoding='utf-8'))
IDS = [row['id'] for row in CATALOG['features']]


def resolve(features):
    if (not isinstance(features, list) or len(features)>len(IDS)
            or any(not isinstance(item,str) for item in features) or len(set(features))!=len(features)):
        raise ValueError('Choose features without duplicates')
    rows = {row['id']:row for row in CATALOG['features']}
    chosen = set()
    def add(name):
        if name not in rows:
            raise ValueError('Unknown feature: '+name)
        for dependency in rows[name]['requires']:
            add(dependency)
        chosen.add(name)
    for name in features:
        add(name)
    if not chosen:
        raise ValueError('Choose at least one feature')
    return {'features':[name for name in IDS if name in chosen],
        'automaticallyIncluded':[name for name in IDS if name in chosen and name not in features],
        'mt5Core':any(rows[name]['mt5Core'] for name in chosen)}

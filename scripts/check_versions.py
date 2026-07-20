#!/usr/bin/env python3
# Copyright 2026 Pentaho
# Licensed under the Apache License, Version 2.0.
"""Assert the version strings agree across the repo (run in CI).

Umbrella/pdi2dag: ``pyproject.toml``, ``pdi2dag/__init__.py`` and the
``VERSION.md`` *Current version* must match. The provider carries its own
independent version (``pyproject.toml`` + ``airflow_pentaho/__init__.py``)
which must be internally consistent. The Studio webapp version
(``package.json``) is intentionally decoupled and is not checked here.
"""
from __future__ import annotations

import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent


def grab(rel, pattern):
    text = (ROOT / rel).read_text(encoding='utf-8')
    m = re.search(pattern, text)
    return m.group(1) if m else None


def check(group, values):
    ref = next(iter(values.values()))
    ok = True
    print('[{}] expected {}'.format(group, ref))
    for name, val in values.items():
        good = val == ref
        ok = ok and good
        print('  {:9} {} = {}'.format('OK' if good else 'MISMATCH', name, val))
    return ok


umbrella = {
    'pyproject.toml':
        grab('pyproject.toml', r'(?m)^version\s*=\s*"([^"]+)"'),
    'pdi2dag/__init__.py':
        grab('pdi2dag/__init__.py', r"__version__\s*=\s*'([^']+)'"),
    'VERSION.md (Current version)':
        grab('VERSION.md', r'Current version:\s*([0-9][^*\s]+)'),
}
provider = {
    'airflow-pentaho-provider/pyproject.toml':
        grab('airflow-pentaho-provider/pyproject.toml',
             r'(?m)^version\s*=\s*"([^"]+)"'),
    'airflow_pentaho/__init__.py':
        grab('airflow-pentaho-provider/airflow_pentaho/__init__.py',
             r"__version__\s*=\s*'([^']+)'"),
}

ok = check('umbrella / pdi2dag', umbrella)
ok = check('provider', provider) and ok

print('\n{}'.format('all versions consistent' if ok
                    else 'VERSION MISMATCH — update the files above'))
sys.exit(0 if ok else 1)

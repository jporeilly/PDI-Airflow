#!/usr/bin/env python
"""Run a transformation on Carte and report what it actually did.

Carte's ``executeTrans`` does not reliably hand back the run id, and a
transformation name is NOT unique - re-running one leaves several entries
with the same ``transname`` in ``/kettle/status/``, in no guaranteed
order. Picking "the last one" silently reports on a previous run.

So identify the run by *difference*: snapshot the ids Carte knows for
this name, execute, then take the id that appeared. That is unambiguous
even with concurrent runs of the same transformation.

Also surfaces the failure mode that bit ingest_from_minio: a step can
finish with zero errors having read nothing at all (an unresolvable
s3:// path with "file required" off matches no files). Carte calls that
Finished; this script calls it out and exits non-zero unless you pass
--allow-empty.

    python scripts/carte_run.py --trans /CSCU/txn_report
"""
import argparse
import os
import sys
import time
import xml.etree.ElementTree as ET

import requests
from requests.auth import HTTPBasicAuth

STATUS = '/kettle/status/'
EXECUTE = '/kettle/executeTrans/'
TRANS_STATUS = '/kettle/transStatus/'

# Carte reports these while the run is still in flight.
BUSY = {'Running', 'Waiting', 'Initializing', 'Preparing Execution'}


def _get(base, endpoint, auth, **params):
    params['xml'] = 'Y'
    rs = requests.get(base + endpoint, params=params, auth=auth, timeout=60)
    rs.raise_for_status()
    return ET.fromstring(rs.content)


def _run_ids(base, auth, name):
    """Ids Carte currently knows for this transformation name."""
    root = _get(base, STATUS, auth)
    return {t.findtext('id') for t in root.iter('transstatus')
            if t.findtext('transname') == name}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--trans', required=True,
                    help="repository path, e.g. /CSCU/txn_report")
    ap.add_argument('--rep', default='Default')
    ap.add_argument('--base', default='http://localhost:8081')
    ap.add_argument('--level', default='Detailed')
    ap.add_argument('--timeout', type=int, default=300,
                    help='seconds to wait for completion')
    ap.add_argument('--allow-empty', action='store_true',
                    help='do not fail when the run read zero rows')
    args = ap.parse_args(argv)

    # Carte creds: env first so they stay off the command line.
    auth = HTTPBasicAuth(os.environ.get('CARTE_USER', 'cluster'),
                         os.environ.get('CARTE_PASSWORD', 'cluster'))
    base = args.base.rstrip('/')
    name = args.trans.rstrip('/').split('/')[-1]

    before = _run_ids(base, auth, name)
    print('running {} on {}'.format(args.trans, base))

    rs = requests.get(base + EXECUTE, auth=auth, timeout=args.timeout,
                      params={'rep': args.rep, 'trans': args.trans,
                              'level': args.level})
    if rs.status_code >= 400:
        print(rs.text[:2000], file=sys.stderr)
        return 2

    # The new id is whatever appeared since the snapshot. Carte registers
    # the run a moment after accepting the request, so give it a beat.
    run_id = None
    for _ in range(20):
        new = _run_ids(base, auth, name) - before
        if new:
            run_id = sorted(new)[0]
            break
        time.sleep(0.5)
    if not run_id:
        print('could not identify the run - Carte registered no new id for '
              '{!r}. Check the repository path.'.format(name), file=sys.stderr)
        return 2
    print('run id: {}'.format(run_id))

    deadline = time.time() + args.timeout
    root = None
    while time.time() < deadline:
        root = _get(base, TRANS_STATUS, auth, name=name, id=run_id)
        if (root.findtext('status_desc') or '') not in BUSY:
            break
        time.sleep(1)

    status = root.findtext('status_desc')
    err = (root.findtext('error_desc') or '').strip()
    print('status: {}{}'.format(status, '  | error: ' + err if err else ''))

    total_in = 0
    failed = 0
    for s in root.iter('stepstatus'):
        vals = {k: int(s.findtext(k) or 0) for k in
                ('linesRead', 'linesWritten', 'linesInput',
                 'linesOutput', 'errors')}
        total_in += vals['linesInput'] + vals['linesRead']
        failed += vals['errors']
        print('  {:<28} read={:<6} written={:<6} input={:<6} output={:<6} '
              'errors={}'.format(s.findtext('stepname'), vals['linesRead'],
                                 vals['linesWritten'], vals['linesInput'],
                                 vals['linesOutput'], vals['errors']))

    if status != 'Finished' or err or failed:
        return 1
    if total_in == 0 and not args.allow_empty:
        print('\nFinished but moved ZERO rows. Carte treats that as success; '
              'usually it means an input path resolved to nothing (check the '
              'VFS/metastore connection, and set "file required" = Y).',
              file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())

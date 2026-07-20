# -*- coding: utf-8 -*-
# Copyright 2026 Pentaho
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""pdi2dag command line interface.

Examples::

    # Inspect what a PDI file contains
    pdi2dag inspect nightly_etl.kjb

    # Convert a job to a DAG file (one task per TRANS/JOB entry)
    pdi2dag convert nightly_etl.kjb --schedule "0 6 * * *" -o dags/

    # Convert and hand over to a running Airflow with a schedule
    pdi2dag migrate nightly_etl.kjb --schedule "0 6 * * *" \
        --dags-folder /path/to/dags \
        --airflow-url http://localhost:8088 \
        --airflow-user admin --airflow-password admin --trigger
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys

from pdi2dag import __version__
from pdi2dag.generator import ConvertOptions, convert
from pdi2dag.parser import parse_file


def _parse_params(raw_params):
    params = {}
    for raw in raw_params or []:
        if '=' not in raw:
            raise SystemExit(
                "--param must be KEY=VALUE, got '{}'".format(raw))
        key, val = raw.split('=', 1)
        params[key] = val
    return params


def _add_convert_args(parser):
    parser.add_argument('file', help='.kjb or .ktr file to convert')
    parser.add_argument('--schedule', default=None,
                        help='Cron schedule, e.g. "0 6 * * *". '
                             'Omit for manual-only DAGs.')
    parser.add_argument('--dag-id', default=None,
                        help='DAG id (default: PDI job/trans name)')
    parser.add_argument('--conn-id', default='pdi_default',
                        help='Airflow connection id (default pdi_default)')
    parser.add_argument('--mode', choices=['auto', 'wrap', 'explode'],
                        default='auto',
                        help='wrap: one task runs the whole job. explode: '
                             'one task per TRANS/JOB entry (jobs only). '
                             'auto (default): explode jobs, wrap trans.')
    parser.add_argument('--deferrable', action='store_true',
                        help='Generate deferrable Carte operators')
    parser.add_argument('--poll-interval', type=int, default=10,
                        help='Carte status poll interval in seconds')
    parser.add_argument('--param', action='append', metavar='KEY=VALUE',
                        help='Override/add a PDI parameter (repeatable). '
                             'Airflow macros allowed, e.g. '
                             'date="{{ ds }}"')
    parser.add_argument('--retries', type=int, default=0)
    parser.add_argument('--owner', default='pdi2dag')
    parser.add_argument('--start-date', default=None,
                        help='DAG start date YYYY-MM-DD (default: today)')
    parser.add_argument('--level', default='Basic',
                        help='PDI logging level (default Basic)')


def _options_from_args(args):
    return ConvertOptions(
        schedule=args.schedule,
        dag_id=args.dag_id,
        conn_id=args.conn_id,
        mode=args.mode,
        deferrable=args.deferrable,
        poll_interval=args.poll_interval,
        params=_parse_params(args.param),
        retries=args.retries,
        owner=args.owner,
        start_date=args.start_date,
        level=args.level)


def _write_dag(result, out):
    if out and os.path.isdir(out):
        out_file = os.path.join(out, result.dag_id + '.py')
    elif out:
        out_file = out
    else:
        out_file = result.dag_id + '.py'
    with open(out_file, 'w', encoding='utf-8') as f:
        f.write(result.code)
    return out_file


def _print_warnings(result):
    for warning in result.warnings:
        print('  WARNING: {}'.format(warning))


def cmd_inspect(args):
    doc = parse_file(args.file)
    print('Kind:        {}'.format(doc.kind))
    print('Name:        {}'.format(doc.name))
    print('Repo path:   {}'.format(doc.repo_path))
    if doc.description:
        print('Description: {}'.format(doc.description))
    if doc.parameters:
        print('Parameters:')
        for p in doc.parameters:
            print('  - {} (default: {!r})'.format(p.name, p.default))
    if doc.kind == 'job':
        print('Entries:')
        for e in doc.entries:
            marker = ' [START]' if e.is_start else ''
            path = ' -> {}'.format(e.path) if e.path else ''
            print('  - [{}] {}{}{}'.format(
                e.entry_type, e.name, path, marker))
        print('Hops:')
        for h in doc.hops:
            kind = 'success' if h.evaluation else 'failure'
            if h.unconditional:
                kind = 'unconditional'
            state = '' if h.enabled else ' (disabled)'
            print('  - {} -> {} [{}]{}'.format(
                h.from_name, h.to_name, kind, state))
    return 0


def cmd_convert(args):
    doc = parse_file(args.file)
    result = convert(doc, _options_from_args(args))
    out_file = _write_dag(result, args.output)
    print('Generated {} (dag_id: {})'.format(out_file, result.dag_id))
    _print_warnings(result)
    return 0


def cmd_migrate(args):
    doc = parse_file(args.file)
    result = convert(doc, _options_from_args(args))

    dag_file = _write_dag(result, args.dags_folder)
    print('Deployed {} (dag_id: {})'.format(dag_file, result.dag_id))
    _print_warnings(result)

    if args.airflow_url:
        from pdi2dag.airflow_api import AirflowClient
        client = AirflowClient(args.airflow_url, args.airflow_user,
                               args.airflow_password)
        print('Waiting for Airflow to parse the DAG...')
        client.wait_for_dag(result.dag_id)
        client.set_paused(result.dag_id, False)
        print("DAG '{}' is active with schedule {!r}.".format(
            result.dag_id, args.schedule))
        if args.trigger:
            run = client.trigger_dag_run(result.dag_id)
            print('Triggered run: {}'.format(
                run.get('dag_run_id', '<unknown>')))
    return 0


def _load_carte_metrics(path, trans_details):
    """Load Carte transStatus XML into {trans_name: step_metrics}.

    ``path`` may be a single .xml file (matched to the sole/first
    transformation) or a directory of ``<transname>.xml`` files.
    """
    if not path:
        return {}
    import xmltodict
    from pdi2dag.lineage import parse_carte_step_metrics
    out = {}
    if os.path.isdir(path):
        for tname in trans_details:
            f = os.path.join(path, tname + '.xml')
            if os.path.exists(f):
                with open(f, encoding='utf-8') as fh:
                    out[tname] = parse_carte_step_metrics(
                        xmltodict.parse(fh.read()))
    elif os.path.exists(path):
        with open(path, encoding='utf-8') as fh:
            metrics = parse_carte_step_metrics(xmltodict.parse(fh.read()))
        # attach to the single/first transformation
        name = next(iter(trans_details), None)
        if name:
            out[name] = metrics
    return out


def cmd_lineage(args):
    from pdi2dag.lineage import (build_job_model_events,
                                 build_trans_model_events, emit)
    from pdi2dag.parser import parse_trans_detail

    doc = parse_file(args.file)
    events = []
    trans_details = {}
    if doc.kind == 'job':
        if args.ktr_dir:
            for entry in doc.executable_entries:
                if entry.entry_type != 'TRANS' or not entry.path:
                    continue
                trans_name = entry.path.split('/')[-1]
                ktr = os.path.join(args.ktr_dir, trans_name + '.ktr')
                if not os.path.exists(ktr):
                    print('  WARNING: no .ktr found for {} ({}) - '
                          'steps skipped.'.format(entry.name, ktr))
                    continue
                detail = parse_trans_detail(ktr)
                trans_details[trans_name] = detail
                print('  {}: {} steps spliced into the job '
                      'graph.'.format(detail.name, len(detail.steps)))
        events += build_job_model_events(
            doc, namespace=args.namespace, trans_details=trans_details)
        print('Job {}: {} entries mapped.'.format(
            doc.name, len(doc.executable_entries)))
    else:
        ktr_path = args.file
        detail = parse_trans_detail(ktr_path)
        trans_details[detail.name] = detail
        events += build_trans_model_events(detail,
                                           namespace=args.namespace)
        print('Transformation {}: {} steps mapped.'.format(
            detail.name, len(detail.steps)))

    if args.pdc_url or args.out_file:
        # PDC ingestion (endpoint or file import) wants plugin-shaped
        # events: repo-path names + ParentRunFacet, plus table datasets
        # from Table Input/Output steps. Optional Carte transStatus adds
        # real row counts and run state.
        from pdi2dag.lineage import (build_pdc_etl_events,
                                     build_pdc_trans_events)
        step_metrics = _load_carte_metrics(args.carte_status, trans_details)
        if step_metrics:
            print('  enriched with Carte run metrics for: {}'.format(
                ', '.join(step_metrics)))
        if doc.kind == 'job':
            events = build_pdc_etl_events(
                doc, trans_details=trans_details,
                server_name=args.pdi_server, step_metrics=step_metrics)
        else:
            events = build_pdc_trans_events(
                trans_details[doc.name], doc.repo_path,
                server_name=args.pdi_server,
                step_metrics=step_metrics.get(doc.name))

    if args.out_file:
        import json as _json
        with open(args.out_file, 'w', encoding='utf-8') as fh:
            for ev in events:
                fh.write(_json.dumps(ev) + '\n')
        print('{} OpenLineage events written to {} (newline-delimited '
              'JSON - the PDI plugin file-consumer format).'.format(
                  len(events), args.out_file))
        print('Import in PDC: Data Catalog -> ETL -> Actions -> Import.')
        return 0

    if args.dry_run:
        print('{} OpenLineage events built (dry run, not sent).'.format(
            len(events)))
        return 0

    if args.pdc_url:
        from pdi2dag.lineage import emit_pdc
        sent = emit_pdc(events, args.pdc_url, args.pdc_user,
                        args.pdc_password)
        print('{} OpenLineage events sent to PDC at {} '
              '(lineage/api/events).'.format(sent, args.pdc_url))
        print('View under Data Catalog -> ETL Pipelines / Lineage.')
    else:
        sent = emit(events, args.marquez_url)
        print('{} OpenLineage events sent to {} (namespace {}).'.format(
            sent, args.marquez_url, args.namespace))
        print('View the graph in the Marquez UI (pick the namespace, '
              'then any job, then the graph tab).')
    return 0


def cmd_provision(args):
    """Pre-create PDC data connections from a job/transformation's
    PDI connection definitions (identity + username, no password)."""
    from pdi2dag.lineage import provision_connections
    from pdi2dag.parser import parse_trans_detail

    details = []
    doc = parse_file(args.file)
    if doc.kind == 'transformation':
        details.append(parse_trans_detail(args.file))
    elif args.ktr_dir:
        for entry in doc.executable_entries:
            if entry.entry_type == 'TRANS' and entry.path:
                ktr = os.path.join(args.ktr_dir,
                                   entry.path.split('/')[-1] + '.ktr')
                if os.path.exists(ktr):
                    details.append(parse_trans_detail(ktr))
    if not details:
        print('No transformations with connections found. For a job, '
              'pass --ktr-dir with its .ktr files.')
        return 1

    results = provision_connections(
        details, args.pdc_url, args.pdc_user, args.pdc_password,
        dry_run=args.dry_run)
    for name, status in results:
        print('  {}: {}'.format(name, status))
    if not args.dry_run:
        print('\nSet each connection\'s password in PDC (Data Sources -> '
              'Edit -> credentials) to enable scanning. Passwords are '
              'never handled by pdi2dag.')
    return 0


def cmd_deploy(args):
    dest = os.path.join(args.dags_folder,
                        os.path.basename(args.dag_file))
    shutil.copyfile(args.dag_file, dest)
    print('Copied {} -> {}'.format(args.dag_file, dest))
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog='pdi2dag',
        description='Migrate PDI jobs/transformations to Airflow DAGs.')
    parser.add_argument('--version', action='version',
                        version='pdi2dag {}'.format(__version__))
    sub = parser.add_subparsers(dest='command', required=True)

    p_inspect = sub.add_parser(
        'inspect', help='Show the orchestration structure of a PDI file')
    p_inspect.add_argument('file')
    p_inspect.set_defaults(func=cmd_inspect)

    p_convert = sub.add_parser(
        'convert', help='Convert a PDI file to an Airflow DAG file')
    _add_convert_args(p_convert)
    p_convert.add_argument('-o', '--output', default=None,
                           help='Output file or directory '
                                '(default: ./<dag_id>.py)')
    p_convert.set_defaults(func=cmd_convert)

    p_migrate = sub.add_parser(
        'migrate',
        help='Convert, deploy to a dags folder and activate in Airflow')
    _add_convert_args(p_migrate)
    p_migrate.add_argument('--dags-folder', required=True,
                           help='Airflow dags folder to deploy into')
    p_migrate.add_argument('--airflow-url', default=None,
                           help='Airflow webserver URL; if set, the DAG '
                                'is unpaused after deploy')
    p_migrate.add_argument('--airflow-user', default='admin')
    p_migrate.add_argument('--airflow-password', default='admin')
    p_migrate.add_argument('--trigger', action='store_true',
                           help='Trigger a run after activation')
    p_migrate.set_defaults(func=cmd_migrate)

    p_lineage = sub.add_parser(
        'lineage',
        help='Publish the PDI structure (job entries, transformation '
             'steps) to Marquez as OpenLineage jobs')
    p_lineage.add_argument('file', help='.kjb or .ktr file')
    p_lineage.add_argument('--ktr-dir', default=None,
                           help='Folder holding the .ktr files of the '
                                "job's TRANS entries - enables "
                                'step-level lineage')
    p_lineage.add_argument('--marquez-url',
                           default='http://localhost:6001',
                           help='Marquez API base URL '
                                '(default http://localhost:6001)')
    p_lineage.add_argument('--namespace', default='pdi',
                           help='OpenLineage namespace for Marquez '
                                '(default pdi)')
    p_lineage.add_argument('--pdi-server', default='pdi2dag',
                           help='PDI Server node name for PDC ETL '
                                '(the OpenLineage namespace the plugin '
                                'derives from the host; default pdi2dag)')
    p_lineage.add_argument('--dry-run', action='store_true',
                           help='Build events but do not send them')
    p_lineage.add_argument('--carte-status', default=None,
                           help='Carte transStatus XML file (single '
                                'transformation) or a directory of '
                                '<transname>.xml files (a job) - adds '
                                'real row counts and run state from the '
                                'actual Carte execution')
    p_lineage.add_argument('--out-file', default=None,
                           help='Write events to a newline-delimited '
                                'JSON file (the PDI OpenLineage plugin '
                                'file-consumer format) for PDC Import')
    p_lineage.add_argument('--pdc-url', default=None,
                           help='Send to Pentaho Data Catalog instead of '
                                'Marquez (e.g. https://pentaho.io) - uses '
                                'the /lineage/api/events ingestion '
                                'endpoint')
    p_lineage.add_argument('--pdc-user', default=None,
                           help='PDC username (Keycloak)')
    p_lineage.add_argument('--pdc-password', default=None,
                           help='PDC password')
    p_lineage.set_defaults(func=cmd_lineage)

    p_provision = sub.add_parser(
        'provision',
        help='Pre-create PDC data connections from a PDI file\'s '
             'connection definitions (so lineage attaches to real '
             'connections; no passwords are handled)')
    p_provision.add_argument('file', help='.kjb or .ktr file')
    p_provision.add_argument('--ktr-dir', default=None,
                             help='Folder with the job\'s .ktr files')
    p_provision.add_argument('--pdc-url', default='https://pentaho.io')
    p_provision.add_argument('--pdc-user', required=True)
    p_provision.add_argument('--pdc-password', required=True,
                             help='PDC (Keycloak) password for API auth '
                                  '- NOT a database password')
    p_provision.add_argument('--dry-run', action='store_true',
                             help='Show what would be created')
    p_provision.set_defaults(func=cmd_provision)

    p_deploy = sub.add_parser(
        'deploy', help='Copy an existing DAG file into a dags folder')
    p_deploy.add_argument('dag_file')
    p_deploy.add_argument('--dags-folder', required=True)
    p_deploy.set_defaults(func=cmd_deploy)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == '__main__':
    sys.exit(main())

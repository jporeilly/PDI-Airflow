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
"""Generate Airflow DAG source code from a parsed PDI document.

Two conversion modes:

- ``wrap``: one Carte task runs the whole job/transformation as-is.
  Safest - PDI keeps all internal control flow.
- ``explode`` (jobs only): every TRANS/JOB entry becomes its own
  Airflow task and PDI hops become task dependencies, so Airflow gains
  per-entry retries, logs and parallelism.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional

from pdi2dag import __version__
from pdi2dag.parser import TYPE_JOB, TYPE_TRANS, PdiDocument


@dataclass
class ConvertOptions:
    schedule: Optional[str] = None      # cron string, None = manual only
    dag_id: Optional[str] = None
    conn_id: str = 'pdi_default'
    mode: str = 'auto'                  # auto | wrap | explode
    deferrable: bool = False
    poll_interval: int = 10
    params: Dict[str, str] = field(default_factory=dict)
    owner: str = 'pdi2dag'
    tags: List[str] = field(default_factory=lambda: ['pdi', 'pdi2dag'])
    retries: int = 0
    start_date: Optional[str] = None    # YYYY-MM-DD, default: today
    level: str = 'Basic'
    # HOW the PDI work executes. 'carte' submits to the Carte server the
    # connection points at (a cluster master is just a connection - e.g.
    # `--conn-id pdi_cluster`); 'local' runs Kitchen/Pan on the Airflow
    # worker itself.
    run_config: str = 'carte'           # carte | local
    # Every generated DAG ends in a publish_pdi_lineage task by default,
    # so each SCHEDULED run refreshes Marquez/PDC with that run's
    # lineage and row counts.
    lineage: bool = True
    marquez_url: Optional[str] = None   # baked default; Variable overrides
    pdc_conn_id: str = 'pdc_lineage'
    server_name: str = 'pdi2dag'        # OL namespace / PDC server node


@dataclass
class ConvertResult:
    dag_id: str
    code: str
    warnings: List[str] = field(default_factory=list)


def _sanitize_id(name):
    cleaned = re.sub(r'[^0-9A-Za-z_\-.]', '_', name.strip())
    cleaned = re.sub(r'_+', '_', cleaned).strip('_')
    return cleaned or 'task'


def _unique_id(base, used):
    candidate = base
    i = 2
    while candidate in used:
        candidate = '{}_{}'.format(base, i)
        i += 1
    used.add(candidate)
    return candidate


def _build_params(doc, overrides):
    """PDI named parameters + CLI overrides -> task params dict."""
    params = {p.name: p.default for p in doc.parameters}
    params.update(overrides)
    return params


def _collapse_dependencies(doc):
    """Map PDI hops to dependencies between executable entries.

    Control entries (Start, Dummy, Success...) are collapsed: a path
    TRANS_A -> Dummy -> TRANS_B becomes TRANS_A >> TRANS_B. Returns
    ``(deps, incoming_hop_kinds, warnings)`` where deps maps entry name
    -> sorted list of upstream executable entry names.
    """
    warnings = []
    by_name = {e.name: e for e in doc.entries}
    enabled_hops = [h for h in doc.hops if h.enabled]
    for hop in doc.hops:
        if not hop.enabled:
            warnings.append(
                'Disabled hop {} -> {} was skipped.'.format(
                    hop.from_name, hop.to_name))

    incoming = {}   # entry name -> list of hops into it
    for hop in enabled_hops:
        incoming.setdefault(hop.to_name, []).append(hop)

    def exec_upstreams(name, seen):
        """Walk backwards through control entries to find executable
        upstream entries."""
        results = []
        for hop in incoming.get(name, []):
            src = by_name.get(hop.from_name)
            if src is None or src.name in seen:
                continue
            if src.is_executable:
                results.append((src.name, hop))
            else:
                seen.add(src.name)
                results.extend(exec_upstreams(src.name, seen))
        return results

    deps = {}
    hop_kinds = {}
    for entry in doc.executable_entries:
        upstreams = exec_upstreams(entry.name, set())
        deps[entry.name] = sorted({name for name, _ in upstreams})
        for _, hop in upstreams:
            if hop.unconditional:
                hop_kinds[entry.name] = 'all_done'
                warnings.append(
                    "Unconditional hop into '{}' mapped to "
                    "trigger_rule='all_done'.".format(entry.name))
            elif not hop.evaluation:
                hop_kinds[entry.name] = 'one_failed'
                warnings.append(
                    "Failure hop into '{}' mapped to "
                    "trigger_rule='one_failed' - review this "
                    'branch.'.format(entry.name))
    return deps, hop_kinds, warnings


def _header(doc, options, warnings):
    lines = [
        '# -*- coding: utf-8 -*-',
        '"""DAG generated by pdi2dag v{} - review before production use.'
        .format(__version__),
        '',
        'Source:    {}'.format(doc.source_file.replace('\\', '/')),
        'PDI {}: {} (repository path {})'.format(
            doc.kind, doc.name, doc.repo_path),
    ]
    if warnings:
        lines.append('')
        lines.append('Migration notes:')
        for w in warnings:
            lines.append('  - {}'.format(w))
    lines.append('"""')
    return lines


def _dag_block(dag_id, doc, options):
    schedule = repr(options.schedule)
    start = options.start_date
    if start:
        year, month, day = (int(x) for x in start.split('-'))
    else:
        today = date.today()
        year, month, day = today.year, today.month, today.day
    description = 'Migrated from PDI {} {}'.format(
        doc.kind, repr(doc.name))
    return [
        'with DAG(',
        '    dag_id={},'.format(repr(dag_id)),
        '    description={},'.format(repr(description)),
        '    schedule={},'.format(schedule),
        "    start_date=pendulum.datetime({}, {}, {}, tz='UTC'),".format(
            year, month, day),
        '    catchup=False,',
        '    default_args={{'
        "'owner': {}, 'retries': {}}},".format(
            repr(options.owner), options.retries),
        '    tags={},'.format(repr(options.tags)),
        ') as dag:',
    ]


def _operator_kwargs_lines(indent, options):
    lines = [
        "{}pdi_conn_id={},".format(indent, repr(options.conn_id)),
        '{}params=PDI_PARAMS,'.format(indent),
        '{}level={},'.format(indent, repr(options.level)),
    ]
    if options.deferrable and options.run_config == 'carte':
        lines.append('{}deferrable=True,'.format(indent))
        lines.append('{}poll_interval={},'.format(
            indent, options.poll_interval))
    return lines


def _operator_for(kind, options):
    """(operator class name, target kwarg) for one executable, per the
    run configuration: Carte submits the repository path to the server
    behind the connection; local runs Kitchen/Pan on the worker with
    the same repository coordinates."""
    if options.run_config == 'local':
        return (('PanOperator', 'trans') if kind == TYPE_TRANS
                else ('KitchenOperator', 'job'))
    return (('CarteTransOperator', 'trans') if kind == TYPE_TRANS
            else ('CarteJobOperator', 'job'))


def _target_lines(indent, arg, repo_path, options):
    """The lines binding a task to its PDI executable. Carte takes the
    full repository path; Kitchen/Pan take name + directory."""
    if options.run_config == 'local':
        directory, _, name = (repo_path or '/').rpartition('/')
        return [
            '{}{}={},'.format(indent, arg, repr(name)),
            '{}directory={},'.format(indent, repr(directory or '/')),
        ]
    return ['{}{}={},'.format(indent, arg, repr(repo_path))]


def convert(doc: PdiDocument, options: ConvertOptions,
            trans_details=None) -> ConvertResult:
    """Generate Airflow DAG source for a parsed PDI document.

    ``trans_details`` (transformation name -> PdiTransDetail) enriches
    the baked lineage spec with each transformation's datasets and
    column facets; without it the spec still carries the job structure.
    """
    mode = options.mode
    if doc.kind != 'job':
        mode = 'wrap'
    elif mode == 'auto':
        mode = 'explode' if doc.executable_entries else 'wrap'

    dag_id = _sanitize_id(options.dag_id or doc.name)
    params = _build_params(doc, options.params)
    warnings = []

    if (doc.kind == 'job' and not doc.executable_entries
            and options.mode != 'wrap'):
        warnings.append(
            'Job has no TRANS/JOB entries; generated a wrap-mode task '
            'instead.')
        mode = 'wrap'

    body = []
    needs_empty = False

    if options.deferrable and options.run_config == 'local':
        warnings.append(
            'deferrable applies to Carte runs only; local Kitchen/Pan '
            'tasks occupy a worker slot for their duration.')

    all_task_ids = []
    if mode == 'wrap':
        kind = TYPE_JOB if doc.kind == 'job' else TYPE_TRANS
        op, arg = _operator_for(kind, options)
        task_id = _sanitize_id('run_{}'.format(doc.name))
        all_task_ids.append(task_id)
        body.append('    {} = {}('.format(task_id, op))
        body.append('        task_id={},'.format(repr(task_id)))
        body.extend(_target_lines('        ', arg, doc.repo_path, options))
        body.extend(_operator_kwargs_lines('        ', options))
        body.append('    )')
    else:
        deps, hop_kinds, dep_warnings = _collapse_dependencies(doc)
        warnings.extend(dep_warnings)

        used_ids = set()
        task_ids = {}
        for entry in doc.executable_entries:
            task_ids[entry.name] = _unique_id(
                _sanitize_id(entry.name), used_ids)

        unsupported = [
            e for e in doc.entries
            if not e.is_executable and not e.is_start
            and e.entry_type not in ('SUCCESS', 'DUMMY', 'SPECIAL')]
        for entry in unsupported:
            warnings.append(
                "Entry '{}' of type {} has no Airflow equivalent and was "
                'not migrated - port it manually (e.g. BashOperator, '
                'EmailOperator).'.format(entry.name, entry.entry_type))

        for entry in doc.executable_entries:
            task_id = task_ids[entry.name]
            all_task_ids.append(task_id)
            op, arg = _operator_for(entry.entry_type, options)
            if not entry.path:
                warnings.append(
                    "Entry '{}' has no resolvable repository path; "
                    'fix the generated task manually.'.format(entry.name))
            if entry.filename and not entry.path:
                needs_empty = True
            body.append('    {} = {}('.format(task_id, op))
            body.append('        task_id={},'.format(repr(task_id)))
            body.extend(_target_lines(
                '        ', arg, entry.path or 'FIXME/' + entry.name,
                options))
            body.extend(_operator_kwargs_lines('        ', options))
            if entry.name in hop_kinds:
                body.append('        trigger_rule={},'.format(
                    repr(hop_kinds[entry.name])))
            body.append('    )')
            body.append('')

        dep_lines = []
        for entry_name, upstreams in deps.items():
            for upstream in upstreams:
                dep_lines.append('    {} >> {}'.format(
                    task_ids[upstream], task_ids[entry_name]))
        if dep_lines:
            body.append('    # Dependencies from PDI hops')
            body.extend(sorted(dep_lines))

    lineage_lines = []
    spec_lines = []
    if options.lineage and all_task_ids:
        from pdi2dag.lineage import build_dag_lineage_spec
        if doc.kind == 'job':
            spec = build_dag_lineage_spec(
                doc=doc, trans_details=trans_details,
                server_name=options.server_name)
        else:
            detail = (trans_details or {}).get(doc.name)
            if detail is not None:
                spec = build_dag_lineage_spec(
                    detail=detail, server_name=options.server_name)
            else:
                spec = {'server_name': options.server_name,
                        'job_name': None, 'job_type': 'transformation',
                        'entries': [{'name': doc.repo_path,
                                     'job_type': 'transformation',
                                     'trans': doc.name}]}
        import pprint
        spec_lines = ['', '# Lineage spec baked at migration time; the',
                      '# publish task stamps run ids and merges the',
                      "# run's Carte metrics at execution time.",
                      'PDI_LINEAGE = {}'.format(
                          pprint.pformat(spec, width=72, sort_dicts=False)),
                      '']
        lineage_lines = [
            '',
            '    publish_lineage = PythonOperator(',
            "        task_id='publish_pdi_lineage',",
            '        python_callable=publish_pdi_lineage,',
            '        op_kwargs={',
            "            'spec': PDI_LINEAGE,",
            "            'pdi_conn_id': {},".format(repr(options.conn_id)),
            "            'marquez_url': {},".format(
                repr(options.marquez_url)),
            "            'pdc_conn_id': {},".format(
                repr(options.pdc_conn_id)),
            '        },',
            "        trigger_rule='all_done',",
            '    )',
            '    # every run publishes its own lineage, success or not.',
            '    # As a TEARDOWN the task never masks a real failure -',
            '    # the run state follows the PDI tasks, not the publisher.',
            '    publish_lineage.as_teardown()',
        ]
        for task_id in all_task_ids:
            lineage_lines.append('    {} >> publish_lineage'.format(task_id))

    header = _header(doc, options, warnings)

    imports = [
        '',
        'import pendulum',
        '',
        'from airflow import DAG',
    ]
    ops_used = {line.strip().split(' = ')[1].split('(')[0]
                for line in body if ' = Carte' in line}
    for op in sorted(ops_used):
        imports.append(
            'from airflow_pentaho.operators.carte import {}'.format(op))
    kettle_ops = {line.strip().split(' = ')[1].split('(')[0]
                  for line in body
                  if ' = PanOperator' in line
                  or ' = KitchenOperator' in line}
    for op in sorted(kettle_ops):
        imports.append(
            'from airflow_pentaho.operators.kettle import {}'.format(op))
    if lineage_lines:
        imports.append('from airflow_pentaho.lineage import '
                       'publish_pdi_lineage')
        imports.append('try:')
        imports.append('    from airflow.providers.standard.operators.'
                       'python import PythonOperator')
        imports.append('except ImportError:  # Airflow 2.x')
        imports.append('    from airflow.operators.python import '
                       'PythonOperator')
    if needs_empty:
        imports.append('from airflow.operators.empty import EmptyOperator')
    imports.append('')

    param_lines = ['PDI_PARAMS = {']
    for k, v in params.items():
        param_lines.append('    {}: {},'.format(repr(k), repr(v)))
    param_lines.append('}')
    param_lines.append('')

    code = '\n'.join(
        header + imports + param_lines + spec_lines
        + _dag_block(dag_id, doc, options) + body + lineage_lines) + '\n'
    return ConvertResult(dag_id=dag_id, code=code, warnings=warnings)

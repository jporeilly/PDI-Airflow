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
"""Emit PDI structure to Marquez as OpenLineage events.

Airflow's OpenLineage provider only sees Airflow tasks - the inside of
a PDI job stays a black box. This module opens it up:

- **Job level** (.kjb): every TRANS/JOB entry becomes an OpenLineage
  job; hops become dataset edges (each entry writes a synthetic
  ``pdi://`` dataset that its downstream entries read), so Marquez
  renders the same graph you see in Spoon.
- **Step level** (.ktr): every step becomes an OpenLineage job and
  every hop a dataset edge - the full step graph of a transformation.

Events are emitted as completed runs (START + COMPLETE), so the graph
appears immediately without waiting for a Carte execution.
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timedelta, timezone

import requests

from pdi2dag.generator import _collapse_dependencies, _sanitize_id
from pdi2dag.parser import TYPE_TRANS

PRODUCER = 'https://www.pentaho.com/pdi2dag'
SCHEMA_URL = 'https://openlineage.io/spec/2-0-2/OpenLineage.json'
JOB_TYPE_FACET_URL = ('https://openlineage.io/spec/facets/2-0-3/'
                      'JobTypeJobFacet.json')
DOC_FACET_URL = ('https://openlineage.io/spec/facets/1-0-1/'
                 'DocumentationJobFacet.json')
OUTPUT_STATS_URL = ('https://openlineage.io/spec/facets/1-0-2/'
                    'OutputStatisticsOutputDatasetFacet.json')
DQ_METRICS_URL = ('https://openlineage.io/spec/facets/1-0-1/'
                  'DataQualityMetricsInputDatasetFacet.json')


def _dataset(namespace, name):
    return {'namespace': namespace, 'name': name}


def _output_dataset(namespace, name, row_count=None):
    """Output dataset with an optional real rowCount (from Carte)."""
    ds = {'namespace': namespace, 'name': name}
    if row_count is not None:
        ds['outputFacets'] = {
            'outputStatistics': {
                '_producer': PRODUCER,
                '_schemaURL': OUTPUT_STATS_URL,
                'rowCount': int(row_count),
            },
        }
    return ds


def _input_dataset(namespace, name, row_count=None):
    """Input dataset with an optional real rowCount (from Carte)."""
    ds = {'namespace': namespace, 'name': name}
    if row_count is not None:
        ds['inputFacets'] = {
            'dataQualityMetrics': {
                '_producer': PRODUCER,
                '_schemaURL': DQ_METRICS_URL,
                'rowCount': int(row_count),
            },
        }
    return ds


def parse_carte_step_metrics(trans_status):
    """Extract per-step runtime metrics from a Carte ``transStatus``
    response (already parsed by xmltodict). Returns
    ``{step_name: {read, written, input, output, updated, rejected,
    errors}}``. Safe on missing/partial data.
    """
    ts = (trans_status or {}).get('transstatus') or {}
    lst = (ts.get('stepstatuslist') or {}).get('stepstatus') or []
    if isinstance(lst, dict):
        lst = [lst]

    def _int(v):
        try:
            return int(v)
        except (TypeError, ValueError):
            return 0

    metrics = {}
    for s in lst:
        name = s.get('stepname')
        if not name:
            continue
        metrics[name] = {
            'read': _int(s.get('linesRead')),
            'written': _int(s.get('linesWritten')),
            'input': _int(s.get('linesInput')),
            'output': _int(s.get('linesOutput')),
            'updated': _int(s.get('linesUpdated')),
            'rejected': _int(s.get('linesRejected')),
            'errors': _int(s.get('errors')),
        }
    return metrics


def _job_facets(description, job_type):
    return {
        'documentation': {
            '_producer': PRODUCER,
            '_schemaURL': DOC_FACET_URL,
            'description': description,
        },
        'jobType': {
            '_producer': PRODUCER,
            '_schemaURL': JOB_TYPE_FACET_URL,
            'processingType': 'BATCH',
            'integration': 'PENTAHO',
            'jobType': job_type,
        },
    }


def _run_events(namespace, job_name, inputs, outputs, description,
                job_type, event_time=None):
    """A START/COMPLETE pair describing one job with its edges."""
    start = event_time or datetime.now(timezone.utc)
    base = {
        'producer': PRODUCER,
        'schemaURL': SCHEMA_URL,
        'run': {'runId': str(uuid.uuid4())},
        'job': {
            'namespace': namespace,
            'name': job_name,
            'facets': _job_facets(description, job_type),
        },
        'inputs': [_dataset(namespace, n) for n in inputs],
        'outputs': [_dataset(namespace, n) for n in outputs],
    }
    return [
        dict(base, eventType='START', eventTime=start.isoformat()),
        dict(base, eventType='COMPLETE',
             eventTime=(start + timedelta(seconds=1)).isoformat()),
    ]


def build_job_model_events(doc, namespace='pdi', trans_details=None):
    """Entry-level lineage for a parsed .kjb document.

    One OpenLineage job per TRANS/JOB entry, connected through
    synthetic ``pdi://<job>/<entry>`` datasets derived from the hops
    (control entries collapsed, like the DAG generator does).

    When ``trans_details`` (a dict of transformation name ->
    :class:`PdiTransDetail`) covers an entry's transformation, its step
    graph is spliced INTO the job graph: the entry job feeds a
    ``.../start`` dataset into the transformation's first step(s), and
    the terminal step(s) produce the entry's result dataset that
    downstream entries consume - job entries and steps render as one
    connected graph in Marquez.
    """
    trans_details = trans_details or {}
    deps, _, _ = _collapse_dependencies(doc)
    events = []
    for entry in doc.executable_entries:
        slug = _sanitize_id(entry.name)
        job_name = '{}.{}'.format(doc.name, slug)
        result_ds = 'pdi://{}/{}'.format(doc.name, slug)
        inputs = ['pdi://{}/{}'.format(doc.name, _sanitize_id(up))
                  for up in deps.get(entry.name, [])]
        description = "PDI {} entry '{}'{}".format(
            'job' if entry.entry_type == 'JOB' else 'transformation',
            entry.name,
            ' -> {}'.format(entry.path) if entry.path else '')

        trans_name = (entry.path or '').split('/')[-1]
        detail = (trans_details.get(trans_name)
                  if entry.entry_type == TYPE_TRANS else None)
        if detail and detail.steps:
            # Route the flow through the steps: entry -> start ds ->
            # first steps -> ... -> terminal steps -> result ds.
            start_ds = result_ds + '/start'
            events.extend(_run_events(
                namespace, job_name, inputs, [start_ds], description,
                entry.entry_type))
            events.extend(build_trans_model_events(
                detail, namespace=namespace,
                entry_input=start_ds, exit_output=result_ds))
        else:
            events.extend(_run_events(
                namespace, job_name, inputs, [result_ds], description,
                entry.entry_type))
    return events


def build_trans_model_events(detail, namespace='pdi', prefix=None,
                             entry_input=None, exit_output=None):
    """Step-level lineage for a parsed .ktr (PdiTransDetail).

    One OpenLineage job per step; each hop becomes a dataset edge
    (``pdi://<trans>/<step>``), so Marquez draws the step graph.

    ``entry_input`` / ``exit_output`` splice the step graph into a
    surrounding job graph: first steps (no upstream hop) additionally
    read ``entry_input``; terminal steps (no downstream hop)
    additionally write ``exit_output``.
    """
    upstream = {}
    has_downstream = set()
    for hop in detail.hops:
        if hop.enabled:
            upstream.setdefault(hop.to_name, []).append(hop.from_name)
            has_downstream.add(hop.from_name)

    trans_id = prefix or detail.name
    events = []
    for step in detail.steps:
        slug = _sanitize_id(step.name)
        job_name = '{}.{}'.format(trans_id, slug)
        outputs = ['pdi://{}/{}'.format(trans_id, slug)]
        inputs = ['pdi://{}/{}'.format(trans_id, _sanitize_id(up))
                  for up in upstream.get(step.name, [])]
        if entry_input and step.name not in upstream:
            inputs.append(entry_input)
        if exit_output and step.name not in has_downstream:
            outputs.append(exit_output)
        description = "PDI step '{}' (type {}) in transformation " \
            "'{}'".format(step.name, step.step_type, detail.name)
        events.extend(_run_events(
            namespace, job_name, inputs, outputs, description, 'STEP'))
    return events


# The exact convention used by the official PDI OpenLineage plugin
# (pdi-openlineage-plugin-core 0.7.0), decompiled from the plugin JAR:
#   producer  = the plugin's GitHub tree URL
#   namespace = the PDI server hostname (Config localHostname /
#               HostnameResolver) for repository jobs; file:// URI for
#               file-based ones. This becomes the "PDI Server" node.
#   job name  = the repository path with a leading '/' (getPathAndName
#               + prependIfMissing '/'), e.g. /demo/nightly_etl -
#               path segments become the tree's Folders.
#   jobType facet: processingType=BATCH, integration=PDI,
#               jobType='job'|'transformation' (lowercase)
#   the Job > Transformation tree comes from ParentRunFacet
#   (run + job + root), NOT from datasets.
PDI_PLUGIN_PRODUCER = ('https://github.com/pentaho/pdi-plugins-ee/tree/'
                       'pdi-openlineage-plugin-ee/pdi-openlineage-plugin')


def _pdi_job_facets(job_type):
    """jobType facet exactly as the plugin's JobFacets emits it."""
    return {
        'jobType': {
            '_producer': PDI_PLUGIN_PRODUCER,
            '_schemaURL': JOB_TYPE_FACET_URL,
            'processingType': 'BATCH',
            'integration': 'PDI',
            'jobType': job_type,   # 'job' or 'transformation', lowercase
        },
    }


# --- database dataset naming (matches what PDC ingests: it creates a
# data source <TYPE>-<host>-<schema> and TABLE entities from these) ---

# Dataset namespace schemes, per the OpenLineage dataset naming spec.
# These MUST match what the catalog already holds or the lineage lands
# on a second, disconnected node: PDC catalogues PostgreSQL as
# `postgres://host:port`, so emitting `postgresql://` produced a
# lookalike dataset with the identical name that was never linked to the
# real table.
_DB_SCHEME = {
    'POSTGRESQL': 'postgres', 'POSTGRES': 'postgres',
    'MYSQL': 'mysql', 'MARIADB': 'mysql',
    'ORACLE': 'oracle', 'MSSQL': 'sqlserver',
    'MSSQLNATIVE': 'sqlserver', 'SQLSERVER': 'sqlserver',
    'VERTICA': 'vertica', 'SNOWFLAKEHV': 'snowflake',
    'GENERIC': 'jdbc',
}
_DEFAULT_PORT = {
    'postgres': '5432', 'mysql': '3306', 'oracle': '1521',
    'sqlserver': '1433', 'vertica': '5433',
}


def _conn_namespace(conn):
    """OpenLineage dataset namespace for a DB connection:
    ``<scheme>://<host>:<port>`` (the storage system)."""
    scheme = _DB_SCHEME.get((conn.db_type or '').upper(), 'jdbc')
    host = conn.server or 'localhost'
    port = conn.port or _DEFAULT_PORT.get(scheme, '')
    return '{}://{}:{}'.format(scheme, host, port) if port \
        else '{}://{}'.format(scheme, host)


_TABLE_RE = re.compile(
    r'\b(?:FROM|JOIN)\s+"?([A-Za-z_][\w]*(?:"?\."?[A-Za-z_][\w]*)?)"?',
    re.IGNORECASE)


def _tables_from_sql(sql):
    """Best-effort source tables from a Table Input SQL (FROM/JOIN)."""
    if not sql:
        return []
    seen, out = set(), []
    for m in _TABLE_RE.findall(sql):
        name = m.replace('"', '')
        if name.lower() not in seen:
            seen.add(name.lower())
            out.append(name)
    return out


def _file_dataset(path, vfs_scheme='s3'):
    """OpenLineage dataset for a file/object step. Follows the file
    naming convention: object stores keep their scheme
    (``s3://bucket`` + key), local/VFS files use ``file`` namespace +
    the path. PDI ``${var}`` tokens are left intact.

    ``pvfs://`` is unwrapped first. It is PDI's *connection-scoped* form
    - ``pvfs://<connection-name>/<bucket>/<key>`` - so the leading
    segment is a PDI alias, not a storage host. Emitted verbatim it
    would namespace the dataset by the alias (``pvfs://cscu-minio``) and
    bury the bucket in the name, so the lineage could never match the
    catalogued object store. Rewrite to the physical ``s3://<bucket>``
    form, which is what the catalog actually holds.
    """
    p = (path or '').replace('\\', '/')
    m = re.match(r'^pvfs://([^/]+)/(.*)$', p)
    if m:
        # Drop the connection alias; the next segment is the bucket.
        p = '{}://{}'.format(vfs_scheme, m.group(2))
    m = re.match(r'^([a-zA-Z][a-zA-Z0-9+.-]*)://([^/]+)/?(.*)$', p)
    if m:
        scheme, host, rest = m.group(1), m.group(2), m.group(3)
        if scheme in ('file',):
            return {'namespace': 'file', 'name': '/' + rest}
        return {'namespace': '{}://{}'.format(scheme, host),
                'name': rest or host}
    # bare local path
    return {'namespace': 'file', 'name': p}


def _fq_dataset_name(conn, schema, table):
    """Dataset name as the plugin's SqlDataset builds it:
    ``database.schema.table`` (FORMAT_DB_SCHEMA_TABLE = '%s.%s.%s').
    Falls back to schema.table or table when parts are unknown."""
    database = (conn.database if conn else '') or ''
    if database and schema and table:
        return '{}.{}.{}'.format(database, schema, table)
    if schema and table:
        return '{}.{}'.format(schema, table)
    return table


# Steps PDC recognises for lineage (per the PDI OpenLineage plugin
# documentation). Anything else - Write to Log, Dummy, Select Values,
# calculators - carries no dataset, so it contributes nothing to the
# graph.
PDC_LINEAGE_STEP_TYPES = {
    'TableInput', 'TableOutput',
    'TextFileInput', 'TextFileOutput',
    'CsvInput', 'S3CSVINPUT', 'S3FileOutputPlugin',
    'ExcelInput', 'ExcelOutput', 'ExcelWriter',
    'TypeExitExcelWriterStep',
}


def lineage_warnings(detail):
    """Why a transformation may show nothing in PDC's lineage graph.

    PDC draws an edge between datasets. A transformation whose only
    downstream step is unsupported (classically **Write to Log**) emits
    an input and no output - half an edge, nothing to draw - yet PDC
    still answers 200, so the publish looks successful. Surface that
    before the user goes hunting in the UI.
    """
    inputs, outputs = trans_datasets(detail)
    warnings = []
    if not inputs and not outputs:
        warnings.append(
            "'{}' has no lineage-carrying steps, so PDC will show "
            'nothing. Supported: Table input/output, Text file '
            'input/output, S3 CSV input, S3 file output, Excel '
            'input/writer.'.format(detail.name))
    elif not outputs:
        unsupported = sorted({
            s.step_type for s in detail.steps
            if s.step_type not in PDC_LINEAGE_STEP_TYPES})
        warnings.append(
            "'{}' reads data but writes no dataset, so PDC has only "
            'half an edge and draws nothing. Add a supported output '
            'step (Table output, Text file output, S3 file output). '
            'Unsupported steps here: {}.'.format(
                detail.name, ', '.join(unsupported) or 'none'))
    return warnings


def trans_datasets(detail, step_metrics=None):
    """Resolve a transformation's input/output DB datasets from its
    Table Input / Table Output steps, matching the PDI OpenLineage
    plugin's naming: namespace ``<protocol>://<host>:<port>``, name
    ``database.schema.table``.

    When ``step_metrics`` (from :func:`parse_carte_step_metrics`) is
    given, each dataset gets a real ``rowCount`` from the Carte run -
    Table Input rows read, Table Output rows written.

    Returns (inputs, outputs) as lists of ``{'namespace', 'name',
    'rowCount'?}`` dicts."""
    step_metrics = step_metrics or {}
    conns = {c.name: c for c in detail.connections}
    default_ns = 'jdbc://unknown'
    inputs, outputs = [], []
    for step in detail.steps:
        m = step_metrics.get(step.name, {})
        if step.step_type == 'TableInput' and step.sql:
            conn = conns.get(step.connection)
            ns = _conn_namespace(conn) if conn else default_ns
            rows = m.get('input') or m.get('read')
            for tok in _tables_from_sql(step.sql):
                schema, table = (tok.split('.', 1) if '.' in tok
                                 else ('', tok))
                ds = {'namespace': ns,
                      'name': _fq_dataset_name(conn, schema, table)}
                if rows:
                    ds['rowCount'] = rows
                inputs.append(ds)
        elif step.step_type == 'TableOutput' and step.table:
            conn = conns.get(step.connection)
            ns = _conn_namespace(conn) if conn else default_ns
            rows = m.get('written') or m.get('output')
            ds = {'namespace': ns,
                  'name': _fq_dataset_name(conn, step.schema, step.table)}
            if rows:
                ds['rowCount'] = rows
            outputs.append(ds)
        elif step.is_file_input and step.files:
            rows = m.get('output') or m.get('read')
            for f in step.files:
                ds = _file_dataset(f)
                if rows:
                    ds['rowCount'] = rows
                inputs.append(ds)
        elif step.is_file_output and step.files:
            rows = m.get('written') or m.get('input')
            for f in step.files:
                ds = _file_dataset(f)
                if rows:
                    ds['rowCount'] = rows
                outputs.append(ds)
    return inputs, outputs


def build_pdc_etl_events(doc, trans_details=None, namespace=None,
                         server_name='pdi2dag', step_metrics=None):
    """Events matching the official PDI OpenLineage plugin, so PDC's
    ETL Pipelines tree (PDI Server > Folder > Job > Transformation)
    materializes.

    ``server_name`` is the namespace = the "PDI Server" node label
    (the plugin uses the configured/resolved hostname). Job and
    transformation names are their repository paths; the tree's
    Job > Transformation nesting comes from the ParentRunFacet, which
    references the parent job run and the root run.

    When ``trans_details`` (transformation name -> PdiTransDetail) is
    given, each transformation event carries the input/output table
    datasets resolved from its Table Input/Output steps - PDC then
    creates the data connections, TABLE entities and dataset lineage.
    """
    trans_details = trans_details or {}
    step_metrics = step_metrics or {}   # trans name -> parsed metrics
    ns = namespace or server_name
    now = datetime.now(timezone.utc)
    events = []

    job_run_id = str(uuid.uuid4())
    job_name = doc.repo_path            # e.g. /demo/nightly_etl

    # Parent/root run facet shared by every child (transformation) run.
    parent_run_facet = {
        'parent': {
            '_producer': PDI_PLUGIN_PRODUCER,
            '_schemaURL': 'https://openlineage.io/spec/facets/1-0-1/'
                          'ParentRunFacet.json',
            'run': {'runId': job_run_id},
            'job': {'namespace': ns, 'name': job_name},
            'root': {
                'run': {'runId': job_run_id},
                'job': {'namespace': ns, 'name': job_name},
            },
        },
    }

    def run_events(name, run_id, run_facets, job_type, event_time,
                   inputs=None, outputs=None, failed=False):
        base = {
            'producer': PDI_PLUGIN_PRODUCER,
            'schemaURL': SCHEMA_URL,
            'run': {'runId': run_id, 'facets': run_facets},
            'job': {
                'namespace': ns,
                'name': name,
                'facets': _pdi_job_facets(job_type),
            },
            'inputs': [_input_dataset(d['namespace'], d['name'],
                                      d.get('rowCount'))
                       for d in (inputs or [])],
            'outputs': [_output_dataset(d['namespace'], d['name'],
                                        d.get('rowCount'))
                        for d in (outputs or [])],
        }
        return [
            dict(base, eventType='START', eventTime=event_time.isoformat()),
            dict(base, eventType='FAIL' if failed else 'COMPLETE',
                 eventTime=(event_time + timedelta(seconds=2)).isoformat()),
        ]

    # Root job event (no parent - it IS the root).
    events += run_events(job_name, job_run_id, {}, 'job', now)

    # Each executable entry as a child run linked to the job via parent,
    # carrying its table datasets (+ real row counts when Carte metrics
    # are supplied) when the .ktr is available.
    for i, entry in enumerate(doc.executable_entries):
        name = entry.path or '{}/{}'.format(
            doc.repo_path.rsplit('/', 1)[0], _sanitize_id(entry.name))
        job_type = 'job' if entry.entry_type == 'JOB' else 'transformation'
        inputs = outputs = None
        failed = False
        if entry.entry_type == TYPE_TRANS:
            tname = (entry.path or '').split('/')[-1]
            detail = trans_details.get(tname)
            if detail:
                metrics = step_metrics.get(tname)
                inputs, outputs = trans_datasets(detail, metrics)
                failed = bool(metrics) and any(
                    m.get('errors') for m in metrics.values())
        events += run_events(
            name, str(uuid.uuid4()), dict(parent_run_facet), job_type,
            now + timedelta(seconds=3 + i), inputs, outputs, failed)
    return events


def build_pdc_trans_events(detail, repo_path, namespace=None,
                           server_name='pdi2dag', step_metrics=None):
    """PDC events for a standalone transformation (.ktr) - one
    transformation run (no parent job) carrying its table datasets,
    with real row counts + run state when ``step_metrics`` (from a
    Carte transStatus) are supplied."""
    ns = namespace or server_name
    now = datetime.now(timezone.utc)
    inputs, outputs = trans_datasets(detail, step_metrics)
    failed = bool(step_metrics) and any(
        m.get('errors') for m in (step_metrics or {}).values())
    job = {
        'namespace': ns,
        'name': repo_path,
        'facets': _pdi_job_facets('transformation'),
    }
    run_id = str(uuid.uuid4())
    base = {
        'producer': PDI_PLUGIN_PRODUCER,
        'schemaURL': SCHEMA_URL,
        'run': {'runId': run_id, 'facets': {}},
        'job': job,
        'inputs': [_input_dataset(d['namespace'], d['name'],
                                  d.get('rowCount')) for d in inputs],
        'outputs': [_output_dataset(d['namespace'], d['name'],
                                    d.get('rowCount')) for d in outputs],
    }
    return [
        dict(base, eventType='START', eventTime=now.isoformat()),
        dict(base, eventType='FAIL' if failed else 'COMPLETE',
             eventTime=(now + timedelta(seconds=2)).isoformat()),
    ]


def emit(events, marquez_url, timeout=30):
    """POST OpenLineage events to a Marquez (or any OL-compatible)
    endpoint. Returns the number of events accepted."""
    url = marquez_url.rstrip('/') + '/api/v1/lineage'
    for event in events:
        rs = requests.post(url, json=event, timeout=timeout)
        if rs.status_code >= 400:
            raise RuntimeError(
                'Marquez rejected event for job {} (HTTP {}): {}'.format(
                    event['job']['name'], rs.status_code, rs.text[:300]))
    return len(events)


def _pdc_token(base_url, username, password, realm='pdc',
               client_id='pdc-client', verify_tls=False, timeout=30):
    if not verify_tls:
        import urllib3
        urllib3.disable_warnings()
    """Bearer token from PDC's Keycloak (same flow the official PDI
    OpenLineage plugin documents: client pdc-client, scope openid)."""
    url = '{}/keycloak/realms/{}/protocol/openid-connect/token'.format(
        base_url.rstrip('/'), realm)
    rs = requests.post(url, data={
        'client_id': client_id, 'grant_type': 'password',
        'username': username, 'password': password, 'scope': 'openid'},
        verify=verify_tls, timeout=timeout)
    if rs.status_code >= 400:
        raise RuntimeError('PDC Keycloak auth failed (HTTP {}): {}'.format(
            rs.status_code, rs.text[:200]))
    token = rs.json().get('access_token')
    if not token:
        raise RuntimeError('PDC Keycloak returned no access_token')
    return token


# PDI connection type -> PDC databaseType code
_PDC_DB_TYPE = {
    'POSTGRESQL': 'POSTGRES', 'POSTGRES': 'POSTGRES',
    'MYSQL': 'MYSQL', 'MARIADB': 'MYSQL',
    'ORACLE': 'ORACLE', 'MSSQL': 'MSSQL', 'MSSQLNATIVE': 'MSSQL',
    'VERTICA': 'VERTICA',
}


def collect_connections(details):
    """From parsed transformations (PdiTransDetail list), return the
    unique database connections keyed by identity, each with the set
    of schemas its Table Input/Output steps reference.

    Returns a list of ``(PdiConnection, sorted_schemas)`` tuples.
    """
    found = {}   # (type, host, port, db) -> [conn, {schemas}]
    for detail in details:
        by_name = {c.name: c for c in detail.connections}
        for step in detail.steps:
            conn = by_name.get(step.connection)
            if not conn:
                continue
            key = (conn.db_type, conn.server, conn.port, conn.database)
            entry = found.setdefault(key, [conn, set()])
            if step.step_type == 'TableOutput' and step.schema:
                entry[1].add(step.schema)
            elif step.step_type == 'TableInput' and step.sql:
                for tok in _tables_from_sql(step.sql):
                    if '.' in tok:
                        entry[1].add(tok.split('.', 1)[0])
    return [(c, sorted(s)) for c, s in found.values()]


def build_connection_body(conn, schemas):
    """PDC create-data-source body for a PDI connection - identity,
    username and schemas only. **No password** (never handled here);
    the user completes credentials in PDC."""
    db_type = _PDC_DB_TYPE.get((conn.db_type or '').upper(),
                               (conn.db_type or '').upper())
    scheme = _DB_SCHEME.get((conn.db_type or '').upper(), 'jdbc')
    port = conn.port or _DEFAULT_PORT.get(scheme, '')
    name = '{}-{}-{}'.format(db_type, conn.server, conn.database)
    return {
        'resourceName': name,
        'fqdnId': name,
        'configMethod': 'credentials',
        'databaseType': db_type,
        'host': conn.server,
        'port': str(port),
        'databaseName': conn.database,
        'userName': conn.username or '',
        'schemaNames': schemas,
        'description': 'Provisioned by pdi2dag from a PDI connection '
                       '(set the password in PDC to enable scanning).',
    }


def provision_connections(details, base_url, username, password,
                          verify_tls=False, timeout=30, dry_run=False):
    """Pre-create PDC data connections from PDI connection definitions
    so lineage attaches to real, credentialed connections instead of
    stubs. Returns a list of ``(resourceName, status)`` tuples.

    ``dry_run=True`` builds the bodies without sending them. Passwords
    are never sent - complete each connection in PDC afterwards.
    """
    bodies = [build_connection_body(c, s)
              for c, s in collect_connections(details)]
    if dry_run:
        return [(b['resourceName'], 'dry-run') for b in bodies]

    token = _pdc_token(base_url, username, password,
                       verify_tls=verify_tls, timeout=timeout)
    url = base_url.rstrip('/') + '/api/public/v2/data-sources'
    headers = {'Authorization': 'Bearer ' + token}
    results = []
    for body in bodies:
        rs = requests.post(url, json=body, headers=headers,
                           verify=verify_tls, timeout=timeout)
        if rs.status_code < 400:
            results.append((body['resourceName'], 'created'))
        else:
            results.append((body['resourceName'],
                            'HTTP {}: {}'.format(rs.status_code,
                                                 rs.text[:160])))
    return results


def emit_pdc(events, base_url, username, password, verify_tls=False,
             timeout=30):
    """POST OpenLineage events to Pentaho Data Catalog's ingestion
    endpoint (``/lineage/api/events`` - the same endpoint the official
    PDI OpenLineage plugin targets). PDC builds its ETL Pipelines
    hierarchy and lineage graph from them. Returns events accepted."""
    token = _pdc_token(base_url, username, password,
                       verify_tls=verify_tls, timeout=timeout)
    url = base_url.rstrip('/') + '/lineage/api/events'
    headers = {'Authorization': 'Bearer ' + token}
    for event in events:
        rs = requests.post(url, json=event, headers=headers,
                           verify=verify_tls, timeout=timeout)
        if rs.status_code >= 400:
            raise RuntimeError(
                'PDC rejected event for job {} (HTTP {}): {}'.format(
                    event['job']['name'], rs.status_code, rs.text[:300]))
    return len(events)

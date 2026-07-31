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
import socket
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

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
COLUMN_LINEAGE_URL = ('https://openlineage.io/spec/facets/1-2-0/'
                      'ColumnLineageDatasetFacet.json')
SCHEMA_FACET_URL = ('https://openlineage.io/spec/facets/1-1-1/'
                    'SchemaDatasetFacet.json')


def _dataset(namespace, name):
    return {'namespace': namespace, 'name': name}


def _output_dataset(namespace, name, row_count=None, producer=None,
                    facets=None):
    """Output dataset with an optional real rowCount (from Carte).

    ``producer`` should match the enclosing event's producer - a facet
    claiming a different origin than the event carrying it is
    inconsistent, and a consumer is entitled to ignore it.
    """
    ds = {'namespace': namespace, 'name': name}
    if facets:
        ds['facets'] = facets
    if row_count is not None:
        ds['outputFacets'] = {
            'outputStatistics': {
                '_producer': producer or PRODUCER,
                '_schemaURL': OUTPUT_STATS_URL,
                'rowCount': int(row_count),
            },
        }
    return ds


def _input_dataset(namespace, name, row_count=None, producer=None,
                   facets=None):
    """Input dataset with an optional real rowCount (from Carte)."""
    ds = {'namespace': namespace, 'name': name}
    if facets:
        ds['facets'] = facets
    if row_count is not None:
        ds['inputFacets'] = {
            'dataQualityMetrics': {
                '_producer': producer or PRODUCER,
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
                job_type, event_time=None, rows_in=None, rows_out=None):
    """A START/COMPLETE pair describing one job with its edges.

    ``rows_in`` / ``rows_out`` attach real Carte counts to the edges.
    Left as None the datasets stay bare ``{namespace, name}``, so
    callers without metrics are unaffected.
    """
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
        'inputs': [_input_dataset(namespace, n, rows_in) for n in inputs],
        'outputs': [_output_dataset(namespace, n, rows_out) for n in outputs],
    }
    return [
        dict(base, eventType='START', eventTime=start.isoformat()),
        dict(base, eventType='COMPLETE',
             eventTime=(start + timedelta(seconds=1)).isoformat()),
    ]


def build_job_model_events(doc, namespace='pdi', trans_details=None,
                           step_metrics=None):
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
                entry_input=start_ds, exit_output=result_ds,
                step_metrics=(step_metrics or {}).get(trans_name)))
        else:
            events.extend(_run_events(
                namespace, job_name, inputs, [result_ds], description,
                entry.entry_type))
    return events


def build_trans_model_events(detail, namespace='pdi', prefix=None,
                             entry_input=None, exit_output=None,
                             step_metrics=None):
    """Step-level lineage for a parsed .ktr (PdiTransDetail).

    One OpenLineage job per step; each hop becomes a dataset edge
    (``pdi://<trans>/<step>``), so Marquez draws the step graph.

    ``entry_input`` / ``exit_output`` splice the step graph into a
    surrounding job graph: first steps (no upstream hop) additionally
    read ``entry_input``; terminal steps (no downstream hop)
    additionally write ``exit_output``.

    ``step_metrics`` (from :func:`parse_carte_step_metrics`) puts the
    real Carte counts on the edges, so the graph shows how many rows
    each step actually moved rather than only which steps connect.
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
        m = (step_metrics or {}).get(step.name) or {}
        # 'written' is what the step emitted downstream, 'read' what it
        # consumed from upstream. A source step legitimately reads 0 -
        # it pulls from the database, not across a hop.
        events.extend(_run_events(
            namespace, job_name, inputs, outputs, description, 'STEP',
            rows_in=m.get('read'), rows_out=m.get('written')))
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


# The old placeholder default. Treated as "not chosen" so existing
# settings auto-upgrade to a real hostname rather than silently keeping
# a name that never matches a PDI server.
_PLACEHOLDER_SERVER = 'pdi2dag'
_LOCAL_HOSTS = {'localhost', '127.0.0.1', '::1', ''}


def resolve_server_name(explicit=None, carte_url=None):
    """The PDI server name to use as the OpenLineage namespace.

    The plugin uses the **PDI server hostname** (its Config
    localHostname / HostnameResolver), and PDC turns that namespace into
    the *PDI Server* node its ETL tree hangs from. An arbitrary string
    still produces a valid graph, but one attributed to a server that
    does not exist.

    Order: an explicit name wins; otherwise the Carte host, since that
    *is* the machine running PDI; otherwise this machine's hostname.
    """
    if explicit and explicit != _PLACEHOLDER_SERVER:
        return explicit
    host = ''
    if carte_url:
        host = urlsplit(carte_url).hostname or ''
    if host.lower() not in _LOCAL_HOSTS:
        return host
    # Carte is on this machine, so this machine is the PDI server.
    return socket.gethostname()


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


def _resolve_ref(ref, conns):
    """A columns.py source ref -> (namespace, dataset name, field),
    using the same naming the datasets themselves get - so a facet's
    inputFields point at datasets that actually exist in the event."""
    if ref[0] == 'db':
        _kind, conn_name, table_token, col = ref
        conn = conns.get(conn_name)
        ns = _conn_namespace(conn) if conn else 'jdbc://unknown'
        schema, table = (table_token.split('.', 1) if '.' in table_token
                         else ('', table_token))
        return ns, _fq_dataset_name(conn, schema, table), col
    _kind, path, col = ref
    ds = _file_dataset(path)
    return ds['namespace'], ds['name'], col


def dataset_column_facets(detail, producer=None):
    """Offline column-level lineage as OpenLineage dataset facets.

    Returns ``(column_facets, schema_fields)``:

    * ``column_facets``: {(namespace, name): columnLineage facet} for
      output datasets where at least one column's origin is known.
    * ``schema_fields``: {(namespace, name): [column, ...]} for every
      dataset whose columns the .ktr reveals - an output's written
      columns, an input table's referenced columns, a file input's
      declared fields.

    This is the free-plugin parity feature: the paid PDI OpenLineage
    plugin derives the same facet at runtime with a SQL parser; this
    derives it offline from the .ktr (see ``pdi2dag/columns.py`` for
    what is modelled and where lineage is honestly omitted).
    """
    from pdi2dag.columns import output_column_lineage, select_columns

    producer = producer or PRODUCER
    conns = {c.name: c for c in detail.connections}
    per_step = output_column_lineage(detail)
    column_facets = {}
    schema_fields = {}

    def _remember(key, column):
        cols = schema_fields.setdefault(key, [])
        if column not in cols:
            cols.append(column)

    # inputs: the columns each source table/file provably supplies
    for step in detail.steps:
        if step.step_type == 'TableInput' and step.sql:
            cols = select_columns(step.sql)
            for _out, refs in cols or []:
                for table_token, col in refs:
                    ns, name, field = _resolve_ref(
                        ('db', step.connection, table_token, col), conns)
                    _remember((ns, name), field)
        elif step.is_file_input and step.files:
            for f in (step.column_info or {}).get('fields') or []:
                for path in step.files:
                    ds = _file_dataset(path)
                    _remember((ds['namespace'], ds['name']), f)

    # outputs: written columns + their lineage
    fields_by_key = {}
    for step in detail.steps:
        columns = per_step.get(step.name)
        if not columns:
            continue
        targets = []
        if step.step_type == 'TableOutput' and step.table:
            conn = conns.get(step.connection)
            ns = _conn_namespace(conn) if conn else 'jdbc://unknown'
            targets.append((ns, _fq_dataset_name(conn, step.schema,
                                                 step.table)))
        elif step.is_file_output:
            for f in step.files:
                ds = _file_dataset(f)
                targets.append((ds['namespace'], ds['name']))
        for key in targets:
            fields = fields_by_key.setdefault(key, {})
            for column, refs in columns.items():
                _remember(key, column)
                resolved = sorted(_resolve_ref(r, conns) for r in refs)
                inputs = [{'namespace': ns, 'name': name, 'field': col}
                          for ns, name, col in resolved]
                entry = fields.setdefault(column, {'inputFields': []})
                known = {(i['namespace'], i['name'], i['field'])
                         for i in entry['inputFields']}
                entry['inputFields'].extend(
                    i for i in inputs
                    if (i['namespace'], i['name'], i['field']) not in known)

    for key, fields in fields_by_key.items():
        traced = {c: e for c, e in fields.items() if e['inputFields']}
        if traced:
            column_facets[key] = {
                '_producer': producer,
                '_schemaURL': COLUMN_LINEAGE_URL,
                'fields': traced,
            }
    return column_facets, schema_fields


def _dataset_facets(key, column_facets, schema_fields, producer,
                    is_output):
    """The ``facets`` dict for one dataset, or None."""
    facets = {}
    cols = schema_fields.get(key)
    if cols:
        facets['schema'] = {
            '_producer': producer,
            '_schemaURL': SCHEMA_FACET_URL,
            'fields': [{'name': c} for c in cols],
        }
    if is_output and key in column_facets:
        facets['columnLineage'] = column_facets[key]
    return facets or None


def _spec_datasets(detail):
    """Input/output dataset specs for a DAG's baked lineage: the same
    identities :func:`trans_datasets` resolves, plus the baked
    schema/columnLineage facets and the ``_step``/``_counters`` hints
    the provider's runtime publisher uses to attach that run's real
    Carte row counts (Table Input reads 'input'/'read'; Table Output
    writes 'written'/'output'; file steps per their Carte counters)."""
    conns = {c.name: c for c in detail.connections}
    col_facets, schema_fields = dataset_column_facets(
        detail, producer=PDI_PLUGIN_PRODUCER)
    default_ns = 'jdbc://unknown'
    inputs, outputs = [], []

    def _spec(key, step_name, counters, is_output):
        ds = {'namespace': key[0], 'name': key[1]}
        facets = _dataset_facets(key, col_facets, schema_fields,
                                 PDI_PLUGIN_PRODUCER, is_output)
        if facets:
            ds['facets'] = facets
        ds['_step'] = step_name
        ds['_counters'] = counters
        return ds

    for step in detail.steps:
        if step.step_type == 'TableInput' and step.sql:
            conn = conns.get(step.connection)
            ns = _conn_namespace(conn) if conn else default_ns
            for tok in _tables_from_sql(step.sql):
                schema, table = (tok.split('.', 1) if '.' in tok
                                 else ('', tok))
                key = (ns, _fq_dataset_name(conn, schema, table))
                inputs.append(_spec(key, step.name, ['input', 'read'],
                                    False))
        elif step.step_type == 'TableOutput' and step.table:
            conn = conns.get(step.connection)
            ns = _conn_namespace(conn) if conn else default_ns
            key = (ns, _fq_dataset_name(conn, step.schema, step.table))
            outputs.append(_spec(key, step.name, ['written', 'output'],
                                 True))
        elif step.is_file_input and step.files:
            for f in step.files:
                ds = _file_dataset(f)
                inputs.append(_spec((ds['namespace'], ds['name']),
                                    step.name, ['output', 'read'], False))
        elif step.is_file_output and step.files:
            for f in step.files:
                ds = _file_dataset(f)
                outputs.append(_spec((ds['namespace'], ds['name']),
                                     step.name, ['written', 'input'],
                                     True))
    return inputs, outputs


def build_dag_lineage_spec(doc=None, detail=None, trans_details=None,
                           server_name='pdi2dag'):
    """The lineage spec pdi2dag bakes into a generated DAG.

    The generated DAG's terminal task hands this to the provider's
    ``publish_pdi_lineage``, which stamps run ids/times, merges the
    run's Carte metrics and posts to Marquez/PDC - so a SCHEDULED run
    keeps the catalog current without a manual publish step.

    Give either ``doc`` (a .kjb, with ``trans_details`` mapping
    transformation names to parsed .ktr details) or ``detail`` (a
    standalone .ktr).
    """
    trans_details = trans_details or {}
    spec = {'server_name': server_name}
    if doc is not None and doc.kind == 'job':
        spec['job_name'] = doc.repo_path
        spec['job_type'] = 'job'
        entries = []
        for entry in doc.executable_entries:
            name = entry.path or '{}/{}'.format(
                doc.repo_path.rsplit('/', 1)[0], _sanitize_id(entry.name))
            e = {'name': name,
                 'job_type': ('transformation'
                              if entry.entry_type == TYPE_TRANS
                              else 'job'),
                 'trans': ''}
            if entry.entry_type == TYPE_TRANS:
                tname = (entry.path or '').split('/')[-1]
                d = trans_details.get(tname)
                if d is not None:
                    e['trans'] = d.name or tname
                    e['inputs'], e['outputs'] = _spec_datasets(d)
            entries.append(e)
        spec['entries'] = entries
    elif detail is not None:
        inputs, outputs = _spec_datasets(detail)
        spec['job_name'] = None
        spec['job_type'] = 'transformation'
        spec['entries'] = [{
            'name': '/{}'.format(detail.name),
            'job_type': 'transformation',
            'trans': detail.name,
            'inputs': inputs,
            'outputs': outputs,
        }]
    else:
        spec['job_name'] = None
        spec['entries'] = []
    return spec


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
                   inputs=None, outputs=None, failed=False,
                   facet_maps=None):
        col_facets, schema_fields = facet_maps or ({}, {})

        def ds_facets(d, is_output):
            return _dataset_facets((d['namespace'], d['name']),
                                   col_facets, schema_fields,
                                   PDI_PLUGIN_PRODUCER, is_output)

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
                                      d.get('rowCount'),
                                      producer=PDI_PLUGIN_PRODUCER,
                                      facets=ds_facets(d, False))
                       for d in (inputs or [])],
            'outputs': [_output_dataset(d['namespace'], d['name'],
                                        d.get('rowCount'),
                                        producer=PDI_PLUGIN_PRODUCER,
                                        facets=ds_facets(d, True))
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
        facet_maps = None
        if entry.entry_type == TYPE_TRANS:
            tname = (entry.path or '').split('/')[-1]
            detail = trans_details.get(tname)
            if detail:
                metrics = step_metrics.get(tname)
                inputs, outputs = trans_datasets(detail, metrics)
                facet_maps = dataset_column_facets(
                    detail, producer=PDI_PLUGIN_PRODUCER)
                failed = bool(metrics) and any(
                    m.get('errors') for m in metrics.values())
        events += run_events(
            name, str(uuid.uuid4()), dict(parent_run_facet), job_type,
            now + timedelta(seconds=3 + i), inputs, outputs, failed,
            facet_maps=facet_maps)
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
    col_facets, schema_fields = dataset_column_facets(
        detail, producer=PDI_PLUGIN_PRODUCER)
    failed = bool(step_metrics) and any(
        m.get('errors') for m in (step_metrics or {}).values())
    job = {
        'namespace': ns,
        'name': repo_path,
        'facets': _pdi_job_facets('transformation'),
    }

    def ds_facets(d, is_output):
        return _dataset_facets((d['namespace'], d['name']),
                               col_facets, schema_fields,
                               PDI_PLUGIN_PRODUCER, is_output)

    run_id = str(uuid.uuid4())
    base = {
        'producer': PDI_PLUGIN_PRODUCER,
        'schemaURL': SCHEMA_URL,
        'run': {'runId': run_id, 'facets': {}},
        'job': job,
        'inputs': [_input_dataset(d['namespace'], d['name'],
                                  d.get('rowCount'),
                                  producer=PDI_PLUGIN_PRODUCER,
                                  facets=ds_facets(d, False))
                   for d in inputs],
        'outputs': [_output_dataset(d['namespace'], d['name'],
                                    d.get('rowCount'),
                                    producer=PDI_PLUGIN_PRODUCER,
                                    facets=ds_facets(d, True))
                    for d in outputs],
    }
    return [
        dict(base, eventType='START', eventTime=now.isoformat()),
        dict(base, eventType='FAIL' if failed else 'COMPLETE',
             eventTime=(now + timedelta(seconds=2)).isoformat()),
    ]



def verify_pdc_events(events, base_url, username, password,
                      verify_tls=False, timeout=30, pages=20):
    """Read published events back out of PDC and confirm they stored.

    PDC answers **200 for events it can build nothing from**, so the
    POST status is not evidence of anything. This re-reads the event
    store and matches on (job name, eventType).

    ``/lineage/api/events`` pages at a fixed size regardless of any
    ``limit`` - walk until nothing new appears rather than trusting one
    response, or a stored event will look missing.
    """
    token = _pdc_token(base_url, username, password,
                       verify_tls=verify_tls, timeout=timeout)
    url = base_url.rstrip('/') + '/lineage/api/events'
    headers = {'Authorization': 'Bearer ' + token}
    seen, stored = set(), set()
    for _ in range(pages):
        rs = requests.get(url, headers=headers, verify=verify_tls,
                          timeout=timeout)
        rs.raise_for_status()
        batch = ((rs.json().get('data') or {}).get('events')) or []
        fresh = [e for e in batch if e.get('_id') not in seen]
        if not fresh:
            break
        for e in fresh:
            seen.add(e.get('_id'))
            job = (e.get('job') or {})
            stored.add((job.get('namespace'), job.get('name'),
                        e.get('eventType')))
    missing = [
        {'namespace': e['job']['namespace'], 'job': e['job']['name'],
         'eventType': e['eventType']}
        for e in events
        if (e['job']['namespace'], e['job']['name'], e['eventType'])
        not in stored]
    return {'sent': len(events),
            'confirmed': len(events) - len(missing),
            'missing': missing}


def unmatched_datasets(events, known_namespaces):
    """Datasets whose namespace matches nothing the catalog holds.

    This is the `postgresql://` vs `postgres://` class of bug: the event
    is accepted, stored and well-formed, and lands on a *new* node that
    never joins the graph. Comparing against the namespaces the catalog
    actually uses is the only cheap way to catch it.
    """
    known = {n for n in known_namespaces if n}
    out = []
    for event in events:
        for ds in list(event.get('inputs') or []) +                 list(event.get('outputs') or []):
            ns = ds.get('namespace')
            # 'file' datasets are lineage-only by nature - a local path
            # is not expected to be a catalogued data source.
            if ns and ns != 'file' and ns not in known:
                entry = {'namespace': ns, 'name': ds.get('name')}
                if entry not in out:
                    out.append(entry)
    return out


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

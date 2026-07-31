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
"""Publish PDI-shaped OpenLineage from a *scheduled* DAG run.

pdi2dag bakes a lineage *spec* into every generated DAG: the job's
repository path, its entries, and each entry's datasets - including the
offline-derived ``schema`` and ``columnLineage`` facets. This module is
the runtime half: a terminal task calls :func:`publish_pdi_lineage`,
which stamps fresh run ids and times, enriches the datasets with REAL
row counts from the Carte server that just executed the run (when the
run configuration is Carte), and posts the events to Marquez and/or
Pentaho Data Catalog.

So a migrated pipeline does not need a manual "publish lineage" step:
every scheduled run refreshes the catalog with the numbers of that run.

Backends resolve at runtime, in Airflow:

* Marquez - the ``pdi2dag_marquez_url`` Airflow Variable, else the
  ``marquez_url`` baked by pdi2dag, else skipped.
* PDC - an Airflow connection (default id ``pdc_lineage``): host = the
  PDC base URL, login/password = the publishing account, extras may set
  ``realm``, ``client_id`` and ``verify_tls``. Missing = skipped.

With neither configured the task succeeds and says so - the DAG's own
scheduling is never hostage to the catalog.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone

import requests

try:
    from airflow.sdk import Variable            # Airflow 3.x
except ImportError:                             # pragma: no cover
    from airflow.models import Variable         # Airflow 2.x

from airflow_pentaho.hooks.carte import PentahoCarteHook

log = logging.getLogger(__name__)

# Match the official PDI OpenLineage plugin, so PDC's ETL tree and the
# events any run of the paid plugin would emit line up.
PDI_PLUGIN_PRODUCER = ('https://github.com/pentaho/pdi-plugins-ee/tree/'
                       'pdi-openlineage-plugin-ee/pdi-openlineage-plugin')
SCHEMA_URL = 'https://openlineage.io/spec/2-0-2/OpenLineage.json'
JOB_TYPE_URL = ('https://openlineage.io/spec/facets/2-0-3/'
                'JobTypeJobFacet.json')
PARENT_RUN_URL = ('https://openlineage.io/spec/facets/1-0-1/'
                  'ParentRunFacet.json')
OUTPUT_STATS_URL = ('https://openlineage.io/spec/facets/1-0-2/'
                    'OutputStatisticsOutputDatasetFacet.json')
DQ_METRICS_URL = ('https://openlineage.io/spec/facets/1-0-1/'
                  'DataQualityMetricsInputDatasetFacet.json')


def _job_facets(job_type):
    return {
        'jobType': {
            '_producer': PDI_PLUGIN_PRODUCER,
            '_schemaURL': JOB_TYPE_URL,
            'processingType': 'BATCH',
            'integration': 'PDI',
            'jobType': job_type,
        },
    }


def _step_metrics(trans_status):
    """``{step name: {counter: int}}`` from a Carte transStatus dict
    (the hook already parses the XML with xmltodict)."""
    ts = (trans_status or {}).get('transstatus') or {}
    lst = (ts.get('stepstatuslist') or {}).get('stepstatus') or []
    if isinstance(lst, dict):
        lst = [lst]

    def _int(v):
        try:
            return int(v)
        except (TypeError, ValueError):
            return 0

    out = {}
    for s in lst:
        if s.get('stepname'):
            out[s['stepname']] = {
                'read': _int(s.get('linesRead')),
                'written': _int(s.get('linesWritten')),
                'input': _int(s.get('linesInput')),
                'output': _int(s.get('linesOutput')),
                'errors': _int(s.get('errors')),
            }
    return out


def _dataset(spec_ds, metrics, is_output):
    """A spec dataset -> wire dataset: baked facets kept, runtime row
    count attached when the Carte run reported the hinted step."""
    ds = {'namespace': spec_ds['namespace'], 'name': spec_ds['name']}
    if spec_ds.get('facets'):
        ds['facets'] = spec_ds['facets']
    rows = None
    step = spec_ds.get('_step')
    if step and metrics.get(step):
        for counter in spec_ds.get('_counters') or []:
            if metrics[step].get(counter):
                rows = metrics[step][counter]
                break
    if rows is not None:
        key = 'outputFacets' if is_output else 'inputFacets'
        facet = 'outputStatistics' if is_output else 'dataQualityMetrics'
        url = OUTPUT_STATS_URL if is_output else DQ_METRICS_URL
        ds[key] = {facet: {
            '_producer': PDI_PLUGIN_PRODUCER,
            '_schemaURL': url,
            'rowCount': int(rows),
        }}
    return ds


def build_run_events(spec, metrics_by_trans=None, event_time=None):
    """OpenLineage events for ONE run of the migrated pipeline.

    Fresh run ids and event times each call; datasets from the baked
    spec; row counts and FAIL states from ``metrics_by_trans``
    (transformation name -> step metrics) when Carte supplied them.
    """
    metrics_by_trans = metrics_by_trans or {}
    now = event_time or datetime.now(timezone.utc)
    ns = spec.get('server_name') or 'pdi2dag'
    events = []

    def pair(name, job_type, run_id, run_facets, inputs, outputs,
             start, failed):
        base = {
            'producer': PDI_PLUGIN_PRODUCER,
            'schemaURL': SCHEMA_URL,
            'run': {'runId': run_id, 'facets': run_facets},
            'job': {'namespace': ns, 'name': name,
                    'facets': _job_facets(job_type)},
            'inputs': inputs,
            'outputs': outputs,
        }
        return [
            dict(base, eventType='START', eventTime=start.isoformat()),
            dict(base, eventType='FAIL' if failed else 'COMPLETE',
                 eventTime=(start + timedelta(seconds=2)).isoformat()),
        ]

    entries = spec.get('entries') or []
    job_name = spec.get('job_name')
    parent_facet = {}
    if job_name:
        job_run_id = str(uuid.uuid4())
        any_failed = False
        entry_events = []
        parent_facet = {'parent': {
            '_producer': PDI_PLUGIN_PRODUCER,
            '_schemaURL': PARENT_RUN_URL,
            'run': {'runId': job_run_id},
            'job': {'namespace': ns, 'name': job_name},
            'root': {'run': {'runId': job_run_id},
                     'job': {'namespace': ns, 'name': job_name}},
        }}
        for i, entry in enumerate(entries):
            metrics = metrics_by_trans.get(entry.get('trans') or '', {})
            failed = any(m.get('errors') for m in metrics.values())
            any_failed = any_failed or failed
            entry_events += pair(
                entry['name'], entry.get('job_type', 'transformation'),
                str(uuid.uuid4()), dict(parent_facet),
                [_dataset(d, metrics, False)
                 for d in entry.get('inputs') or []],
                [_dataset(d, metrics, True)
                 for d in entry.get('outputs') or []],
                now + timedelta(seconds=3 + i), failed)
        events += pair(job_name, spec.get('job_type', 'job'), job_run_id,
                       {}, [], [], now, any_failed)
        events += entry_events
    else:
        for i, entry in enumerate(entries):
            metrics = metrics_by_trans.get(entry.get('trans') or '', {})
            failed = any(m.get('errors') for m in metrics.values())
            events += pair(
                entry['name'], entry.get('job_type', 'transformation'),
                str(uuid.uuid4()), {},
                [_dataset(d, metrics, False)
                 for d in entry.get('inputs') or []],
                [_dataset(d, metrics, True)
                 for d in entry.get('outputs') or []],
                now + timedelta(seconds=i), failed)
    return events


def _fetch_carte_metrics(spec, pdi_conn_id):
    """Per-transformation step metrics from the Carte server that just
    ran the DAG's tasks. Best-effort: a status miss (local Kitchen/Pan
    run, recycled Carte, offline server) logs and moves on - the events
    then carry structure without that run's counts."""
    out = {}
    names = sorted({e.get('trans') for e in spec.get('entries') or []
                    if e.get('trans')})
    if not names:
        return out
    try:
        conn = PentahoCarteHook(conn_id=pdi_conn_id).get_conn()
    except Exception as exc:  # noqa: BLE001 - any hook/config error
        log.info('No Carte metrics: %s', exc)
        return out
    for name in names:
        try:
            status = conn.trans_status(name, None)
            out[name] = _step_metrics(status)
        except Exception as exc:  # noqa: BLE001 - per-trans best effort
            log.info("No Carte status for '%s': %s", name, exc)
    return out


def _post_marquez(events, marquez_url, timeout=30):
    url = marquez_url.rstrip('/') + '/api/v1/lineage'
    for event in events:
        rs = requests.post(url, json=event, timeout=timeout)
        rs.raise_for_status()


def _pdc_token(base_url, username, password, realm, client_id,
               verify_tls, timeout=30):
    rs = requests.post(
        '{}/keycloak/realms/{}/protocol/openid-connect/token'.format(
            base_url.rstrip('/'), realm),
        data={'grant_type': 'password', 'client_id': client_id,
              'username': username, 'password': password,
              'scope': 'openid'},
        verify=verify_tls, timeout=timeout)
    rs.raise_for_status()
    return rs.json()['access_token']


def _post_pdc(events, conn, timeout=30):
    """POST events to PDC's lineage ingest using an Airflow connection:
    host = base URL, login/password = account, extras may override
    ``realm`` (pdc), ``client_id`` (pdc-client), ``verify_tls``."""
    extra = conn.extra_dejson if hasattr(conn, 'extra_dejson') else {}
    base = (conn.host or '').rstrip('/')
    if not base.startswith('http'):
        base = 'https://{}'.format(base)
    token = _pdc_token(base, conn.login, conn.password,
                       extra.get('realm', 'pdc'),
                       extra.get('client_id', 'pdc-client'),
                       bool(extra.get('verify_tls', False)),
                       timeout=timeout)
    for event in events:
        rs = requests.post(
            base + '/lineage/api/events', json=event,
            headers={'Authorization': 'Bearer {}'.format(token)},
            verify=bool(extra.get('verify_tls', False)), timeout=timeout)
        rs.raise_for_status()


def publish_pdi_lineage(spec, pdi_conn_id='pdi_default',
                        marquez_url=None, pdc_conn_id='pdc_lineage',
                        **_context):
    """The generated DAG's terminal task: publish this run's lineage.

    Returns a summary dict (lands in XCom): how many events, which
    backends received them, which transformations contributed real
    Carte metrics.
    """
    metrics = _fetch_carte_metrics(spec, pdi_conn_id)
    events = build_run_events(spec, metrics)

    resolved_marquez = None
    try:
        resolved_marquez = Variable.get('pdi2dag_marquez_url')
    except Exception:  # noqa: BLE001 - unset variable
        resolved_marquez = marquez_url
    if resolved_marquez:
        _post_marquez(events, resolved_marquez)

    pdc_host = None
    if pdc_conn_id:
        try:
            conn = PentahoCarteHook.get_connection(pdc_conn_id)
        except Exception:  # noqa: BLE001 - connection not defined
            conn = None
        if conn is not None:
            _post_pdc(events, conn)
            pdc_host = conn.host

    if not resolved_marquez and not pdc_host:
        log.info('No lineage backend configured (Variable '
                 'pdi2dag_marquez_url or connection %r) - nothing '
                 'published.', pdc_conn_id)
    return {
        'events': len(events),
        'marquez': resolved_marquez,
        'pdc': pdc_host,
        'carte_metrics': sorted(metrics),
    }

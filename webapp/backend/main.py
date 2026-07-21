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
"""Migration Studio backend.

FastAPI wrapper around the pdi2dag core. Interactive API docs at
``/docs`` (Swagger UI) and ``/redoc``. Error contract (PDC suite
convention): every error body is ``{"error": msg}`` - never FastAPI's
``detail``. Long work (deploy + wait + activate) runs as a background
job polled via ``GET /api/jobs/{id}``.

Run:  uvicorn main:app --port 5012   (from webapp/backend)
(5000/5010 = PDC-Glossary, 5011 = PDC-Policy, 6001 = Marquez API)
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
import uuid
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlsplit, urlunsplit

import requests
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from pdi2dag import __version__
from pdi2dag.airflow_api import AirflowClient
from pdi2dag.dpapi import protect as dpapi_protect
from pdi2dag.dpapi import unprotect as dpapi_unprotect
from pdi2dag.generator import ConvertOptions, convert
from pdi2dag.lineage import (build_job_model_events,
                             build_pdc_etl_events,
                             build_trans_model_events, emit, emit_pdc)
from pdi2dag.parser import parse_file, parse_trans_detail

ROOT = Path(__file__).resolve().parents[1]          # webapp/
SETTINGS_FILE = ROOT / 'settings.json'
DIST = ROOT / 'frontend' / 'dist'
LOG_DIR = ROOT / 'logs'


def _setup_logging():
    """Rotating file log (webapp/logs/studio.log) + console, so a
    long-running / service install leaves a diagnosable trail."""
    logger = logging.getLogger('studio')
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        '%(asctime)s %(levelname)-7s %(message)s')
    try:
        LOG_DIR.mkdir(exist_ok=True)
        fh = RotatingFileHandler(
            LOG_DIR / 'studio.log', maxBytes=1_000_000, backupCount=5,
            encoding='utf-8')
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    except OSError:  # read-only dir - fall back to console only
        pass
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


log = _setup_logging()

DEFAULT_SETTINGS = {
    'airflow_url': 'http://localhost:8088',
    'airflow_user': 'admin',
    'airflow_password': 'admin',
    'dags_folder': str(
        ROOT.parent / 'workshop' / 'dags' / 'deploy-target'),
    'carte_url': 'http://localhost:8081',
    'carte_user': 'cluster',
    'carte_password': 'cluster',
    'carte_architecture': 'single',   # single | cluster
    'marquez_url': 'http://localhost:6001',
    'marquez_web_url': 'http://localhost:3000',
    'marquez_namespace': 'pdi',
    'pdc_url': 'https://pentaho.io',
    'pdc_user': '',
    'pdc_password': '',
    'pdi_server': 'pdi2dag',
}

TAGS = [
    {'name': 'pdi', 'description':
        'Parse and convert PDI files (.kjb jobs, .ktr transformations).'},
    {'name': 'jobs', 'description':
        'Background jobs - deploy/migrate runs asynchronously; poll '
        'until status leaves "running".'},
    {'name': 'lineage', 'description':
        'Publish PDI structure to Marquez as OpenLineage events.'},
    {'name': 'services', 'description':
        'Connected services (Airflow, Carte/PDI, Marquez, PDC) and '
        'studio settings.'},
]

app = FastAPI(
    title='PDI-Airflow Migration Studio API',
    version=__version__,
    description='Convert Pentaho Data Integration jobs and '
                'transformations into scheduled Apache Airflow DAGs, '
                'deploy them, and publish PDI lineage to Marquez. '
                'Errors always return `{"error": msg}`.',
    openapi_tags=TAGS,
)


log.info('PDI-AirFlow Migration Studio %s starting', __version__)


@app.middleware('http')
async def security_headers(request: Request, call_next):
    """Conservative headers - the Studio is a local same-origin app."""
    resp = await call_next(request)
    resp.headers.setdefault('X-Content-Type-Options', 'nosniff')
    resp.headers.setdefault('X-Frame-Options', 'DENY')
    resp.headers.setdefault('Referrer-Policy', 'no-referrer')
    return resp


@app.exception_handler(Exception)
async def unhandled(request: Request, exc: Exception):
    log.exception('Unhandled error on %s %s',
                  request.method, request.url.path)
    return JSONResponse(status_code=500, content={'error': str(exc)})


@app.exception_handler(RequestValidationError)
async def invalid(request: Request, exc: RequestValidationError):
    return JSONResponse(status_code=422, content={'error': str(exc)})


def _err(msg, status=400):
    return JSONResponse(status_code=status, content={'error': msg})


# ------------------------------------------------------------ models

class PdiFile(BaseModel):
    """A PDI file passed by content - nothing is stored server-side."""
    filename: str = Field(examples=['nightly_etl.kjb'])
    content: str = Field(description='Raw XML of the .kjb/.ktr file')
    repo_path: str = Field(
        '', description="Repository path Carte should run, e.g. "
                        "/CSCU/txn_report. Files are uploaded by content, so "
                        "the folder is not knowable from the upload and the "
                        "file's own <directory> is often stale - set this "
                        "when the object does not live at the repo root.",
        examples=['/CSCU/txn_report'])


class ConvertOptionsModel(BaseModel):
    schedule: str = Field('', description='Cron, empty = manual only',
                          examples=['0 6 * * *'])
    dag_id: str = Field('', description='Override; default = PDI name')
    conn_id: str = 'pdi_default'
    mode: str = Field('auto', description='auto | wrap | explode')
    deferrable: bool = True
    poll_interval: int = 10
    retries: int = 0
    owner: str = 'pdi2dag'
    start_date: str = Field('', description='YYYY-MM-DD; default today')
    level: str = 'Basic'
    params: Dict[str, str] = Field(
        default_factory=dict,
        description='PDI parameters; Airflow macros allowed',
        examples=[{'date': '{{ ds }}'}])


class ConvertRequest(PdiFile):
    options: ConvertOptionsModel = ConvertOptionsModel()


class ConvertResponse(BaseModel):
    dag_id: str
    code: str
    warnings: List[str]


class LineagePublishRequest(BaseModel):
    files: List[PdiFile] = Field(
        description='Jobs and their transformations together - step '
                    'graphs are spliced into the job graph')
    target: str = Field(
        'marquez',
        description='marquez (lab lineage backend) or pdc (Pentaho '
                    'Data Catalog /lineage/api/events ingestion)')


class MigrateRequest(BaseModel):
    dag_id: str
    code: str = Field(description='Generated DAG source')
    activate: bool = Field(True, description='Unpause after parse')
    trigger: bool = Field(False, description='Trigger a first run')
    schedule: str = ''


class SettingsModel(BaseModel):
    """Partial bodies merge server-side (suite convention)."""
    airflow_url: Optional[str] = None
    airflow_user: Optional[str] = None
    airflow_password: Optional[str] = None
    dags_folder: Optional[str] = None
    carte_url: Optional[str] = None
    carte_user: Optional[str] = None
    carte_password: Optional[str] = None
    carte_architecture: Optional[str] = None
    marquez_url: Optional[str] = None
    marquez_web_url: Optional[str] = None
    marquez_namespace: Optional[str] = None
    pdc_url: Optional[str] = None
    pdc_user: Optional[str] = None
    pdc_password: Optional[str] = None
    pdi_server: Optional[str] = None


# ---------------------------------------------------------- settings

# Password fields are stored DPAPI-encrypted at rest (Windows) and
# decrypted for in-process use; see pdi2dag.dpapi.
SECRET_FIELDS = ('airflow_password', 'carte_password', 'pdc_password')


def load_settings():
    settings = dict(DEFAULT_SETTINGS)
    if SETTINGS_FILE.exists():
        try:
            settings.update(json.loads(
                SETTINGS_FILE.read_text(encoding='utf-8')))
        except (OSError, ValueError):
            pass
    for field in SECRET_FIELDS:
        if settings.get(field):
            settings[field] = dpapi_unprotect(settings[field])
    return settings


@app.get('/api/settings', tags=['services'],
         summary='Current studio settings')
def get_settings():
    return load_settings()


@app.post('/api/settings', tags=['services'],
          summary='Update settings (partial merge)')
def post_settings(body: SettingsModel):
    settings = load_settings()
    settings.update({k: v for k, v in body.model_dump().items()
                     if v is not None})
    # Encrypt secrets at rest; the returned dict keeps plaintext for the
    # (local) caller/UI.
    to_write = dict(settings)
    for field in SECRET_FIELDS:
        if to_write.get(field):
            to_write[field] = dpapi_protect(to_write[field])
    SETTINGS_FILE.write_text(
        json.dumps(to_write, indent=2), encoding='utf-8')
    return settings


@app.get('/api/version', tags=['services'], summary='Studio version')
def version():
    return {'version': __version__}


@app.get('/api/browse/folder', tags=['services'],
         summary='Open a native folder picker (Studio runs locally)')
def browse_folder():
    """Pops the OS folder-chooser on the machine running the backend and
    returns the chosen absolute path. Only meaningful when the Studio is
    run locally (the dialog appears on the server host); returns an
    error on a headless host. Runs the dialog in a fresh subprocess so
    Tkinter never touches FastAPI's worker threads."""
    import subprocess
    import sys
    code = (
        "import tkinter as tk\n"
        "from tkinter import filedialog\n"
        "r = tk.Tk(); r.withdraw(); r.attributes('-topmost', True)\n"
        "p = filedialog.askdirectory(title='Select the DAGs folder')\n"
        "r.destroy()\n"
        "print(p or '')\n"
    )
    try:
        out = subprocess.run(
            [sys.executable, '-c', code],
            capture_output=True, text=True, timeout=180)
    except Exception as e:  # noqa: BLE001 - local convenience only
        return _err('Folder picker unavailable: {}'.format(e))
    if out.returncode != 0:
        return _err('Folder picker unavailable on this host '
                    '(no desktop session?).')
    lines = [ln for ln in (out.stdout or '').splitlines() if ln.strip()]
    return {'path': lines[-1].strip() if lines else ''}


# --------------------------------------------------------- pdi routes

def _parse_content(filename, content, repo_path=''):
    suffix = os.path.splitext(filename or 'file.kjb')[1] or '.kjb'
    fd, tmp = tempfile.mkstemp(suffix=suffix)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write(content)
        doc = parse_file(tmp)
    finally:
        os.unlink(tmp)
    doc.source_file = filename
    # The upload carries no folder, so an object in /CSCU parses as if it
    # were at the repo root and Carte would fail to find it. An explicit
    # repo_path is the only reliable source of truth here.
    if repo_path:
        clean = '/' + repo_path.strip('/')
        directory, _, name = clean.rpartition('/')
        doc.directory = directory or '/'
        if name:
            doc.name = name
    return doc


def _parse_trans_content(filename, content):
    fd, tmp = tempfile.mkstemp(suffix='.ktr')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write(content)
        return parse_trans_detail(tmp)
    finally:
        os.unlink(tmp)


def _hop_kind(hop):
    if hop.unconditional:
        return 'unconditional'
    return 'success' if hop.evaluation else 'failure'


@app.post('/api/inspect', tags=['pdi'],
          summary='Parse a PDI file into its orchestration structure')
def inspect(body: PdiFile):
    """Returns kind, repository path, named parameters, entries (for
    jobs) and hops. Step-level detail is used by lineage publishing,
    not by this endpoint."""
    try:
        doc = _parse_content(body.filename, body.content,
                             getattr(body, 'repo_path', ''))
    except ValueError as e:
        return _err(str(e))
    return {
        'kind': doc.kind,
        'name': doc.name,
        'repo_path': doc.repo_path,
        'description': doc.description,
        'parameters': [
            {'name': p.name, 'default': p.default,
             'description': p.description}
            for p in doc.parameters],
        'entries': [
            {'name': e.name, 'type': e.entry_type, 'path': e.path,
             'is_start': e.is_start, 'executable': e.is_executable}
            for e in doc.entries],
        'hops': [
            {'from': h.from_name, 'to': h.to_name,
             'enabled': h.enabled, 'kind': _hop_kind(h)}
            for h in doc.hops],
    }


@app.post('/api/convert', tags=['pdi'], response_model=ConvertResponse,
          summary='Generate an Airflow DAG from a PDI file')
def convert_route(body: ConvertRequest):
    """Jobs explode into one task per TRANS/JOB entry (hops become
    dependencies); transformations become a single Carte task. Review
    the returned warnings - they are the migration TODO list."""
    try:
        doc = _parse_content(body.filename, body.content,
                             getattr(body, 'repo_path', ''))
    except ValueError as e:
        return _err(str(e))
    o = body.options
    options = ConvertOptions(
        schedule=o.schedule or None,
        dag_id=o.dag_id or None,
        conn_id=o.conn_id or 'pdi_default',
        mode=o.mode,
        deferrable=o.deferrable,
        poll_interval=o.poll_interval,
        params=dict(o.params),
        retries=o.retries,
        owner=o.owner,
        start_date=o.start_date or None,
        level=o.level)
    result = convert(doc, options)
    return {'dag_id': result.dag_id, 'code': result.code,
            'warnings': result.warnings}


# ------------------------------------------------------------ lineage

@app.post('/api/lineage/publish', tags=['lineage'],
          summary='Publish PDI structure to Marquez')
def lineage_publish(body: LineagePublishRequest):
    """Emits OpenLineage events: job entries as jobs with hop-derived
    dataset edges; transformation step graphs are spliced into the job
    graph (entry -> steps -> next entry). Unreferenced transformations
    get standalone step graphs."""
    if not body.files:
        return _err('No files supplied')
    settings = load_settings()
    ns = settings['marquez_namespace']

    job_docs, trans_details = [], {}
    for f in body.files:
        try:
            if f.filename.lower().endswith('.kjb'):
                job_docs.append(_parse_content(f.filename, f.content))
            else:
                detail = _parse_trans_content(f.filename, f.content)
                trans_details[detail.name] = detail
        except ValueError as e:
            return _err('{}: {}'.format(f.filename, e))

    events, jobs, steps = [], 0, 0
    referenced = set()
    for doc in job_docs:
        if body.target in ('pdc', 'file'):
            # Plugin-accurate: hostname namespace + repo-path names +
            # ParentRunFacet + table datasets from Table Input/Output.
            events.extend(build_pdc_etl_events(
                doc, trans_details=trans_details,
                server_name=settings.get('pdi_server', 'pdi2dag')))
        else:
            events.extend(build_job_model_events(
                doc, namespace=ns, trans_details=trans_details))
        jobs += len(doc.executable_entries)
        for entry in doc.executable_entries:
            trans_name = (entry.path or '').split('/')[-1]
            if trans_name in trans_details:
                referenced.add(trans_name)
                steps += len(trans_details[trans_name].steps)

    for name, detail in trans_details.items():
        if name not in referenced:
            events.extend(build_trans_model_events(detail, namespace=ns))
            steps += len(detail.steps)

    if body.target == 'file':
        # Return the newline-delimited JSON for PDC's ETL Import action
        ndjson = '\n'.join(json.dumps(e) for e in events) + '\n'
        return JSONResponse(content={'events': len(events), 'jobs': jobs,
                                     'steps': steps, 'target': 'file',
                                     'ndjson': ndjson})

    try:
        if body.target == 'pdc':
            if not settings['pdc_user']:
                return _err('Set pdc_user / pdc_password under Settings '
                            'first')
            emit_pdc(events, settings['pdc_url'],
                     settings['pdc_user'], settings['pdc_password'])
        else:
            emit(events, settings['marquez_url'])
    except (RuntimeError, requests.RequestException) as e:
        return _err(str(e), status=502)
    return {'events': len(events), 'jobs': jobs, 'steps': steps,
            'namespace': ns, 'target': body.target}


class GraphRequest(BaseModel):
    files: List[PdiFile]


def _marquez_states(settings):
    """Best-effort latest-run states from Marquez, keyed by job name."""
    try:
        rs = requests.get(
            '{}/api/v1/namespaces/{}/jobs?limit=200'.format(
                settings['marquez_url'], settings['marquez_namespace']),
            timeout=5)
        if rs.status_code != 200:
            return {}
        return {j.get('name'): (j.get('latestRun') or {}).get('state')
                for j in rs.json().get('jobs', [])}
    except requests.RequestException:
        return {}


@app.post('/api/pdi/graph', tags=['lineage'],
          summary='Hierarchical PDI graph: jobs > transformations > steps')
def pdi_graph(body: GraphRequest):
    """The Marquez-can't-do-this view: a nested graph model of the
    given files - job entries with dependencies, and inside each
    transformation its step graph. Latest run states are overlaid from
    Marquez when present (best effort)."""
    from pdi2dag.generator import _collapse_dependencies, _sanitize_id

    if not body.files:
        return _err('No files supplied')
    settings = load_settings()
    states = _marquez_states(settings)

    job_docs, trans_details = [], {}
    for f in body.files:
        try:
            if f.filename.lower().endswith('.kjb'):
                job_docs.append(_parse_content(f.filename, f.content))
            else:
                detail = _parse_trans_content(f.filename, f.content)
                trans_details[detail.name] = detail
        except ValueError as e:
            return _err('{}: {}'.format(f.filename, e))

    def steps_block(detail, prefix):
        return {
            'nodes': [
                {'id': _sanitize_id(s.name), 'name': s.name,
                 'step_type': s.step_type,
                 'state': states.get('{}.{}'.format(
                     prefix, _sanitize_id(s.name)))}
                for s in detail.steps],
            'edges': [
                {'from': _sanitize_id(h.from_name),
                 'to': _sanitize_id(h.to_name)}
                for h in detail.hops if h.enabled],
        }

    jobs_out, referenced = [], set()
    for doc in job_docs:
        deps, _, _ = _collapse_dependencies(doc)
        entries = []
        for entry in doc.executable_entries:
            slug = _sanitize_id(entry.name)
            trans_name = (entry.path or '').split('/')[-1]
            detail = trans_details.get(trans_name)
            if detail:
                referenced.add(trans_name)
            entries.append({
                'id': slug,
                'name': entry.name,
                'type': entry.entry_type,
                'path': entry.path,
                'deps': [_sanitize_id(u)
                         for u in deps.get(entry.name, [])],
                'state': states.get('{}.{}'.format(doc.name, slug)),
                'steps': (steps_block(detail, detail.name)
                          if detail else None),
            })
        jobs_out.append({'name': doc.name, 'path': doc.repo_path,
                         'entries': entries})

    standalone = [
        {'name': name, 'steps': steps_block(detail, name)}
        for name, detail in trans_details.items()
        if name not in referenced]

    return {'jobs': jobs_out, 'transformations': standalone}


# ------------------------------------------------------- service proxies

@app.get('/api/airflow/status', tags=['services'],
         summary='Airflow reachability and DAG count')
def airflow_status():
    settings = load_settings()
    url = settings['airflow_url']
    reachable, total, api = False, None, None
    try:
        # AirflowClient auto-detects the REST API version - v2 + JWT on
        # Airflow 3.x, v1 + basic auth on 2.x. A hard-coded /api/v1 probe
        # 404s on Airflow 3.3, which read as "offline".
        client = AirflowClient(
            url, settings['airflow_user'], settings['airflow_password'],
            timeout=6)
        data = client._request('GET', '/dags?limit=1')
        reachable = True
        api = client._api
        total = (data or {}).get('total_entries')
    except Exception:
        reachable, total = False, None
    return {'reachable': reachable, 'url': url, 'dag_count': total,
            'api': api}


@app.get('/api/airflow/connections', tags=['services'],
         summary='Carte (pentaho-type) connections defined in Airflow')
def airflow_connections():
    """Lists the Airflow connections of type ``pentaho`` - the Carte
    servers/clusters a generated DAG can target via ``pdi_conn_id``.
    Powers the connection picker on the Configure page. Returns an empty
    list (not an error) when Airflow is unreachable so the picker
    degrades to free text."""
    settings = load_settings()
    url = settings['airflow_url']
    conns = []
    try:
        client = AirflowClient(
            url, settings['airflow_user'], settings['airflow_password'],
            timeout=6)
        data = client._request('GET', '/connections?limit=200')
        for c in (data or {}).get('connections', []):
            if c.get('conn_type') != 'pentaho':
                continue
            cid = c.get('connection_id') or c.get('conn_id')
            if not cid:
                continue
            conns.append({
                'conn_id': cid,
                'host': c.get('host'),
                'port': c.get('port'),
                'source': 'db',
            })
    except Exception:  # noqa: BLE001 - picker falls back to free text
        conns = []
    # The REST API only lists metadata-DB connections; env-var ones like
    # the lab's AIRFLOW_CONN_PDI_DEFAULT don't appear. pdi_default is the
    # provider's default and always works, so always offer it.
    if not any(c['conn_id'] == 'pdi_default' for c in conns):
        conns.insert(0, {'conn_id': 'pdi_default', 'host': None,
                         'port': None, 'source': 'env'})
    return {'connections': conns}


def _pdi_level(job):
    """Human label from the OpenLineage jobType facet: STEP for
    transformation steps, TRANS/JOB for PDI entries, DAG/TASK for
    Airflow-emitted lineage."""
    facets = job.get('facets') or {}
    job_type = (facets.get('jobType') or {}).get('jobType')
    if job_type:
        return job_type
    # Airflow provider lineage: DAGs have no dot, tasks do
    return 'TASK' if '.' in (job.get('name') or '') else 'DAG'


@app.get('/api/pdc/status', tags=['services'],
         summary='Pentaho Data Catalog reachability and auth')
def pdc_status():
    settings = load_settings()
    url = settings.get('pdc_url') or ''
    reachable = False
    authenticated = False
    if url:
        try:
            rs = requests.get(
                url.rstrip('/') + '/keycloak/realms/pdc/'
                '.well-known/openid-configuration',
                verify=False, timeout=5)
            reachable = rs.status_code < 500
        except requests.RequestException:
            reachable = False
        if reachable and settings.get('pdc_user') \
                and settings.get('pdc_password'):
            try:
                from pdi2dag.lineage import _pdc_token
                token = _pdc_token(url, settings['pdc_user'],
                                   settings['pdc_password'],
                                   verify_tls=False, timeout=8)
                authenticated = True
            except Exception:  # noqa: BLE001 - status probe only
                authenticated = False
    # Holding a Keycloak token does NOT mean lineage will publish - PDC
    # can hand out a perfectly good token and still reject every API
    # call at its gateway. Probe the endpoint we actually POST to, or
    # the light says "connected" while every publish 401s. GET it rather
    # than POST: we want reachability, not a junk lineage event. 401/403
    # means the token was refused; anything else (405, 404, 2xx) means we
    # got past auth.
    if authenticated:
        try:
            probe = requests.get(
                url.rstrip('/') + '/lineage/api/events',
                headers={'Authorization': 'Bearer ' + token},
                verify=False, timeout=8)
            lineage_ok = probe.status_code not in (401, 403)
            lineage_detail = 'HTTP {}'.format(probe.status_code)
        except requests.RequestException as e:
            lineage_ok, lineage_detail = False, str(e)[:120]
    else:
        lineage_ok, lineage_detail = False, 'not authenticated'
    return {'reachable': reachable, 'authenticated': authenticated,
            'lineage_ok': lineage_ok, 'lineage_detail': lineage_detail,
            'url': url}


@app.get('/api/carte/status', tags=['services'],
         summary='Carte / PDI reachability and auth')
def carte_status():
    """Probes the Carte server the deployed DAGs delegate to. Carte's
    server-wide status lives at ``/kettle/status/?xml=Y`` behind basic
    auth (default cluster/cluster). HTTP 200 = reachable + authed; 401 =
    reachable but wrong credentials."""
    settings = load_settings()
    url = (settings.get('carte_url') or '').rstrip('/')
    reachable, authenticated = False, False
    if url:
        try:
            rs = requests.get(
                url + '/kettle/status/?xml=Y',
                auth=(settings.get('carte_user') or '',
                      settings.get('carte_password') or ''),
                timeout=5)
            reachable = rs.status_code < 500
            authenticated = rs.status_code == 200
        except requests.RequestException:
            reachable, authenticated = False, False
    return {'reachable': reachable, 'authenticated': authenticated,
            'url': url}


def _marquez_web_url(settings):
    """Marquez UI URL for the 'open the graph' links. The UI and API run
    on the same host, so when the API URL points at a remote host but the
    UI URL is still localhost (the default), follow the API host and keep
    the UI's port/scheme - otherwise the graph link opens the wrong (or a
    dead) local Marquez. An explicitly-set remote UI URL is respected."""
    web = (settings.get('marquez_web_url') or '').strip()
    api = (settings.get('marquez_url') or '').strip()
    if not web:
        return api
    local = ('localhost', '127.0.0.1', '')
    try:
        w, a = urlsplit(web), urlsplit(api)
        if (w.hostname or '') in local and a.hostname \
                and a.hostname not in local:
            netloc = a.hostname + (':%d' % w.port if w.port else '')
            return urlunsplit((w.scheme or a.scheme or 'http', netloc,
                               w.path, w.query, w.fragment))
    except ValueError:
        pass
    return web


@app.get('/api/marquez/status', tags=['services'],
         summary='Marquez reachability and namespace count')
def marquez_status():
    settings = load_settings()
    url = settings.get('marquez_url') or ''
    reachable, count = False, None
    if url:
        try:
            rs = requests.get(
                url.rstrip('/') + '/api/v1/namespaces?limit=1',
                timeout=5)
            reachable = rs.status_code < 400
            if reachable and rs.content:
                count = len(rs.json().get('namespaces', []))
        except requests.RequestException:
            reachable, count = False, None
    return {'reachable': reachable, 'url': url,
            'web_url': _marquez_web_url(settings),
            'namespace_count': count}


@app.get('/api/marquez/jobs', tags=['lineage'],
         summary='Jobs in the configured Marquez namespace')
def marquez_jobs():
    settings = load_settings()
    ns = settings['marquez_namespace']
    try:
        rs = requests.get(
            '{}/api/v1/namespaces/{}/jobs?limit=100'.format(
                settings['marquez_url'], ns),
            timeout=10)
    except requests.RequestException as e:
        return _err('Cannot reach Marquez: {}'.format(e), status=502)
    if rs.status_code == 404:
        jobs = []
    elif rs.status_code >= 400:
        return _err('Marquez returned HTTP {}'.format(rs.status_code),
                    status=502)
    else:
        jobs = rs.json().get('jobs', [])
    return {
        'namespace': ns,
        'marquez_url': _marquez_web_url(settings),
        'jobs': [
            {
                'name': j.get('name'),
                'type': _pdi_level(j),
                'state': (j.get('latestRun') or {}).get('state'),
                'duration_ms': (j.get('latestRun') or {}).get('durationMs'),
                'updated_at': j.get('updatedAt'),
            }
            for j in jobs],
    }


# ------------------------------------------------------------------ jobs

JOBS = {}


def _run_migrate(job, payload: MigrateRequest):
    settings = load_settings()
    try:
        dags_folder = Path(settings['dags_folder'])
        dag_file = dags_folder / '{}.py'.format(payload.dag_id)

        job.update(phase='Writing DAG file', detail=str(dag_file), done=1)
        dags_folder.mkdir(parents=True, exist_ok=True)
        dag_file.write_text(payload.code, encoding='utf-8')
        job['events'].append('Wrote {}'.format(dag_file))

        client = AirflowClient(settings['airflow_url'],
                               settings['airflow_user'],
                               settings['airflow_password'])
        result = {'dag_id': payload.dag_id, 'dag_file': str(dag_file),
                  'activated': False, 'run_id': None}

        if payload.activate or payload.trigger:
            job.update(phase='Waiting for the scheduler to parse the DAG',
                       detail='new files are scanned every 5 minutes',
                       done=2)
            client.wait_for_dag(payload.dag_id)
            job['events'].append('DAG parsed by the scheduler')

        if payload.activate:
            job.update(phase='Activating (unpausing)', done=3)
            client.set_paused(payload.dag_id, False)
            result['activated'] = True
            job['events'].append('DAG unpaused - schedule {} live'.format(
                payload.schedule or 'manual'))

        if payload.trigger:
            job.update(phase='Triggering first run', done=4)
            run = client.trigger_dag_run(payload.dag_id)
            result['run_id'] = run.get('dag_run_id')
            job['events'].append('Triggered run {}'.format(
                result['run_id']))

        job.update(status='done', phase='Done', detail='',
                   done=job['total'], result=result)
    except Exception as e:  # noqa: BLE001 - job reports its own failure
        job.update(status='error', detail=str(e))


@app.post('/api/jobs/migrate', tags=['jobs'],
          summary='Start a deploy/activate/trigger job for one DAG')
def start_migrate(payload: MigrateRequest):
    """Returns immediately with the job dict; poll GET /api/jobs/{id}
    until status is 'done' or 'error'. The wait-for-parse phase can
    take up to one scheduler scan cycle (~5 minutes) for new files."""
    job = {
        'id': uuid.uuid4().hex[:12],
        'status': 'running',
        'phase': 'Starting',
        'detail': '',
        'done': 0,
        'total': 5,
        'events': [],
        'result': None,
    }
    JOBS[job['id']] = job
    threading.Thread(target=_run_migrate, args=(job, payload),
                     daemon=True).start()
    return job


@app.get('/api/jobs/{job_id}', tags=['jobs'],
         summary='Poll a background job')
def get_job(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        return _err('Unknown job', status=404)
    return job


# ------------------------------------------------------------- static UI

class _NoCacheIndex(StaticFiles):
    """Serve the built UI, but never let index.html be cached.

    Vite fingerprints the bundles (index-<hash>.js), so those are safe to
    cache hard - but index.html is what *points* at them. Cached, the
    browser keeps loading yesterday's bundle after a rebuild and the UI
    silently runs stale code. No-cache on the entry point only: the
    hashed assets stay immutable, so this costs one small conditional
    request per load, not the bundle.
    """

    async def get_response(self, path, scope):
        response = await super().get_response(path, scope)
        # Normalise: on Windows this arrives as 'assets\\index-<hash>.js'.
        rel = str(path).replace('\\', '/').lstrip('./')
        if rel in ('', 'index.html') or rel.endswith('.html'):
            response.headers['Cache-Control'] = \
                'no-cache, no-store, must-revalidate'
        elif rel.startswith('assets/'):
            response.headers['Cache-Control'] = \
                'public, max-age=31536000, immutable'
        return response


if DIST.exists():
    app.mount('/', _NoCacheIndex(directory=str(DIST), html=True),
              name='frontend')

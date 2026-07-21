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
"""Read run metrics back off a Carte server.

Lineage is far more useful with real numbers attached - "read 17 rows"
is a fact you can reconcile against the catalog's profiled row count,
where a bare edge is only a claim about shape. This pulls the per-step
counts from Carte's ``transStatus`` so they can ride along as
OpenLineage facets.

**Never pick a run by list position.** Carte's ``/kettle/status/``
returns transformations in no guaranteed order, and re-running one
leaves several entries sharing a ``transname``. Taking the last element
silently reports on an *earlier* run - which is exactly how a fixed
pipeline can appear to still be broken. Runs are selected by newest
parsed ``logdate`` here; when you control the execution, diffing run ids
around it (see ``scripts/carte_run.py``) is stronger still.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime

import requests

STATUS_ENDPOINT = '/kettle/status/'
TRANS_STATUS_ENDPOINT = '/kettle/transStatus/'

# Carte renders log dates as 'yyyy/MM/dd HH:mm:ss.SSS'.
_LOGDATE_FORMATS = ('%Y/%m/%d %H:%M:%S.%f', '%Y/%m/%d %H:%M:%S')


def _parse_logdate(value):
    for fmt in _LOGDATE_FORMATS:
        try:
            return datetime.strptime((value or '').strip(), fmt)
        except (ValueError, AttributeError):
            continue
    return None


def _run_started(node):
    """Start time of a ``transstatus`` entry.

    Carte spells this ``log_date``; ``logdate`` appears in some versions
    and in much of the documentation. Read both - looking for only one
    means every entry parses as undated and selection quietly falls back
    to whatever came first, which defeats the point of choosing by time.
    """
    return (_parse_logdate(node.findtext('log_date'))
            or _parse_logdate(node.findtext('logdate')))


def latest_run_id(base_url, trans_name, auth=None, timeout=15):
    """Id of the most recent run of ``trans_name``, or None.

    Chosen by newest ``logdate``, never by position in the response.
    """
    rs = requests.get(base_url.rstrip('/') + STATUS_ENDPOINT,
                      params={'xml': 'Y'}, auth=auth, timeout=timeout)
    rs.raise_for_status()
    root = ET.fromstring(rs.content)
    best, best_when = None, None
    for node in root.iter('transstatus'):
        if node.findtext('transname') != trans_name:
            continue
        when = _run_started(node)
        # An unparseable date must not beat a real one, but a lone
        # undated entry is still better than returning nothing.
        if best is None or (when and (best_when is None or when > best_when)):
            best, best_when = node.findtext('id'), when or best_when
    return best


def fetch_step_metrics(base_url, trans_name, auth=None, timeout=15):
    """Per-step metrics for the latest run of ``trans_name``.

    Returns ``{step_name: {read, written, input, output, errors, ...}}``,
    or ``{}`` when the transformation has never run on this server.
    """
    from pdi2dag.lineage import parse_carte_step_metrics
    run_id = latest_run_id(base_url, trans_name, auth=auth, timeout=timeout)
    if not run_id:
        return {}
    rs = requests.get(base_url.rstrip('/') + TRANS_STATUS_ENDPOINT,
                      params={'name': trans_name, 'id': run_id, 'xml': 'Y'},
                      auth=auth, timeout=timeout)
    rs.raise_for_status()
    import xmltodict
    return parse_carte_step_metrics(xmltodict.parse(rs.content))

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
"""Reconcile what the catalog profiled against what the pipeline moved.

Two independent measurements of the same table:

- **PDC** profiled it and recorded a row count.
- **Carte** actually read it during a run and reported rows read.

Agreement is a real (if modest) data-quality signal; disagreement is
worth investigating - a filtered query, a partial load, a stale profile.
Neither PDC's nor Marquez's UI puts these two numbers side by side, so
this does the comparison explicitly rather than leaving it to the eye.

The comparison is deliberately conservative: a table PDC has never
profiled, or a transformation Carte has never run, is reported as
**unknown** rather than being scored as a match or a mismatch. A missing
measurement is not evidence of agreement.
"""

from __future__ import annotations

MATCH = 'match'
MISMATCH = 'mismatch'
UNKNOWN = 'unknown'


def pdc_table_row_counts(entities):
    """``{(database, schema, table): rows}`` from PDC TABLE entities.

    Takes the payload of ``POST /api/public/v2/entities/filter`` with
    ``types: ['TABLE']``; row counts live in ``metadata.stats.rows`` and
    are absent until the table has been profiled.
    """
    out = {}
    for ent in entities or []:
        if ent.get('type') != 'TABLE':
            continue
        table = ent.get('name')
        meta = ent.get('metadata') or {}
        tbl = meta.get('table') or {}
        rows = (meta.get('stats') or {}).get('rows')
        database = tbl.get('databaseName') or ''
        schema = tbl.get('schemaName') or ''
        if table:
            out[(database, schema, table)] = rows
    return out


def _split_dataset_name(name):
    """``database.schema.table`` -> its parts, tolerating shorter forms."""
    parts = (name or '').split('.')
    if len(parts) >= 3:
        return parts[0], parts[1], '.'.join(parts[2:])
    if len(parts) == 2:
        return '', parts[0], parts[1]
    return '', '', parts[0] if parts else ''


def reconcile_inputs(inputs, pdc_counts):
    """Compare each input dataset's Carte rowCount with PDC's profile.

    ``inputs`` are OpenLineage input datasets as emitted (the Carte
    count lives in ``inputFacets.dataQualityMetrics.rowCount``).
    Returns one row per dataset, ready to render.
    """
    results = []
    for ds in inputs or []:
        name = ds.get('name', '')
        carte = ((ds.get('inputFacets') or {})
                 .get('dataQualityMetrics') or {}).get('rowCount')
        database, schema, table = _split_dataset_name(name)
        profiled = pdc_counts.get((database, schema, table))
        if profiled is None:
            # Fall back to matching on table name alone - a database or
            # schema recorded differently on either side should not be
            # reported as "PDC has never seen this".
            candidates = [v for (d, s, t), v in pdc_counts.items()
                          if t == table]
            profiled = candidates[0] if len(candidates) == 1 else None

        if carte is None or profiled is None:
            status, detail = UNKNOWN, _why_unknown(carte, profiled)
        elif int(carte) == int(profiled):
            status, detail = MATCH, 'both report {} rows'.format(carte)
        else:
            status = MISMATCH
            detail = ('PDC profiled {} rows, the pipeline read {} '
                      '(difference {})'.format(
                          profiled, carte, int(carte) - int(profiled)))
        results.append({
            'dataset': name,
            'namespace': ds.get('namespace', ''),
            'pdc_rows': profiled,
            'carte_rows': carte,
            'status': status,
            'detail': detail,
        })
    return results


def _why_unknown(carte, profiled):
    if carte is None and profiled is None:
        return 'no Carte run and no PDC profile for this dataset'
    if carte is None:
        return ('PDC profiled {} rows, but this transformation has not '
                'run on Carte'.format(profiled))
    return ('the pipeline read {} rows, but PDC has not profiled this '
            'table'.format(carte))

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
"""The generated DAG's per-run lineage publisher."""

from unittest import mock

from airflow_pentaho.lineage import (_step_metrics, build_run_events,
                                     publish_pdi_lineage)

SPEC = {
    'server_name': 'cscu-pdi',
    'job_name': '/cscu/cscu_daily_load',
    'job_type': 'job',
    'entries': [{
        'name': '/cscu/extract_transactions',
        'job_type': 'transformation',
        'trans': 'extract_transactions',
        'inputs': [{
            'namespace': 'postgres://192.168.1.200:5433',
            'name': 'cscu_core.cscu_core.transactions',
            'facets': {'schema': {'fields': [{'name': 'txn_id'}]}},
            '_step': 'Read Transactions',
            '_counters': ['input', 'read'],
        }],
        'outputs': [{
            'namespace': 'postgres://192.168.1.200:5433',
            'name': 'cscu_mart.staging.txn_stg',
            'facets': {'columnLineage': {'fields': {}}},
            '_step': 'Load Staging',
            '_counters': ['written', 'output'],
        }],
    }],
}

STATUS = {'transstatus': {'stepstatuslist': {'stepstatus': [
    {'stepname': 'Read Transactions', 'linesInput': '17',
     'linesRead': '0', 'linesWritten': '17', 'linesOutput': '0',
     'errors': '0'},
    {'stepname': 'Load Staging', 'linesInput': '17', 'linesRead': '17',
     'linesWritten': '17', 'linesOutput': '0', 'errors': '0'},
]}}}


class TestBuildRunEvents:
    def test_fresh_run_ids_and_parent_linking(self):
        events = build_run_events(SPEC)
        # job START/COMPLETE + entry START/COMPLETE
        assert [e['eventType'] for e in events] == [
            'START', 'COMPLETE', 'START', 'COMPLETE']
        job_run = events[0]['run']['runId']
        entry = events[2]
        parent = entry['run']['facets']['parent']
        assert parent['run']['runId'] == job_run
        assert parent['job']['name'] == '/cscu/cscu_daily_load'
        # a second call is a NEW run
        assert build_run_events(SPEC)[0]['run']['runId'] != job_run

    def test_carte_metrics_attach_and_hints_stay_private(self):
        metrics = {'extract_transactions': _step_metrics(STATUS)}
        events = build_run_events(SPEC, metrics)
        out = events[2]['outputs'][0]
        assert out['outputFacets']['outputStatistics']['rowCount'] == 17
        assert '_step' not in out and '_counters' not in out
        # baked facets ride along untouched
        assert 'columnLineage' in out['facets']
        ins = events[2]['inputs'][0]
        assert ins['inputFacets']['dataQualityMetrics']['rowCount'] == 17

    def test_step_errors_mark_the_run_failed(self):
        bad = {'transstatus': {'stepstatuslist': {'stepstatus': [
            {'stepname': 'Load Staging', 'errors': '3'}]}}}
        metrics = {'extract_transactions': _step_metrics(bad)}
        events = build_run_events(SPEC, metrics)
        assert events[1]['eventType'] == 'FAIL'      # job
        assert events[3]['eventType'] == 'FAIL'      # entry

    def test_without_metrics_events_carry_structure_only(self):
        events = build_run_events(SPEC, {})
        assert 'outputFacets' not in events[2]['outputs'][0]


class TestPublish:
    @mock.patch('airflow_pentaho.lineage._fetch_carte_metrics',
                return_value={})
    @mock.patch('airflow_pentaho.lineage.Variable')
    @mock.patch('airflow_pentaho.lineage.requests.post')
    def test_marquez_receives_every_event(self, post, variable, _fetch):
        variable.get.side_effect = Exception('unset')
        post.return_value.status_code = 200
        summary = publish_pdi_lineage(
            SPEC, marquez_url='http://marquez:5000', pdc_conn_id=None)
        assert summary['events'] == 4
        assert post.call_count == 4
        assert all(c.args[0] == 'http://marquez:5000/api/v1/lineage'
                   for c in post.call_args_list)

    @mock.patch('airflow_pentaho.lineage._fetch_carte_metrics',
                return_value={})
    @mock.patch('airflow_pentaho.lineage.Variable')
    @mock.patch('airflow_pentaho.lineage.requests.post')
    def test_no_backend_is_a_quiet_success(self, post, variable, _fetch):
        variable.get.side_effect = Exception('unset')
        summary = publish_pdi_lineage(SPEC, marquez_url=None,
                                      pdc_conn_id=None)
        assert summary == {'events': 4, 'marquez': None, 'pdc': None,
                           'carte_metrics': []}
        assert post.call_count == 0

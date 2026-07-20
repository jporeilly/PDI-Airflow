# -*- coding: utf-8 -*-
"""Unit tests for the OpenLineage model emitter."""

import os

import pytest

from pdi2dag.lineage import build_job_model_events, build_trans_model_events
from pdi2dag.parser import parse_file, parse_trans_detail

SAMPLES = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                       'samples')
KTR_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                       'lab', 'docker', 'carte', 'repository',
                       'home', 'bi')


class TestJobModelEvents:

    @pytest.fixture
    def events(self):
        doc = parse_file(os.path.join(SAMPLES, 'nightly_etl.kjb'))
        return build_job_model_events(doc, namespace='pdi')

    def test_one_start_complete_pair_per_entry(self, events):
        assert len(events) == 8  # 4 executable entries x 2 events
        assert {e['eventType'] for e in events} == {'START', 'COMPLETE'}

    def test_hops_become_dataset_edges(self, events):
        by_job = {e['job']['name']: e for e in events
                  if e['eventType'] == 'COMPLETE'}
        load = by_job['nightly_etl.Load_Warehouse']
        inputs = {d['name'] for d in load['inputs']}
        assert inputs == {'pdi://nightly_etl/Extract_Sales',
                          'pdi://nightly_etl/Extract_Customers'}
        assert load['outputs'][0]['name'] == \
            'pdi://nightly_etl/Load_Warehouse'
        publish = by_job['nightly_etl.Publish_Reports']
        assert {d['name'] for d in publish['inputs']} == \
            {'pdi://nightly_etl/Load_Warehouse'}

    def test_job_facets(self, events):
        job = events[0]['job']
        assert job['namespace'] == 'pdi'
        assert job['facets']['jobType']['integration'] == 'PENTAHO'
        assert 'description' in job['facets']['documentation']


class TestSplicedGraph:
    """Steps spliced into the job graph: entry -> steps -> next entry."""

    @pytest.fixture
    def events(self):
        doc = parse_file(os.path.join(SAMPLES, 'nightly_etl.kjb'))
        detail = parse_trans_detail(
            os.path.join(KTR_DIR, 'extract_sales.ktr'))
        return build_job_model_events(
            doc, namespace='pdi',
            trans_details={'extract_sales': detail})

    def test_entry_feeds_first_step(self, events):
        by_job = {e['job']['name']: e for e in events
                  if e['eventType'] == 'COMPLETE'}
        entry = by_job['nightly_etl.Extract_Sales']
        assert entry['outputs'][0]['name'] == \
            'pdi://nightly_etl/Extract_Sales/start'
        first_step = by_job['extract_sales.Get_Variables']
        assert 'pdi://nightly_etl/Extract_Sales/start' in \
            {d['name'] for d in first_step['inputs']}

    def test_terminal_step_feeds_downstream_entry(self, events):
        by_job = {e['job']['name']: e for e in events
                  if e['eventType'] == 'COMPLETE'}
        last_step = by_job['extract_sales.Write_to_log']
        assert 'pdi://nightly_etl/Extract_Sales' in \
            {d['name'] for d in last_step['outputs']}
        load = by_job['nightly_etl.Load_Warehouse']
        assert 'pdi://nightly_etl/Extract_Sales' in \
            {d['name'] for d in load['inputs']}

    def test_entries_without_detail_unchanged(self, events):
        by_job = {e['job']['name']: e for e in events
                  if e['eventType'] == 'COMPLETE'}
        customers = by_job['nightly_etl.Extract_Customers']
        assert customers['outputs'][0]['name'] == \
            'pdi://nightly_etl/Extract_Customers'


class TestTransModelEvents:

    def test_step_graph(self):
        detail = parse_trans_detail(
            os.path.join(KTR_DIR, 'extract_sales.ktr'))
        assert detail.name == 'extract_sales'
        assert [s.name for s in detail.steps] == \
            ['Get Variables', 'Write to log']

        events = build_trans_model_events(detail, namespace='pdi')
        assert len(events) == 4  # 2 steps x 2 events
        by_job = {e['job']['name']: e for e in events
                  if e['eventType'] == 'COMPLETE'}
        write = by_job['extract_sales.Write_to_log']
        assert {d['name'] for d in write['inputs']} == \
            {'pdi://extract_sales/Get_Variables'}
        assert write['job']['facets']['jobType']['jobType'] == 'STEP'

    def test_ktr_only_file_rejected_as_job(self):
        with pytest.raises(ValueError, match='not a transformation'):
            parse_trans_detail(os.path.join(SAMPLES, 'nightly_etl.kjb'))

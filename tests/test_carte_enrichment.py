# -*- coding: utf-8 -*-
"""Carte runtime enrichment: step metrics -> dataset rowCount facets."""

import os

import xmltodict

from pdi2dag.lineage import (build_pdc_trans_events, parse_carte_step_metrics,
                             trans_datasets)
from pdi2dag.parser import parse_trans_detail

CSCU = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                    'samples', 'cscu')

# A Carte transStatus for extract_enrollment (Read Enrollment ->
# Load Staging), as xmltodict would parse the REST XML.
TRANS_STATUS_XML = """<?xml version="1.0"?>
<transstatus>
  <transname>extract_enrollment</transname>
  <id>abc-123</id>
  <status_desc>Finished</status_desc>
  <stepstatuslist>
    <stepstatus>
      <stepname>Read Enrollment</stepname>
      <linesRead>0</linesRead><linesWritten>4820</linesWritten>
      <linesInput>4820</linesInput><linesOutput>0</linesOutput>
      <linesUpdated>0</linesUpdated><linesRejected>0</linesRejected>
      <errors>0</errors><seconds>1.2</seconds>
    </stepstatus>
    <stepstatus>
      <stepname>Load Staging</stepname>
      <linesRead>4820</linesRead><linesWritten>4820</linesWritten>
      <linesInput>0</linesInput><linesOutput>4820</linesOutput>
      <linesUpdated>0</linesUpdated><linesRejected>0</linesRejected>
      <errors>0</errors><seconds>0.9</seconds>
    </stepstatus>
  </stepstatuslist>
</transstatus>"""


def _metrics():
    return parse_carte_step_metrics(xmltodict.parse(TRANS_STATUS_XML))


class TestParseMetrics:

    def test_per_step_metrics(self):
        m = _metrics()
        assert m['Read Enrollment']['input'] == 4820
        assert m['Load Staging']['written'] == 4820
        assert m['Load Staging']['errors'] == 0

    def test_single_step_not_a_list(self):
        one = """<transstatus><stepstatuslist><stepstatus>
          <stepname>Only</stepname><linesWritten>5</linesWritten>
          </stepstatus></stepstatuslist></transstatus>"""
        m = parse_carte_step_metrics(xmltodict.parse(one))
        assert m['Only']['written'] == 5

    def test_empty_status_safe(self):
        assert parse_carte_step_metrics({}) == {}
        assert parse_carte_step_metrics({'transstatus': {}}) == {}


class TestDatasetRowCounts:

    def test_row_counts_attached(self):
        detail = parse_trans_detail(
            os.path.join(CSCU, 'extract_enrollment.ktr'))
        ins, outs = trans_datasets(detail, _metrics())
        assert ins[0]['name'] == 'cscu.registrar.enrollment'
        assert ins[0]['rowCount'] == 4820
        assert outs[0]['name'] == 'cscu_dw.staging.enrollment_stg'
        assert outs[0]['rowCount'] == 4820

    def test_no_metrics_no_rowcount(self):
        detail = parse_trans_detail(
            os.path.join(CSCU, 'extract_enrollment.ktr'))
        ins, outs = trans_datasets(detail)
        assert 'rowCount' not in ins[0]
        assert 'rowCount' not in outs[0]


class TestEnrichedEvents:

    def test_output_statistics_facet(self):
        detail = parse_trans_detail(
            os.path.join(CSCU, 'extract_enrollment.ktr'))
        events = build_pdc_trans_events(
            detail, '/home/cscu/etl/extract_enrollment',
            step_metrics=_metrics())
        complete = [e for e in events if e['eventType'] == 'COMPLETE'][0]
        out = complete['outputs'][0]
        assert out['outputFacets']['outputStatistics']['rowCount'] == 4820
        inp = complete['inputs'][0]
        assert inp['inputFacets']['dataQualityMetrics']['rowCount'] == 4820

    def test_errors_produce_fail_event(self):
        detail = parse_trans_detail(
            os.path.join(CSCU, 'extract_enrollment.ktr'))
        bad = {'Read Enrollment': {'input': 10, 'errors': 3}}
        events = build_pdc_trans_events(
            detail, '/home/cscu/etl/extract_enrollment', step_metrics=bad)
        assert any(e['eventType'] == 'FAIL' for e in events)

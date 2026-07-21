# -*- coding: utf-8 -*-
"""Contract tests for the PDC-shaped OpenLineage emitter.

Every assertion here corresponds to a bug that reached a live PDC and
was found by hand rather than by the suite. They pin the *wire format*,
because that is what a catalog matches on - a dataset whose namespace is
one character off is stored as a separate node and silently never joins
the graph, while PDC still answers HTTP 200.
"""

import os

from pdi2dag.lineage import (PDI_PLUGIN_PRODUCER, _file_dataset,
                             build_pdc_trans_events, lineage_warnings,
                             resolve_server_name, trans_datasets)
from pdi2dag.parser import PdiConnection, parse_trans_detail

SAMPLES = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                       'samples')
PIPELINES = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                         'lab', 'carte', 'pipelines', 'CSCU')

# Exactly what Carte reports for a txn_report run - the emitter reads
# specific keys ('input' for Table Input, 'written' for a file output),
# so a simplified fixture silently tests nothing.
CARTE_RUN = {
    'cscu_transactions': {'read': 0, 'written': 17, 'input': 17,
                          'output': 0, 'errors': 0},
    'Write to log': {'read': 17, 'written': 17, 'input': 0,
                     'output': 0, 'errors': 0},
    'Text file output': {'read': 17, 'written': 17, 'input': 0,
                         'output': 18, 'errors': 0},
}

CSCU_CORE = PdiConnection(name='cscu-core', db_type='POSTGRESQL',
                          server='192.168.1.200', port='5433',
                          database='cscu_core', username='pdc_user')


def _txn_report():
    return parse_trans_detail(
        os.path.join(PIPELINES, 'txn_report.ktr'),
        shared_connections={'cscu-core': CSCU_CORE})


class TestDatasetNaming:
    """The catalog matches on namespace + name, exactly."""

    def test_postgres_scheme_not_postgresql(self):
        # PDC catalogues PostgreSQL as postgres://host:port. Emitting
        # postgresql:// created a lookalike node that never attached.
        ins, _ = trans_datasets(_txn_report())
        assert ins[0]['namespace'] == 'postgres://192.168.1.200:5433'

    def test_name_is_database_schema_table(self):
        ins, _ = trans_datasets(_txn_report())
        assert ins[0]['name'] == 'cscu_core.cscu_core.transactions'

    def test_shared_connection_resolves_namespace(self):
        # Without shared.xml the connection is unknown and the dataset
        # degrades to jdbc://unknown, which matches nothing.
        detail = parse_trans_detail(
            os.path.join(PIPELINES, 'txn_report.ktr'))
        ins, _ = trans_datasets(detail)
        assert ins[0]['namespace'] == 'jdbc://unknown'

    def test_pvfs_unwraps_to_bucket(self):
        # pvfs://<connection>/<bucket>/<key> is a PDI alias; lineage must
        # describe the storage or it cannot match the catalogued bucket.
        d = _file_dataset('pvfs://cscu-minio/cscu-documents/feeds/x.csv')
        assert d == {'namespace': 's3://cscu-documents',
                     'name': 'feeds/x.csv'}


class TestEventShape:

    def test_job_name_is_repo_path(self):
        e = build_pdc_trans_events(_txn_report(), '/CSCU/txn_report')[0]
        assert e['job']['name'] == '/CSCU/txn_report'

    def test_namespace_is_the_server_name(self):
        # PDC turns this into the "PDI Server" node.
        e = build_pdc_trans_events(_txn_report(), '/CSCU/txn_report',
                                   server_name='Office')[0]
        assert e['job']['namespace'] == 'Office'

    def test_jobtype_facet_matches_plugin(self):
        e = build_pdc_trans_events(_txn_report(), '/CSCU/txn_report')[0]
        facet = e['job']['facets']['jobType']
        assert facet['integration'] == 'PDI'
        assert facet['processingType'] == 'BATCH'
        assert facet['jobType'] == 'transformation'   # lowercase

    def test_start_then_complete(self):
        events = build_pdc_trans_events(_txn_report(), '/CSCU/txn_report')
        assert [e['eventType'] for e in events] == ['START', 'COMPLETE']

    def test_errors_produce_fail(self):
        metrics = {'cscu_transactions': {'read': 0, 'written': 0,
                                         'errors': 3}}
        events = build_pdc_trans_events(
            _txn_report(), '/CSCU/txn_report', step_metrics=metrics)
        assert events[-1]['eventType'] == 'FAIL'


class TestProducerConsistency:
    """A facet claiming a different origin than its own event is
    inconsistent, and a consumer may ignore it. This shipped broken
    twice - once unfixed, once 'fixed' on the wrong builder."""

    def test_standalone_transformation_producers_agree(self):
        e = build_pdc_trans_events(
            _txn_report(), '/CSCU/txn_report',
            step_metrics=CARTE_RUN)[-1]
        assert e['producer'] == PDI_PLUGIN_PRODUCER
        for ds in e['inputs']:
            facet = ds['inputFacets']['dataQualityMetrics']
            assert facet['_producer'] == PDI_PLUGIN_PRODUCER
        for ds in e['outputs']:
            facet = ds['outputFacets']['outputStatistics']
            assert facet['_producer'] == PDI_PLUGIN_PRODUCER


class TestRowCounts:

    def test_counts_attach_to_both_ends(self):
        e = build_pdc_trans_events(
            _txn_report(), '/CSCU/txn_report',
            step_metrics=CARTE_RUN)[-1]
        assert (e['inputs'][0]['inputFacets']['dataQualityMetrics']
                ['rowCount']) == 17
        assert (e['outputs'][0]['outputFacets']['outputStatistics']
                ['rowCount']) == 17

    def test_file_output_counts_rows_not_lines(self):
        # Carte's linesOutput for a text file is 18 - the 17 rows plus
        # the header. The dataset must report rows written (17), or the
        # count will never reconcile against a profiled table.
        _, outs = trans_datasets(_txn_report(), CARTE_RUN)
        assert CARTE_RUN['Text file output']['output'] == 18
        assert outs[0]['rowCount'] == 17

    def test_no_metrics_means_bare_datasets(self):
        e = build_pdc_trans_events(_txn_report(), '/CSCU/txn_report')[-1]
        assert 'inputFacets' not in e['inputs'][0]


class TestServerName:

    def test_explicit_wins(self):
        assert resolve_server_name('my-box', 'http://carte:8081') \
            == 'my-box'

    def test_placeholder_is_not_explicit(self):
        # The old default must not survive an upgrade - it names no
        # real PDI server.
        assert resolve_server_name('pdi2dag', 'http://carte-host:8081') \
            == 'carte-host'

    def test_localhost_falls_through_to_hostname(self):
        import socket
        assert resolve_server_name('', 'http://localhost:8081') \
            == socket.gethostname()


class TestLineageWarnings:
    """PDC answers 200 for events it draws nothing from, so the warning
    is the only signal the user gets."""

    def test_no_output_is_warned(self):
        detail = parse_trans_detail(
            os.path.join(PIPELINES, 'ingest_from_minio.ktr'))
        warnings = lineage_warnings(detail)
        assert warnings and 'no dataset' in warnings[0]

    def test_complete_pipeline_is_silent(self):
        assert lineage_warnings(_txn_report()) == []


class TestReconcile:
    """A missing measurement must never be scored as agreement."""

    def _pdc(self, rows):
        from pdi2dag.reconcile import pdc_table_row_counts
        return pdc_table_row_counts([{
            'type': 'TABLE', 'name': 'transactions',
            'metadata': {'stats': {'rows': rows},
                         'table': {'databaseName': 'cscu_core',
                                   'schemaName': 'cscu_core'}}}])

    def _carte(self, rows):
        from pdi2dag.lineage import _input_dataset
        return [_input_dataset('postgres://192.168.1.200:5433',
                               'cscu_core.cscu_core.transactions', rows)]

    def test_equal_counts_match(self):
        from pdi2dag.reconcile import reconcile_inputs
        r = reconcile_inputs(self._carte(17), self._pdc(17))[0]
        assert r['status'] == 'match'

    def test_different_counts_mismatch(self):
        from pdi2dag.reconcile import reconcile_inputs
        r = reconcile_inputs(self._carte(12), self._pdc(17))[0]
        assert r['status'] == 'mismatch'
        assert 'difference -5' in r['detail']

    def test_unprofiled_table_is_unknown_not_match(self):
        from pdi2dag.reconcile import reconcile_inputs
        r = reconcile_inputs(self._carte(17), self._pdc(None))[0]
        assert r['status'] == 'unknown'

    def test_no_carte_run_is_unknown_not_match(self):
        from pdi2dag.reconcile import reconcile_inputs
        r = reconcile_inputs(self._carte(None), self._pdc(17))[0]
        assert r['status'] == 'unknown'

    def test_zero_rows_both_sides_still_matches(self):
        # 0 == 0 is agreement, not a missing measurement. Guards against
        # a falsy-check regression.
        from pdi2dag.reconcile import reconcile_inputs
        r = reconcile_inputs(self._carte(0), self._pdc(0))[0]
        assert r['status'] == 'match'


class TestPublishSafetyNets:
    """The checks that would have caught today's bugs immediately."""

    KNOWN = {'postgres://192.168.1.200:5433', 's3://cscu-documents'}

    def _event(self, namespace):
        return {'inputs': [{'namespace': namespace,
                            'name': 'cscu_core.cscu_core.transactions'}],
                'outputs': []}

    def test_wrong_scheme_is_flagged(self):
        from pdi2dag.lineage import unmatched_datasets
        orphans = unmatched_datasets(
            [self._event('postgresql://192.168.1.200:5433')], self.KNOWN)
        assert orphans[0]['namespace'] == 'postgresql://192.168.1.200:5433'

    def test_matching_scheme_is_clean(self):
        from pdi2dag.lineage import unmatched_datasets
        assert unmatched_datasets(
            [self._event('postgres://192.168.1.200:5433')], self.KNOWN) == []

    def test_local_file_datasets_are_not_orphans(self):
        # A path on disk is lineage-only by nature; flagging it would
        # train the user to ignore the warning.
        from pdi2dag.lineage import unmatched_datasets
        assert unmatched_datasets([self._event('file')], self.KNOWN) == []

# -*- coding: utf-8 -*-
"""Unit tests for the PDI file parser."""

import os

import pytest

from pdi2dag.parser import parse_file

SAMPLES = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                       'samples')


class TestParseJob:

    @pytest.fixture
    def doc(self):
        return parse_file(os.path.join(SAMPLES, 'nightly_etl.kjb'))

    def test_job_metadata(self, doc):
        assert doc.kind == 'job'
        assert doc.name == 'nightly_etl'
        assert doc.repo_path == '/demo/nightly_etl'
        assert 'Nightly warehouse load' in doc.description

    def test_parameters(self, doc):
        names = {p.name: p for p in doc.parameters}
        assert set(names) == {'date', 'region'}
        assert names['region'].default == 'EMEA'
        assert names['date'].default == ''

    def test_entries(self, doc):
        by_name = {e.name: e for e in doc.entries}
        assert by_name['Start'].is_start
        assert by_name['Extract Sales'].entry_type == 'TRANS'
        assert by_name['Extract Sales'].path == '/demo/extract_sales'
        assert by_name['Publish Reports'].entry_type == 'JOB'
        assert by_name['Publish Reports'].path == \
            '/demo/reporting/publish_reports'
        assert by_name['Mail Failure'].entry_type == 'MAIL'
        assert not by_name['Mail Failure'].is_executable
        assert len(doc.executable_entries) == 4

    def test_hops(self, doc):
        assert len(doc.hops) == 7
        failure_hops = [h for h in doc.hops if not h.evaluation
                        and not h.unconditional]
        assert len(failure_hops) == 1
        assert failure_hops[0].to_name == 'Mail Failure'

    def test_original_plugin_sample(self):
        doc = parse_file(os.path.join(SAMPLES, 'test_job.kjb'))
        assert doc.name == 'test_job'
        assert doc.kind == 'job'
        # Start and Success only: nothing executable
        assert doc.executable_entries == []


class TestParseTrans:

    def test_trans_metadata(self):
        doc = parse_file(os.path.join(SAMPLES, 'test_trans.ktr'))
        assert doc.kind == 'transformation'
        assert doc.name == 'test_trans'
        assert doc.repo_path == '/home/test_trans'
        assert doc.entries == []


class TestErrors:

    def test_invalid_root(self, tmp_path):
        bad = tmp_path / 'not_pdi.xml'
        bad.write_text('<workflow><name>x</name></workflow>')
        with pytest.raises(ValueError, match='root element'):
            parse_file(str(bad))

    def test_broken_xml(self, tmp_path):
        bad = tmp_path / 'broken.kjb'
        bad.write_text('<job><name>x')
        with pytest.raises(ValueError, match='Could not parse'):
            parse_file(str(bad))

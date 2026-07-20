# -*- coding: utf-8 -*-
"""Unit tests for the DAG generator."""

import ast
import os

import pytest

from pdi2dag.generator import ConvertOptions, convert
from pdi2dag.parser import parse_file

SAMPLES = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                       'samples')


@pytest.fixture
def job_doc():
    return parse_file(os.path.join(SAMPLES, 'nightly_etl.kjb'))


@pytest.fixture
def trans_doc():
    return parse_file(os.path.join(SAMPLES, 'test_trans.ktr'))


class TestExplodeMode:

    def test_generates_valid_python(self, job_doc):
        result = convert(job_doc, ConvertOptions(schedule='0 6 * * *'))
        ast.parse(result.code)  # raises SyntaxError if invalid

    def test_tasks_and_dependencies(self, job_doc):
        result = convert(job_doc, ConvertOptions(schedule='0 6 * * *'))
        code = result.code
        assert result.dag_id == 'nightly_etl'
        assert "schedule='0 6 * * *'" in code
        # One task per TRANS/JOB entry
        assert "Extract_Sales = CarteTransOperator(" in code
        assert "trans='/demo/extract_sales'" in code
        assert "Publish_Reports = CarteJobOperator(" in code
        assert "job='/demo/reporting/publish_reports'" in code
        # Hops became dependencies; control entries collapsed away
        assert 'Extract_Sales >> Load_Warehouse' in code
        assert 'Extract_Customers >> Load_Warehouse' in code
        assert 'Load_Warehouse >> Publish_Reports' in code
        assert 'Start' not in code.split('"""')[2]  # not in the code body

    def test_parameters_with_overrides(self, job_doc):
        result = convert(job_doc, ConvertOptions(
            params={'date': '{{ ds }}'}))
        assert "'date': '{{ ds }}'" in result.code
        assert "'region': 'EMEA'" in result.code

    def test_unsupported_entry_warned(self, job_doc):
        result = convert(job_doc, ConvertOptions())
        assert any('Mail Failure' in w for w in result.warnings)

    def test_deferrable_kwargs(self, job_doc):
        result = convert(job_doc, ConvertOptions(deferrable=True,
                                                 poll_interval=30))
        assert 'deferrable=True' in result.code
        assert 'poll_interval=30' in result.code


class TestWrapMode:

    def test_job_wrap(self, job_doc):
        result = convert(job_doc, ConvertOptions(mode='wrap'))
        code = result.code
        assert 'run_nightly_etl = CarteJobOperator(' in code
        assert "job='/demo/nightly_etl'" in code
        assert 'CarteTransOperator' not in code
        ast.parse(code)

    def test_trans_always_wraps(self, trans_doc):
        result = convert(trans_doc, ConvertOptions(mode='explode'))
        code = result.code
        assert 'run_test_trans = CarteTransOperator(' in code
        assert "trans='/home/test_trans'" in code
        ast.parse(code)

    def test_empty_job_falls_back_to_wrap(self):
        doc = parse_file(os.path.join(SAMPLES, 'test_job.kjb'))
        result = convert(doc, ConvertOptions())
        assert 'run_test_job = CarteJobOperator(' in result.code
        assert any('no TRANS/JOB entries' in w for w in result.warnings)


class TestDagOptions:

    def test_manual_schedule_is_none(self, trans_doc):
        result = convert(trans_doc, ConvertOptions())
        assert 'schedule=None' in result.code

    def test_dag_id_override_sanitized(self, trans_doc):
        result = convert(trans_doc, ConvertOptions(
            dag_id='My Fancy DAG!'))
        assert result.dag_id == 'My_Fancy_DAG'

    def test_start_date(self, trans_doc):
        result = convert(trans_doc, ConvertOptions(
            start_date='2026-01-15'))
        assert 'pendulum.datetime(2026, 1, 15' in result.code


class TestGeneratedDagImports:
    """Load the generated code with the real Airflow installed.

    Skipped automatically when airflow is not importable in the test
    environment.
    """

    def test_dag_parses_in_airflow(self, job_doc, tmp_path):
        pytest.importorskip('airflow')
        pytest.importorskip('airflow_pentaho')
        result = convert(job_doc, ConvertOptions(
            schedule='0 6 * * *', deferrable=True,
            params={'date': '{{ ds }}'}))
        namespace = {}
        exec(compile(result.code, 'nightly_etl.py', 'exec'), namespace)
        dag = namespace['dag']
        assert dag.dag_id == 'nightly_etl'
        task_ids = {t.task_id for t in dag.tasks}
        assert task_ids == {'Extract_Sales', 'Extract_Customers',
                            'Load_Warehouse', 'Publish_Reports'}
        load = dag.get_task('Load_Warehouse')
        upstream = set(load.upstream_task_ids)
        assert upstream == {'Extract_Sales', 'Extract_Customers'}


class TestFileDatasets:
    """File-based steps (CSV / text / Excel) become file datasets."""

    def test_csv_input_to_db_output(self):
        from pdi2dag.parser import parse_trans_detail
        from pdi2dag.lineage import trans_datasets
        d = parse_trans_detail(os.path.join(
            SAMPLES, 'cscu', 'import_ach_csv.ktr'))
        ins, outs = trans_datasets(d)
        assert ins[0]['namespace'] == 'file'
        assert ins[0]['name'] == '/data/cscu/ach_payments_2026.csv'
        assert outs[0]['name'] == 'cscu_mart.staging.ach_stg'

    def test_s3_and_local_file_naming(self):
        from pdi2dag.lineage import _file_dataset
        assert _file_dataset('s3://bucket/path/x.csv') == {
            'namespace': 's3://bucket', 'name': 'path/x.csv'}
        assert _file_dataset(r'C:\data\x.xlsx')['namespace'] == 'file'

# -*- coding: utf-8 -*-
"""Unit tests for the pdi2dag CLI."""

import os

from pdi2dag.cli import main

SAMPLES = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                       'samples')
JOB = os.path.join(SAMPLES, 'nightly_etl.kjb')


class TestCli:

    def test_inspect(self, capsys):
        rc = main(['inspect', JOB])
        out = capsys.readouterr().out
        assert rc == 0
        assert 'nightly_etl' in out
        assert 'Extract Sales' in out
        assert '[failure]' in out

    def test_convert_to_directory(self, tmp_path, capsys):
        rc = main(['convert', JOB, '--schedule', '0 6 * * *',
                   '-o', str(tmp_path),
                   '--param', 'date={{ ds }}'])
        out = capsys.readouterr().out
        assert rc == 0
        dag_file = tmp_path / 'nightly_etl.py'
        assert dag_file.exists()
        code = dag_file.read_text(encoding='utf-8')
        assert "schedule='0 6 * * *'" in code
        assert 'WARNING' in out  # Mail Failure entry warning

    def test_migrate_without_airflow_url(self, tmp_path, capsys):
        dags = tmp_path / 'dags'
        dags.mkdir()
        rc = main(['migrate', JOB, '--dags-folder', str(dags)])
        assert rc == 0
        assert (dags / 'nightly_etl.py').exists()

    def test_deploy(self, tmp_path):
        src = tmp_path / 'some_dag.py'
        src.write_text('# dag')
        dags = tmp_path / 'dags'
        dags.mkdir()
        rc = main(['deploy', str(src), '--dags-folder', str(dags)])
        assert rc == 0
        assert (dags / 'some_dag.py').exists()

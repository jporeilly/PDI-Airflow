# -*- coding: utf-8 -*-
"""Shared test configuration.

AIRFLOW_HOME is pointed at a project-local directory before any airflow
import so tests never touch the user's real Airflow installation.
"""

import json
import os

os.environ.setdefault(
    'AIRFLOW_HOME',
    os.path.join(os.path.dirname(os.path.dirname(__file__)), '.airflow'))
os.environ.setdefault('AIRFLOW__CORE__LOAD_EXAMPLES', 'False')
os.environ.setdefault('AIRFLOW__CORE__UNIT_TEST_MODE', 'True')

import pytest  # noqa: E402
from unittest import mock  # noqa: E402

from airflow.models import Connection  # noqa: E402


CARTE_EXTRA = {
    'rep': 'Default',
    'carte_username': 'cluster',
    'carte_password': 'cluster',
}

KETTLE_EXTRA = {
    'rep': 'Default',
    'pentaho_home': '/opt/pentaho',
}


@pytest.fixture
def carte_connection():
    return Connection(
        conn_id='pdi_default',
        conn_type='pentaho',
        host='http://localhost',
        port=8080,
        login='repo_user',
        password='repo_pass',
        extra=json.dumps(CARTE_EXTRA))


@pytest.fixture
def kettle_connection():
    return Connection(
        conn_id='pdi_default',
        conn_type='pentaho',
        login='repo_user',
        password='repo_pass',
        extra=json.dumps({**CARTE_EXTRA, **KETTLE_EXTRA}))


@pytest.fixture
def mock_carte_get_connection(carte_connection):
    from airflow_pentaho.hooks.carte import PentahoCarteHook
    with mock.patch.object(PentahoCarteHook, 'get_connection',
                           return_value=carte_connection):
        yield


@pytest.fixture
def mock_kettle_get_connection(kettle_connection):
    from airflow_pentaho.hooks.kettle import PentahoHook
    with mock.patch.object(PentahoHook, 'get_connection',
                           return_value=kettle_connection):
        yield

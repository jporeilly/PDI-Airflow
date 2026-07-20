# -*- coding: utf-8 -*-
"""Unit tests for provider metadata."""

import airflow_pentaho


def test_provider_info_shape():
    info = airflow_pentaho.get_provider_info()
    assert info['package-name'] == 'airflow-provider-pentaho'
    assert info['versions'] == [airflow_pentaho.__version__]
    conn_types = info['connection-types']
    assert conn_types[0]['connection-type'] == 'pentaho'
    assert conn_types[0]['hook-class-name'] == \
        'airflow_pentaho.hooks.carte.PentahoCarteHook'


def test_hook_class_importable():
    from airflow_pentaho.hooks.carte import PentahoCarteHook
    assert PentahoCarteHook.conn_type == 'pentaho'

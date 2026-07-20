# -*- coding: utf-8 -*-
"""Unit tests for the Carte hook."""

from unittest import mock

import pytest

from airflow.exceptions import AirflowException

from airflow_pentaho.hooks.carte import PentahoCarteHook


RUN_JOB_XML = (b'<webresult><result>OK</result>'
               b'<message>Job started</message>'
               b'<id>abc-123</id></webresult>')

JOB_STATUS_XML = (b'<jobstatus><jobname>test_job</jobname>'
                  b'<id>abc-123</id>'
                  b'<status_desc>Finished</status_desc>'
                  b'<error_desc/>'
                  b'<logging_string/>'
                  b'<first_log_line_nr>0</first_log_line_nr>'
                  b'<last_log_line_nr>10</last_log_line_nr>'
                  b'</jobstatus>')

ERROR_XML = (b'<webresult><result>ERROR</result>'
             b'<message>Unable to find job</message>'
             b'</webresult>')


def _response(status_code=200, content=b''):
    rs = mock.Mock()
    rs.status_code = status_code
    rs.content = content
    return rs


@pytest.fixture
def client(mock_carte_get_connection):
    hook = PentahoCarteHook(conn_id='pdi_default', level='Debug')
    return hook.get_conn()


class TestPentahoCarteClient:

    def test_client_built_from_connection(self, client):
        assert client.host == 'http://localhost'
        assert client.port == 8080
        assert client.rep == 'Default'
        assert client.username == 'repo_user'
        assert client.password == 'repo_pass'
        assert client.carte_username == 'cluster'
        assert client.carte_password == 'cluster'
        assert client.level == 'Debug'
        assert client.verify_ssl is True

    def test_host_without_scheme_gets_http(self, carte_connection):
        carte_connection.host = 'carte.local'
        with mock.patch.object(PentahoCarteHook, 'get_connection',
                               return_value=carte_connection):
            client = PentahoCarteHook().get_conn()
        assert client.host == 'http://carte.local'

    @mock.patch('airflow_pentaho.hooks.carte.requests.post')
    def test_run_job_posts_credentials_in_body(self, post, client):
        post.return_value = _response(200, RUN_JOB_XML)

        result = client.run_job('/home/bi/test_job', {'date': '2026-07-18'})

        assert result['webresult']['id'] == 'abc-123'
        _, kwargs = post.call_args
        assert kwargs['url'] == 'http://localhost:8080/kettle/executeJob/'
        assert kwargs['data']['user'] == 'repo_user'
        assert kwargs['data']['pass'] == 'repo_pass'
        assert kwargs['data']['job'] == '/home/bi/test_job'
        assert kwargs['data']['date'] == '2026-07-18'
        # Credentials must not travel in the URL
        assert 'repo_pass' not in kwargs['url']
        assert kwargs['auth'].username == 'cluster'

    @mock.patch('airflow_pentaho.hooks.carte.requests.post')
    def test_run_trans_returns_parsed_response(self, post, client):
        post.return_value = _response(
            200, b'<webresult><result>OK</result></webresult>')

        result = client.run_trans('/home/bi/test_trans')

        assert result['webresult']['result'] == 'OK'
        _, kwargs = post.call_args
        assert kwargs['url'] == 'http://localhost:8080/kettle/executeTrans/'
        assert kwargs['data']['trans'] == '/home/bi/test_trans'

    @mock.patch('airflow_pentaho.hooks.carte.requests.post')
    def test_job_status_uses_last_log_line(self, post, client):
        post.return_value = _response(200, JOB_STATUS_XML)

        previous = {'jobstatus': {'last_log_line_nr': '10'}}
        result = client.job_status('test_job', 'abc-123', previous)

        assert result['jobstatus']['status_desc'] == 'Finished'
        _, kwargs = post.call_args
        assert kwargs['data']['from'] == '10'
        assert kwargs['data']['id'] == 'abc-123'

    @mock.patch('airflow_pentaho.hooks.carte.requests.post')
    def test_error_response_raises(self, post, client):
        post.return_value = _response(500, ERROR_XML)

        with pytest.raises(AirflowException, match='Unable to find job'):
            client.run_job('/home/bi/bad_job')

    @mock.patch('airflow_pentaho.hooks.carte.requests.post')
    def test_non_xml_error_raises(self, post, client):
        post.return_value = _response(502, b'Bad Gateway')

        with pytest.raises(AirflowException, match='502'):
            client.run_job('/home/bi/test_job')

    @mock.patch('airflow_pentaho.hooks.carte.requests.post')
    def test_stop_job(self, post, client):
        post.return_value = _response(
            200, b'<webresult><result>OK</result></webresult>')

        client.stop_job('test_job', 'abc-123')

        _, kwargs = post.call_args
        assert kwargs['url'] == 'http://localhost:8080/kettle/stopJob/'
        assert kwargs['data']['name'] == 'test_job'
        assert kwargs['data']['id'] == 'abc-123'

    def test_param_object_unwrapped(self, client):
        class FakeParam:
            value = 'unwrapped'

        with mock.patch(
                'airflow_pentaho.hooks.carte.requests.post') as post:
            post.return_value = _response(200, RUN_JOB_XML)
            client.run_job('/home/bi/test_job', {'p': FakeParam()})
            _, kwargs = post.call_args
        assert kwargs['data']['p'] == 'unwrapped'


class TestHookUi:

    def test_ui_field_behaviour(self):
        behaviour = PentahoCarteHook.get_ui_field_behaviour()
        assert 'schema' in behaviour['hidden_fields']
        assert 'extra' in behaviour['placeholders']

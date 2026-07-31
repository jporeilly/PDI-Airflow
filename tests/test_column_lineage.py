# -*- coding: utf-8 -*-
"""Contract tests for offline column-level lineage.

The paid PDI OpenLineage plugin derives column lineage at runtime with
a SQL parser; ``pdi2dag/columns.py`` derives the same mapping offline
from the .ktr. These tests pin both the SQL analysis and the walk's
step semantics - and, like the emitter tests, the wire format of the
resulting facets, because a facet naming a dataset one character off
never joins the graph.
"""

import os

from pdi2dag.columns import (column_flow, output_column_lineage,
                             select_columns, table_aliases)
from pdi2dag.lineage import (build_pdc_trans_events, dataset_column_facets)
from pdi2dag.parser import (PdiConnection, PdiHop, PdiStep, PdiTransDetail,
                            parse_trans_detail)

SAMPLES = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                       'samples')

CSCU_CORE = PdiConnection(name='cscu-core', db_type='POSTGRESQL',
                          server='192.168.1.200', port='5433',
                          database='cscu_core', username='pdc_user')
CSCU_MART = PdiConnection(name='cscu-mart', db_type='POSTGRESQL',
                          server='192.168.1.200', port='5433',
                          database='cscu_mart', username='pdc_user')


# ------------------------------------------------------------ SQL analysis

class TestSelectColumns:
    def test_aliased_join_resolves_through_the_alias_map(self):
        cols = select_columns(
            'SELECT m.mbr_id, b.br_name FROM cscu_core.members m '
            'JOIN cscu_core.branches b ON b.br_id = m.br_id')
        assert cols == [
            ('mbr_id', [('cscu_core.members', 'mbr_id')]),
            ('br_name', [('cscu_core.branches', 'br_name')]),
        ]

    def test_expressions_carry_every_operand(self):
        cols = select_columns(
            'SELECT coalesce(sum(a.bal_amt), 0) AS total_bal '
            'FROM accounts a JOIN members m ON m.id = a.mbr_id')
        assert cols == [('total_bal', [('accounts', 'bal_amt')])]

    def test_bare_columns_bind_only_with_a_single_table(self):
        single = select_columns('SELECT txn_id, txn_amt FROM cscu.transactions')
        assert single == [
            ('txn_id', [('cscu.transactions', 'txn_id')]),
            ('txn_amt', [('cscu.transactions', 'txn_amt')]),
        ]
        # with a join a bare column is ambiguous - a wrong edge is worse
        # than a missing one
        joined = select_columns(
            'SELECT txn_id FROM a_tab JOIN b_tab ON a_tab.x = b_tab.x')
        assert joined == [('txn_id', [])]

    def test_select_star_is_honestly_unknown(self):
        assert select_columns('SELECT * FROM anything') is None
        assert select_columns('SELECT t.* FROM anything t') is None

    def test_where_clause_variables_do_not_leak_into_columns(self):
        cols = select_columns(
            "SELECT txn_id FROM cscu.transactions "
            "WHERE post_dt >= '${since_dt}'")
        assert cols == [('txn_id', [('cscu.transactions', 'txn_id')])]

    def test_alias_map_keeps_keywords_out(self):
        aliases = table_aliases(
            'SELECT 1 FROM sch.orders o LEFT JOIN sch.lines ON 1=1 '
            'WHERE 1=1 GROUP BY 1')
        assert aliases.get('o') == 'sch.orders'
        assert 'left' not in {k.lower() for k in aliases}
        assert 'where' not in {k.lower() for k in aliases}


# --------------------------------------------------------- step semantics

def _detail(steps, hops, connections=None):
    return PdiTransDetail(name='t', steps=steps, hops=hops,
                          connections=connections or [CSCU_CORE, CSCU_MART])


def _table_input(name='in', sql='SELECT a, b FROM sch.src'):
    return PdiStep(name=name, step_type='TableInput',
                   connection='cscu-core', sql=sql)


def _table_output(name='out', mappings=None):
    step = PdiStep(name=name, step_type='TableOutput',
                   connection='cscu-mart', schema='mart', table='tgt')
    if mappings:
        step.column_info = {'mappings': mappings}
    return step


class TestColumnFlow:
    def test_unknown_steps_pass_fields_through(self):
        steps = [
            _table_input(),
            PdiStep(name='sort', step_type='SortRows'),
            _table_output(),
        ]
        hops = [PdiHop('in', 'sort'), PdiHop('sort', 'out')]
        cols = output_column_lineage(_detail(steps, hops))['out']
        assert list(cols) == ['a', 'b']
        assert cols['a'] == [('db', 'cscu-core', 'sch.src', 'a')]

    def test_select_values_renames_and_prunes(self):
        sel = PdiStep(name='sel', step_type='SelectValues')
        sel.column_info = {'selects': [('a', 'renamed_a')], 'removes': [],
                           'meta_renames': []}
        steps = [_table_input(), sel, _table_output()]
        hops = [PdiHop('in', 'sel'), PdiHop('sel', 'out')]
        cols = output_column_lineage(_detail(steps, hops))['out']
        assert list(cols) == ['renamed_a']
        assert cols['renamed_a'] == [('db', 'cscu-core', 'sch.src', 'a')]

    def test_calculator_unions_its_operands(self):
        calc = PdiStep(name='calc', step_type='Calculator')
        calc.column_info = {'calcs': [('a_plus_b', ['a', 'b'])]}
        steps = [_table_input(), calc, _table_output()]
        hops = [PdiHop('in', 'calc'), PdiHop('calc', 'out')]
        cols = output_column_lineage(_detail(steps, hops))['out']
        assert cols['a_plus_b'] == [('db', 'cscu-core', 'sch.src', 'a'),
                                    ('db', 'cscu-core', 'sch.src', 'b')]

    def test_formula_reads_bracketed_references(self):
        f = PdiStep(name='f', step_type='Formula')
        f.column_info = {'formulas': [('full', '[a] & " " & [b]')]}
        steps = [_table_input(), f, _table_output()]
        hops = [PdiHop('in', 'f'), PdiHop('f', 'out')]
        cols = output_column_lineage(_detail(steps, hops))['out']
        assert cols['full'] == [('db', 'cscu-core', 'sch.src', 'a'),
                                ('db', 'cscu-core', 'sch.src', 'b')]

    def test_stream_lookup_takes_values_from_the_info_stream(self):
        lookup_src = _table_input(name='lkp_src',
                                  sql='SELECT k, v FROM sch.ref')
        lk = PdiStep(name='lk', step_type='StreamLookup')
        lk.column_info = {'from_step': 'lkp_src',
                          'values': [('v', 'looked_up')]}
        steps = [_table_input(), lookup_src, lk, _table_output()]
        hops = [PdiHop('in', 'lk'), PdiHop('lkp_src', 'lk'),
                PdiHop('lk', 'out')]
        cols = output_column_lineage(_detail(steps, hops))['out']
        # main stream fields flow through; the value arrives renamed
        assert cols['a'] == [('db', 'cscu-core', 'sch.src', 'a')]
        assert cols['looked_up'] == [('db', 'cscu-core', 'sch.ref', 'v')]

    def test_table_output_mapping_binds_stream_to_columns(self):
        steps = [_table_input(),
                 _table_output(mappings=[('col_a', 'a'), ('col_b', 'b')])]
        hops = [PdiHop('in', 'out')]
        cols = output_column_lineage(_detail(steps, hops))['out']
        assert list(cols) == ['col_a', 'col_b']
        assert cols['col_a'] == [('db', 'cscu-core', 'sch.src', 'a')]

    def test_select_star_yields_no_columns_not_fabricated_ones(self):
        steps = [_table_input(sql='SELECT * FROM sch.src'), _table_output()]
        hops = [PdiHop('in', 'out')]
        assert output_column_lineage(_detail(steps, hops))['out'] == {}

    def test_disabled_hops_carry_nothing(self):
        steps = [_table_input(), _table_output()]
        hops = [PdiHop('in', 'out', enabled=False)]
        assert output_column_lineage(_detail(steps, hops))['out'] == {}


# ---------------------------------------------------------- facet contract

class TestFacetWireFormat:
    def test_cscu_mart_facet_names_match_the_datasets(self):
        detail = parse_trans_detail(
            os.path.join(SAMPLES, 'cscu', 'build_member_mart.ktr'))
        col, schema = dataset_column_facets(detail)
        key = ('postgres://192.168.1.200:5433',
               'cscu_mart.mart.member_360')
        assert key in col
        fields = col[key]['fields']
        # a joined aggregate traces to the joined table's column
        assert fields['txn_total_amt']['inputFields'] == [{
            'namespace': 'postgres://192.168.1.200:5433',
            'name': 'cscu_core.cscu_core.transactions',
            'field': 'txn_amt',
        }]
        # the input tables expose exactly the columns the SQL reads
        assert schema[('postgres://192.168.1.200:5433',
                       'cscu_core.cscu_core.branches')] == \
            ['br_name', 'br_city']

    def test_events_carry_schema_and_column_facets(self):
        detail = parse_trans_detail(
            os.path.join(SAMPLES, 'cscu', 'extract_transactions.ktr'))
        events = build_pdc_trans_events(detail, '/cscu/extract_transactions',
                                        server_name='cscu-pdi')
        out = events[0]['outputs'][0]
        assert set(out['facets']) == {'schema', 'columnLineage'}
        cl = out['facets']['columnLineage']
        assert cl['_producer'] == events[0]['producer']
        assert cl['fields']['txn_amt']['inputFields'][0]['name'] == \
            'cscu_core.cscu_core.transactions'
        ins = events[0]['inputs'][0]
        assert 'columnLineage' not in ins.get('facets', {})
        assert [f['name'] for f in ins['facets']['schema']['fields']] == \
            ['txn_id', 'acct_id', 'txn_dt', 'post_dt', 'txn_amt',
             'merch_nm', 'mcc_cd']

    def test_a_ktr_with_no_declared_fields_emits_no_fabricated_facets(self):
        detail = parse_trans_detail(
            os.path.join(SAMPLES, 'cscu', 'import_ach_csv.ktr'))
        col, schema = dataset_column_facets(detail)
        assert col == {}
        events = build_pdc_trans_events(detail, '/cscu/import_ach_csv')
        assert all('facets' not in d or 'columnLineage' not in d['facets']
                   for d in events[0]['outputs'])

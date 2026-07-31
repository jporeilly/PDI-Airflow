"""Offline column-level lineage for a transformation (.ktr).

The paid PDI OpenLineage plugin derives column lineage at runtime with a
SQL parser. This module gets to the same answer offline, from the .ktr
alone: parse the Table Input SELECT list into *output field -> source
table columns*, walk the hop graph applying each step's field
semantics, and read the final mapping off the dataset-writing steps.

PDI's own execution model makes the walk tractable: a step's output
stream is its input stream plus whatever the step adds, renames or
removes. A step type this module does not model therefore PASSES FIELDS
THROUGH - which is exactly what most steps (sorts, filters, dummies,
write-to-log) actually do. Where a column's origin genuinely cannot be
known (``SELECT *``, a scripted step that invents fields), the column
simply carries no lineage rather than a guessed one.

A source reference is a tuple:

  ('db',   connection_name, 'schema.table', column)
  ('file', path, column)

Resolution to OpenLineage namespace/name happens in ``lineage.py``,
which owns the naming conventions - this module never imports it.
"""

import re
from collections import OrderedDict

# SQL keywords that can trail a table name where an alias would sit.
_NOT_ALIAS = {
    'on', 'left', 'right', 'inner', 'outer', 'full', 'cross', 'join',
    'where', 'group', 'order', 'having', 'union', 'limit', 'natural',
    'using', 'as',
}

_KEYWORDS = _NOT_ALIAS | {
    'select', 'distinct', 'from', 'and', 'or', 'not', 'in', 'is',
    'null', 'case', 'when', 'then', 'else', 'end', 'between', 'like',
    'by', 'asc', 'desc', 'exists', 'all', 'any', 'over', 'partition',
}


def _strip_comments(sql):
    sql = re.sub(r'--[^\n]*', ' ', sql or '')
    return re.sub(r'/\*.*?\*/', ' ', sql, flags=re.S)


def _split_top_level(text, sep=','):
    """Split on ``sep`` outside parentheses and quotes."""
    parts, depth, buf, quote = [], 0, [], None
    for ch in text:
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = None
            continue
        if ch in ("'", '"'):
            quote = ch
            buf.append(ch)
            continue
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        if ch == sep and depth == 0:
            parts.append(''.join(buf))
            buf = []
        else:
            buf.append(ch)
    if buf:
        parts.append(''.join(buf))
    return [p.strip() for p in parts if p.strip()]


def table_aliases(sql):
    """``{alias_or_table: 'schema.table'}`` from FROM/JOIN clauses.

    Every table is reachable by its alias when one is declared, and by
    its bare (and qualified) name either way.
    """
    sql = _strip_comments(sql)
    out = OrderedDict()
    # The alias group must REFUSE keywords, not just discard them - a
    # match that consumes `JOIN` as an alias also swallows the next
    # table clause, and the second table of every join disappears.
    not_alias = '|'.join(sorted(_NOT_ALIAS))
    for m in re.finditer(
            r'\b(?:FROM|JOIN)\s+"?([A-Za-z_]\w*)"?(?:\."?([A-Za-z_]\w*)"?)?'
            r'(?:\s+(?:AS\s+)?(?!(?:' + not_alias + r')\b)([A-Za-z_]\w*))?',
            sql, re.IGNORECASE):
        first, second, alias = m.group(1), m.group(2), m.group(3)
        token = '{}.{}'.format(first, second) if second else first
        if alias and alias.lower() not in _NOT_ALIAS:
            out[alias] = token
        out[second or first] = token
        out[token] = token
    return out


def _item_refs(expr, aliases, single_table):
    """Source (table_token, column) pairs referenced by one select-list
    expression. Qualified names resolve through the alias map; bare
    identifiers count only when the query reads exactly one table -
    with a join they are ambiguous, and a wrong edge is worse than a
    missing one."""
    refs = []
    seen = set()
    for m in re.finditer(r'"?\b([A-Za-z_]\w*)\b"?\s*\.\s*"?([A-Za-z_]\w*)"?',
                         expr):
        table = aliases.get(m.group(1))
        if table and (table, m.group(2)) not in seen:
            seen.add((table, m.group(2)))
            refs.append((table, m.group(2)))
    if not refs and single_table:
        for m in re.finditer(r'"?\b([A-Za-z_]\w*)\b"?(?!\s*\()', expr):
            ident = m.group(1)
            if ident.lower() in _KEYWORDS or ident.isdigit():
                continue
            if (single_table, ident) not in seen:
                seen.add((single_table, ident))
                refs.append((single_table, ident))
    return refs


def select_columns(sql):
    """The SELECT list as ``[(output_name, [(table, column), ...])]``.

    ``SELECT *`` (or ``alias.*``) returns ``None`` - the field list is
    unknowable offline, so callers emit no column facet rather than a
    fabricated one. An expression with no derivable output name is
    skipped for the same reason.
    """
    sql = _strip_comments(sql)
    m = re.search(r'\bSELECT\b(\s+DISTINCT\b)?(.*?)\bFROM\b', sql,
                  re.IGNORECASE | re.S)
    if not m:
        return None
    select_list = m.group(2)
    if re.search(r'(^|[\s,.])\*', select_list):
        return None
    aliases = table_aliases(sql)
    tables = list(dict.fromkeys(aliases.values()))
    single_table = tables[0] if len(tables) == 1 else None
    out = []
    for item in _split_top_level(select_list):
        alias_m = re.search(r'\s+AS\s+"?([A-Za-z_]\w*)"?\s*$', item,
                            re.IGNORECASE)
        if alias_m:
            name = alias_m.group(1)
            expr = item[:alias_m.start()]
        else:
            simple = re.fullmatch(
                r'"?([A-Za-z_]\w*)"?(?:\s*\.\s*"?([A-Za-z_]\w*)"?)?', item)
            if simple:
                name = simple.group(2) or simple.group(1)
                expr = item
            else:
                # `expr alias` (no AS) - a trailing bare identifier that
                # is not part of the expression's last token
                tail = re.search(r'\)\s+"?([A-Za-z_]\w*)"?\s*$', item)
                if tail:
                    name = tail.group(1)
                    expr = item[:tail.start() + 1]
                else:
                    continue    # unnamed expression: nothing to bind to
        out.append((name, _item_refs(expr, aliases, single_table)))
    return out


_FORMULA_REF = re.compile(r'\[([^\]\[]+)\]')


def _incoming(detail):
    """step name -> [names of steps hopping into it] (enabled hops)."""
    incoming = {s.name: [] for s in detail.steps}
    for hop in detail.hops:
        if hop.enabled and hop.to_name in incoming \
                and hop.from_name in incoming:
            incoming[hop.to_name].append(hop.from_name)
    return incoming


def _topo_order(detail, incoming):
    order, ready = [], [s for s in detail.steps if not incoming[s.name]]
    remaining = {s.name: len(incoming[s.name]) for s in detail.steps}
    by_name = {s.name: s for s in detail.steps}
    seen = set()
    while ready:
        step = ready.pop(0)
        if step.name in seen:
            continue
        seen.add(step.name)
        order.append(step)
        for other, ins in incoming.items():
            if step.name in ins:
                remaining[other] -= 1
                if remaining[other] == 0:
                    ready.append(by_name[other])
    return order


def _merge(states):
    out = OrderedDict()
    for state in states:
        for field, refs in state.items():
            out.setdefault(field, set()).update(refs)
    return out


def _transfer(step, in_state, ctx):
    """One step's output stream from its input stream."""
    info = step.column_info or {}
    t = step.step_type

    if t == 'TableInput':
        cols = select_columns(step.sql)
        if cols is None:
            return OrderedDict()
        out = OrderedDict()
        for name, refs in cols:
            out[name] = {('db', step.connection, table, col)
                         for table, col in refs}
        return out

    if step.is_file_input:
        fields = info.get('fields') or []
        out = OrderedDict()
        for f in fields:
            out[f] = {('file', path, f) for path in step.files} \
                if step.files else set()
        return out

    if t == 'SelectValues':
        selects = info.get('selects') or []
        removes = {r.lower() for r in info.get('removes') or []}
        out = OrderedDict()
        if selects:
            for name, rename in selects:
                out[rename or name] = set(in_state.get(name, set()))
        else:
            for field, refs in in_state.items():
                if field.lower() not in removes:
                    out[field] = set(refs)
        for name, rename in info.get('meta_renames') or []:
            if rename and name in out:
                out[rename] = out.pop(name)
        return out

    if t == 'Calculator':
        out = OrderedDict(in_state)
        for new, operands in info.get('calcs') or []:
            refs = set()
            for op in operands:
                refs |= in_state.get(op, set())
            out[new] = refs
        return out

    if t == 'Formula':
        out = OrderedDict(in_state)
        for new, formula in info.get('formulas') or []:
            refs = set()
            for ref in _FORMULA_REF.findall(formula or ''):
                refs |= in_state.get(ref, set())
            out[new] = refs
        return out

    if t in ('Constant', 'RowGenerator'):
        out = OrderedDict(in_state)
        for name in info.get('adds') or []:
            out.setdefault(name, set())
        return out

    if t == 'StreamLookup':
        lookup_state = ctx.get(info.get('from_step'), OrderedDict())
        out = OrderedDict(in_state)
        for name, rename in info.get('values') or []:
            out[rename or name] = set(lookup_state.get(name, set()))
        return out

    if t == 'DBLookup':
        out = OrderedDict(in_state)
        table = '{}.{}'.format(info.get('schema'), info.get('table')) \
            if info.get('schema') else (info.get('table') or '')
        for src, rename in info.get('values') or []:
            out[rename or src] = {('db', info.get('connection', ''),
                                   table, src)} if table else set()
        return out

    # Unknown step: PDI streams fields through.
    return OrderedDict(in_state)


def column_flow(detail):
    """Walk the hop graph; return ``{step name: {field: set(refs)}}`` -
    each step's OUTPUT stream."""
    incoming = _incoming(detail)
    states = {}
    for step in _topo_order(detail, incoming):
        feeds = incoming[step.name]
        # a stream lookup's info stream feeds the step but is not the
        # main stream - exclude it from the merge the values ride on
        if step.step_type == 'StreamLookup':
            from_step = (step.column_info or {}).get('from_step')
            feeds = [f for f in feeds if f != from_step]
        in_state = _merge([states.get(f, OrderedDict()) for f in feeds])
        states[step.name] = _transfer(step, in_state, states)
    return states


def output_column_lineage(detail):
    """``{output step name: OrderedDict(column -> sorted ref list)}``
    for every dataset-writing step, applying the step's own
    stream-to-column mapping when it declares one."""
    states = column_flow(detail)
    incoming = _incoming(detail)
    out = {}
    for step in detail.steps:
        writes_table = step.step_type == 'TableOutput' and step.table
        if not (writes_table or (step.is_file_output and step.files)):
            continue
        stream = _merge([states.get(f, OrderedDict())
                         for f in incoming[step.name]])
        info = step.column_info or {}
        mappings = info.get('mappings') or []
        columns = OrderedDict()
        if mappings:
            for column, stream_name in mappings:
                columns[column] = sorted(stream.get(stream_name, set()))
        elif step.is_file_output and info.get('fields'):
            for f in info['fields']:
                columns[f] = sorted(stream.get(f, set()))
        else:
            for field, refs in stream.items():
                columns[field] = sorted(refs)
        out[step.name] = columns
    return out

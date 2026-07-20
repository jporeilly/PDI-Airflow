# -*- coding: utf-8 -*-
# Copyright 2026 Pentaho
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Parse PDI .kjb (job) and .ktr (transformation) XML files.

Only orchestration-level information is extracted: the document name,
its named parameters, and — for jobs — the entries (sub-jobs and
transformations) plus the hops that connect them. Step-level detail
inside transformations stays in PDI, where it belongs; the generated
DAG delegates execution to Carte.
"""

from __future__ import annotations

import os
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import List, Optional

# Entry types that map to Airflow operators
TYPE_TRANS = 'TRANS'
TYPE_JOB = 'JOB'
# Entry types that are pure control flow and are dropped from the DAG
CONTROL_TYPES = {'SPECIAL', 'SUCCESS', 'DUMMY'}


@dataclass
class PdiParameter:
    name: str
    default: str = ''
    description: str = ''


@dataclass
class PdiEntry:
    name: str
    entry_type: str
    path: Optional[str] = None      # repository path (no extension)
    filename: Optional[str] = None  # file path, when file-based
    is_start: bool = False
    parallel: bool = False

    @property
    def is_executable(self):
        return self.entry_type in (TYPE_TRANS, TYPE_JOB)


@dataclass
class PdiHop:
    from_name: str
    to_name: str
    enabled: bool = True
    evaluation: bool = True      # follow on success
    unconditional: bool = False  # follow regardless of result


@dataclass
class PdiConnection:
    """A database connection defined at the transformation/job level.

    ``username`` is carried for connection provisioning; the password
    is deliberately never parsed or handled.
    """
    name: str
    db_type: str = ''            # e.g. POSTGRESQL, MYSQL, ORACLE
    server: str = ''
    port: str = ''
    database: str = ''
    username: str = ''


# File-based input/output step types we extract datasets from.
FILE_INPUT_TYPES = {
    'CsvInput', 'TextFileInput', 'ExcelInput', 'FixedInput',
    'JsonInput', 'XBaseInput',
}
FILE_OUTPUT_TYPES = {
    'TextFileOutput', 'ExcelOutput', 'ExcelWriter',
    'TypeExitExcelWriterStep', 'JsonOutput',
}


@dataclass
class PdiStep:
    name: str
    step_type: str
    # Populated for Table Input / Table Output steps (for lineage)
    connection: str = ''         # connection name it uses
    sql: str = ''                # Table Input query
    schema: str = ''             # Table Output target schema
    table: str = ''              # Table Output target table
    # Populated for file-based steps (CSV, text, Excel, JSON...)
    files: List[str] = field(default_factory=list)

    @property
    def is_file_input(self):
        return self.step_type in FILE_INPUT_TYPES

    @property
    def is_file_output(self):
        return self.step_type in FILE_OUTPUT_TYPES


@dataclass
class PdiTransDetail:
    """Step-level structure of a transformation (.ktr)."""
    name: str
    steps: List[PdiStep] = field(default_factory=list)
    hops: List[PdiHop] = field(default_factory=list)
    connections: List[PdiConnection] = field(default_factory=list)


@dataclass
class PdiDocument:
    kind: str                    # 'job' or 'transformation'
    name: str
    directory: str = '/'
    description: str = ''
    source_file: str = ''
    parameters: List[PdiParameter] = field(default_factory=list)
    entries: List[PdiEntry] = field(default_factory=list)
    hops: List[PdiHop] = field(default_factory=list)

    @property
    def repo_path(self):
        directory = self.directory.rstrip('/')
        return '{}/{}'.format(directory, self.name)

    @property
    def executable_entries(self):
        return [e for e in self.entries if e.is_executable]


def _text(node, tag, default=''):
    child = node.find(tag)
    if child is None or child.text is None:
        return default
    return child.text.strip()


def _bool(node, tag, default=False):
    val = _text(node, tag, '')
    if not val:
        return default
    return val.upper() == 'Y'


def _parse_parameters(params_node):
    parameters = []
    if params_node is None:
        return parameters
    for p in params_node.findall('parameter'):
        name = _text(p, 'name')
        if name:
            parameters.append(PdiParameter(
                name=name,
                default=_text(p, 'default_value'),
                description=_text(p, 'description')))
    return parameters


def _entry_path(entry_node, entry_type):
    """Best-effort repository path for a TRANS/JOB entry."""
    name_tag = 'transname' if entry_type == TYPE_TRANS else 'jobname'
    obj_name = _text(entry_node, name_tag)
    directory = _text(entry_node, 'directory')
    filename = _text(entry_node, 'filename')

    if obj_name:
        directory = (directory or '/').rstrip('/')
        return '{}/{}'.format(directory, obj_name), filename or None
    if filename:
        # File-based entry: derive a repo-style path from the file name,
        # stripping PDI internal variables and the extension.
        stem = os.path.splitext(os.path.basename(filename))[0]
        return stem, filename
    return None, None


def _parse_job(root, source_file):
    doc = PdiDocument(
        kind='job',
        name=_text(root, 'name'),
        directory=_text(root, 'directory', '/'),
        description=_text(root, 'description'),
        source_file=source_file,
        parameters=_parse_parameters(root.find('parameters')))

    entries_node = root.find('entries')
    if entries_node is not None:
        for entry_node in entries_node.findall('entry'):
            entry_type = _text(entry_node, 'type').upper()
            path, filename = (None, None)
            if entry_type in (TYPE_TRANS, TYPE_JOB):
                path, filename = _entry_path(entry_node, entry_type)
            doc.entries.append(PdiEntry(
                name=_text(entry_node, 'name'),
                entry_type=entry_type,
                path=path,
                filename=filename,
                is_start=(entry_type == 'SPECIAL'
                          and _bool(entry_node, 'start')),
                parallel=_bool(entry_node, 'parallel')))

    hops_node = root.find('hops')
    if hops_node is not None:
        for hop_node in hops_node.findall('hop'):
            doc.hops.append(PdiHop(
                from_name=_text(hop_node, 'from'),
                to_name=_text(hop_node, 'to'),
                enabled=_bool(hop_node, 'enabled', True),
                evaluation=_bool(hop_node, 'evaluation', True),
                unconditional=_bool(hop_node, 'unconditional')))

    return doc


def _parse_trans(root, source_file):
    info = root.find('info')
    if info is None:
        raise ValueError('Not a valid .ktr file: missing <info> element')
    return PdiDocument(
        kind='transformation',
        name=_text(info, 'name'),
        directory=_text(info, 'directory', '/'),
        description=_text(info, 'description'),
        source_file=source_file,
        parameters=_parse_parameters(info.find('parameters')))


def _step_files(step_node):
    """Extract file path(s) from a file-based step. Handles both the
    ``<filename>`` form (CSV/Fixed input) and the ``<file><name>…``
    form (text/Excel/JSON input & output, which may list several)."""
    files = []
    fn = _text(step_node, 'filename')
    if fn:
        files.append(fn)
    file_node = step_node.find('file')
    if file_node is not None:
        for name_node in file_node.findall('name'):
            if name_node.text and name_node.text.strip():
                files.append(name_node.text.strip())
        # single <file><name> already covered; also <filename> inside file
        for fn_node in file_node.findall('filename'):
            if fn_node.text and fn_node.text.strip():
                files.append(fn_node.text.strip())
    return files


def parse_trans_detail(path):
    """Parse a .ktr's step-level structure (steps + hops).

    Used for lineage emission — the DAG generator deliberately stays at
    orchestration level, but Marquez can show the inside of a
    transformation.
    """
    try:
        tree = ET.parse(path)
    except ET.ParseError as e:
        raise ValueError('Could not parse {}: {}'.format(path, e)) from e
    root = tree.getroot()
    if root.tag != 'transformation':
        raise ValueError(
            '{} is not a transformation (.ktr) file'.format(path))
    info = root.find('info')
    detail = PdiTransDetail(
        name=_text(info, 'name') if info is not None else '')

    # Connection definitions (<connection> at transformation level)
    for conn_node in root.findall('connection'):
        name = _text(conn_node, 'name')
        if name:
            detail.connections.append(PdiConnection(
                name=name,
                db_type=_text(conn_node, 'type'),
                server=_text(conn_node, 'server'),
                port=_text(conn_node, 'port'),
                database=_text(conn_node, 'database'),
                username=_text(conn_node, 'username')))

    for step_node in root.findall('step'):
        step_type = _text(step_node, 'type')
        step = PdiStep(
            name=_text(step_node, 'name'),
            step_type=step_type)
        # Capture table I/O for lineage
        if step_type == 'TableInput':
            step.connection = _text(step_node, 'connection')
            step.sql = _text(step_node, 'sql')
        elif step_type == 'TableOutput':
            step.connection = _text(step_node, 'connection')
            step.schema = _text(step_node, 'schema')
            step.table = _text(step_node, 'table')
        elif step.is_file_input or step.is_file_output:
            step.files = _step_files(step_node)
        detail.steps.append(step)

    order = root.find('order')
    if order is not None:
        for hop_node in order.findall('hop'):
            detail.hops.append(PdiHop(
                from_name=_text(hop_node, 'from'),
                to_name=_text(hop_node, 'to'),
                enabled=_bool(hop_node, 'enabled', True)))
    return detail


def parse_file(path):
    """Parse a .kjb or .ktr file into a :class:`PdiDocument`."""
    try:
        tree = ET.parse(path)
    except ET.ParseError as e:
        raise ValueError('Could not parse {}: {}'.format(path, e)) from e

    root = tree.getroot()
    if root.tag == 'job':
        return _parse_job(root, str(path))
    if root.tag == 'transformation':
        return _parse_trans(root, str(path))
    raise ValueError(
        "Unsupported PDI file {}: root element is <{}>, expected <job> "
        'or <transformation>'.format(path, root.tag))

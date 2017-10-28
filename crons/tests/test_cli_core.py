"""test_cli_core.py: tests expected behavior for global space"""
from os import path
import uuid
import dateutil.parser

import pytest
from plumbum import local
import pandas as pd
import semantic_version

import navitron_crons.exceptions as exceptions
import navitron_crons._version as _version
import navitron_crons.cli_core as cli_core

import helpers

def test_generate_metadata():
    """validate expected behavior for cli_core.append_metadata()"""
    data = cli_core.generate_metadata(
        'test',
        '0.0.0'
    )

    # Validate types
    assert isinstance(data, dict)
    assert semantic_version.Version(data['version'])
    assert semantic_version.Version(data['package_version'])
    assert uuid.UUID(data['write_recipt'])
    assert dateutil.parser.parse(data['cron_datetime'])

    # Validate expected contents
    expected_headers = [
        'write_recipt', 'data_source', 'machine_source', 'version', 'package_version',
        'cron_datetime'
    ]

    unique_values, unique_expected = helpers.find_uniques(
        list(data.keys()),
        expected_headers
    )

    assert unique_expected == []
    if unique_values:
        pytest.xfail(
            'Unexpected values from append_metadata(): {}'.format(unique_values))

python = local['python']
APP_PATH = path.join(helpers.ROOT, 'cli_core.py')
CONFIG_PATH = path.join(helpers.ROOT, 'navitron_crons.cfg')
class TestCLICore:
    """validate parent/meta plumbum cli wrapper"""
    app_command = python[APP_PATH]

    def test_dump_config(self):
        """validate --dump-config arg"""
        output = self.app_command('--dump-config')
        print(output)
        with open(CONFIG_PATH, 'r') as cfg_fh:
            raw_config = cfg_fh.read()

        assert output == raw_config + '\n'

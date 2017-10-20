"""test_cli_core.py: tests expected behavior for global space"""
from os import path

import pytest
from plumbum import local
import pandas as pd

import navitron_crons.exceptions as exceptions
import navitron_crons._version as _version
import navitron_crons.cli_core as cli_core

import helpers

def test_append_metadata():
    """validate expected behavior for cli_core.append_metadata()"""
    dummy_data = [
        {'col1': 10, 'col2': 100},
        {'col1': 20, 'col2': 200}
    ]

    dummy_df = pd.DataFrame(dummy_data)

    data = cli_core.append_metadata(
        dummy_df,
        'test',
        '0.0.0'
    )

    assert isinstance(data, pd.DataFrame)

    expected_headers = ['col1', 'col2', 'cron_datetime', 'metadata']

    unique_values, unique_expected = helpers.find_uniques(
        data.columns.values,
        expected_headers
    )

    assert unique_expected == []
    if unique_values:
        pytest.xfail(
            'Unexpected values from append_metadata(): {}'.format(unique_values))

    # TODO: validate added cols

python = local['python']
APP_PATH = path.join(helpers.ROOT, 'cli_core.py')
CONFIG_PATH = path.join(helpers.ROOT, 'navitron_crons.cfg')
class TestCLICore:
    """validate parent/meta plumbum cli wrapper"""
    app_command = python[APP_PATH]

    def test_config_dump(self):
        """validate --dump-config arg"""
        output = self.app_command('--dump-config')

        with open(CONFIG_PATH, 'r') as cfg_fh:
            raw_config = cfg_fh.read()

        print(output)
        print('----')
        print(raw_config)
        assert output == raw_config + '\n'

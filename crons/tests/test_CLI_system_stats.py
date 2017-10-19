"""test_CLI_system_stats.py: tests expected behavior for CLI application"""
from os import path

import pytest
from plumbum import local
import pandas as pd

import navitron_crons.exceptions as exceptions
import navitron_crons._version as _version
import navitron_crons.navitron_system_stats as navitron_system_stats

import helpers

app_command = local['navitron_system_stats']

def test_get_system_jumps():
    """validate expected behavior for navitron_system_stats.get_system_jumps()"""
    data = navitron_system_stats.get_system_jumps(
        config=helpers.TEST_CONFIG,
        logger=helpers.LOGGER
    )

    assert isinstance(data, pd.DataFrame)

    expected_headers = ['ship_jumps', 'system_id']

    unique_values, unique_expected = helpers.find_uniques(
        data.columns.values,
        expected_headers
    )

    assert unique_expected == []
    if unique_values:
        pytest.xfail(
            'Unexpected values from get_system_jumps(): {}'.format(unique_values))

def test_get_system_kills():
    """validates expected behavior for navitron_system_stats.get_system_kills()"""
    data = navitron_system_stats.get_system_kills(
        config=helpers.TEST_CONFIG,
        logger=helpers.LOGGER
    )

    assert isinstance(data, pd.DataFrame)

    expected_headers = ['npc_kills', 'pod_kills', 'ship_kills', 'system_id']

    unique_values, unique_expected = helpers.find_uniques(
        data.columns.values,
        expected_headers
    )

    assert unique_expected == []
    if unique_values:
        pytest.xfail(
            'Unexpected values from get_system_kills(): {}'.format(unique_values))

class TestCLI:
    """validate cli launches and works as users expect"""
    def test_help(self):
        """validate -h works"""
        output = app_command('-h')

    def test_version(self):
        """validate app name/version are as expected"""
        output = app_command('--version')

        assert output == '{app_name} {version}\n'.format(
            app_name=navitron_system_stats.__app_name__,
            version=navitron_system_stats.__app_version__
        )

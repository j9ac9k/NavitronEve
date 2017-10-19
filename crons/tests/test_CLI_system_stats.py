"""test_CLI_system_stats.py: tests expected behavior for CLI application"""
from os import path

import pytest
from plumbum import local

import navitron_crons.exceptions as exceptions
import navitron_crons._version as _version
import navitron_crons.navitron_system_stats as navitron_system_stats

app_command = local['navitron_system_stats']

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

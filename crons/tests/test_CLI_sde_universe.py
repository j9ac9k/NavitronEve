"""test_CLI_sde_universe.py: tests expected behavior for CLI application"""
from os import path

import pytest
from plumbum import local
import pandas as pd

import navitron_crons.exceptions as exceptions
import navitron_crons._version as _version
import navitron_crons.navitron_sde_universe as navitron_sde_universe

import helpers


class TestCLI:
    """validate cli launches and works as users expect"""
    app_command = local['navitron_sde_universe']

    def test_help(self):
        """validate -h works"""
        output = self.app_command('-h')

    def test_version(self):
        """validate app name/version are as expected"""
        output = self.app_command('--version')

        assert output == '{app_name} {version}\n'.format(
            app_name=navitron_sde_universe.__app_name__,
            version=navitron_sde_universe.__app_version__
        )

"""test_CLI_sde_universe.py: tests expected behavior for CLI application"""
from os import path
import json

import pytest
from plumbum import local
import pandas as pd

import navitron_crons.exceptions as exceptions
import navitron_crons._version as _version
import navitron_crons.navitron_sde_universe as navitron_sde_universe

import helpers

SAMPLE_SYSTEMS = helpers.load_samples('universe_systems_detail.json')
SAMPLE_CONSTELLATIONS = helpers.load_samples('universe_constellations_detail.json')
SAMPLE_REGIONS = helpers.load_samples('universe_regions_detail.json')
SAMPLE_STARGATES = helpers.load_samples('universe_stargates_detail.json')
def test_join_map_details():
    """validate join_map_details() happypath"""
    data_df = navitron_sde_universe.join_map_details(
        SAMPLE_SYSTEMS,
        SAMPLE_CONSTELLATIONS,
        SAMPLE_REGIONS
    )

    assert isinstance(data_df, pd.DataFrame)

    expected_cols = [
        'constellation_id', 'solarSystemName', 'solarSystemPosition', 'security_class',
        'security_status', 'star_id', 'stargates', 'system_id', 'constellationName',
        'region_id', 'regionName'
    ]

    unique_values, unique_expected = helpers.find_uniques(
        data_df.columns.values,
        expected_cols
    )

    assert unique_expected == []
    if unique_values:
        pytest.xfail(
            'Unexpected values from join_map_details(): {}'.format(unique_values))

def test_reshape_system_location():
    """validate reshape_system_location() happypath"""
    system_df = pd.DataFrame(SAMPLE_SYSTEMS)

    pivoted_df = navitron_sde_universe.reshape_system_location(
        system_df
    )

    assert isinstance(pivoted_df, pd.DataFrame)
    assert 'position' not in list(pivoted_df.columns.values)

    expected_cols = [
        'constellation_id', 'name', 'planets', 'security_class', 'security_status',
        'star_id', 'stargates', 'stations', 'system_id', 'x', 'y', 'z'
    ]

    unique_values, unique_expected = helpers.find_uniques(
        pivoted_df.columns.values,
        expected_cols
    )

    assert unique_expected == []
    if unique_values:
        pytest.xfail(
            'Unexpected values from reshape_system_location(): {}'.format(unique_values))

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

"""test_CLI_sde_universe.py: tests expected behavior for CLI application"""
from os import path
import json

import pytest
from plumbum import local
import pandas as pd
import jsonschema

import navitron_crons.exceptions as exceptions
import navitron_crons._version as _version
import navitron_crons.navitron_sde_universe as navitron_sde_universe

import helpers

SAMPLE_SYSTEMS = helpers.load_samples('universe_systems_detail.json')
SAMPLE_CONSTELLATIONS = helpers.load_samples('universe_constellations_detail.json')
SAMPLE_REGIONS = helpers.load_samples('universe_regions_detail.json')
SAMPLE_STARGATES = helpers.load_samples('universe_stargates_detail.json')

SDE_SCHEMA = {
    'type': 'array',
    'items': {
        'type': 'object',
        'properties': {
            'constellation_id': {'type': 'integer'},
            'solarsystem_name': {'type': 'string'},
            'security_class': {'type': 'string'},
            'security_status': {'type': 'number'},
            'star_id': {'type': 'integer'},
            'system_id': {'type': 'integer'},
            'constellation_name': {'type': 'string'},
            'region_id': {'type': 'integer'},
            'region_name': {'type': 'string'},
            'x': {'type': 'number'},
            'y': {'type': 'number'},
            'z': {'type': 'number'},
            'stargates': {
                'type': 'array',
                'items': {'type':'integer'}
            }
        },
        'required':[
            'constellation_id', 'solarsystem_name', 'security_class', 'security_status',
            'star_id', 'system_id', 'constellation_name', 'region_id', 'region_name',
            'x', 'y', 'z', 'stargates'
        ],
        'additionalProperties': False
    }
}
def test_join_map_details():
    """validate join_map_details() happypath"""
    data_df = navitron_sde_universe.join_map_details(
        SAMPLE_SYSTEMS,
        SAMPLE_CONSTELLATIONS,
        SAMPLE_REGIONS
    )

    assert isinstance(data_df, pd.DataFrame)

    expected_cols = [
        'constellation_id', 'solarsystem_name', 'solarsystem_position', 'security_class',
        'security_status', 'star_id', 'system_id', 'constellation_name',
        'region_id', 'region_name'
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
    system_df = system_df.rename(columns={'position':'solarsystem_position'})
    system_df = system_df.drop(['planets', 'stations', 'stargates'], axis=1)

    pivoted_df = navitron_sde_universe.reshape_system_location(
        system_df
    )

    assert isinstance(pivoted_df, pd.DataFrame)
    assert 'solarsystem_position' not in list(pivoted_df.columns.values)

    expected_cols = [
        'constellation_id', 'name', 'security_class', 'security_status',
        'star_id', 'system_id', 'x', 'y', 'z'
    ]

    unique_values, unique_expected = helpers.find_uniques(
        pivoted_df.columns.values,
        expected_cols
    )

    assert unique_expected == []
    if unique_values:
        pytest.xfail(
            'Unexpected values from reshape_system_location(): {}'.format(unique_values))

def test_join_stargate_details():
    """validate join_stargate_details() happypath"""
    systems_df = pd.DataFrame(SAMPLE_SYSTEMS)
    systems_df = systems_df.drop(['stargates'], axis=1)  # prod doesnt have 'stargates'
    data_df = navitron_sde_universe.join_stargate_details(
        systems_df,
        SAMPLE_STARGATES
    )

    assert isinstance(data_df, pd.DataFrame)

    expected_cols = [
        'constellation_id', 'name', 'planets', 'position', 'security_class',
        'security_status', 'star_id', 'stations', 'system_id', 'stargates'
    ]

    unique_values, unique_expected = helpers.find_uniques(
        data_df.columns.values,
        expected_cols
    )

    assert unique_expected == []
    if unique_values:
        pytest.xfail(
            'Unexpected values from reshape_system_location(): {}'.format(unique_values))

def test_schema_check():
    """validate outgoing schema: end-to-end test for data pivots"""
    map_df = navitron_sde_universe.join_map_details(
        SAMPLE_SYSTEMS,
        SAMPLE_CONSTELLATIONS,
        SAMPLE_REGIONS
    )

    map_df = navitron_sde_universe.reshape_system_location(map_df)

    map_df = navitron_sde_universe.join_stargate_details(
        map_df,
        SAMPLE_STARGATES
    )

    map_raw_data = map_df.to_dict(
        orient='records'
    )
    #with open('sde_universe.json', 'w') as json_fh:
    #    json.dump(map_raw_data, json_fh, indent=2)

    jsonschema.validate(map_raw_data, SDE_SCHEMA)


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

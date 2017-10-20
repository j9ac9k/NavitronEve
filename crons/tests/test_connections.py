"""test_connections.py: test database/request helpers"""
from os import path

import pytest
from plumbum import local
import pandas as pd

import navitron_crons.exceptions as exceptions
import navitron_crons._version as _version
import navitron_crons.connections as connections

import helpers

class TestMongoConnection:
    """tests for MongoConnection session manager"""
    test_conn = connections.MongoConnection(
        helpers.TEST_CONFIG,
        logger=helpers.LOGGER
    )

    expected_data = [
        {'dummy_value': 'hello world'}
    ]
    test_collection = 'test_dummy'
    def test_mongo_connection_contents(self):
        """validate test_conn has stuff inside"""
        pass

    def test_mongo_connection_query(self):
        """try to get dummy data out of test database"""
        pass

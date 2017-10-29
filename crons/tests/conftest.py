#!/usr/bin/env python3
"""configtest.py: setup pytest defaults/extensions"""

## Stolen from http://doc.pytest.org/en/latest/example/simple.html#incremental-testing-test-steps
import pytest
from os import path, environ

import helpers

HERE = path.abspath(path.dirname(__file__))
ROOT = path.abspath(path.join(path.dirname(HERE), 'navitron_crons'))

environ['PROSPER__TESTMODE'] = 'True'

## init test space ##
helpers.setup_dirs()
helpers.build_logger()
helpers.load_config()

## Allow for pytest.incremental ##
def pytest_runtest_makereport(item, call):
    if 'incremental' in item.keywords:
        if call.excinfo is not None:
            parent = item.parent
            parent._previousfailed = item

def pytest_runtest_setup(item):
    if 'incremental' in item.keywords:
        previousfailed = getattr(item.parent, '_previousfailed', None)
        if previousfailed is not None:
            pytest.xfail('previous test failed (%s)' % previousfailed.name)

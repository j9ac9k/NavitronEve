#!/usr/bin/env python3
"""configtest.py: setup pytest defaults/extensions"""

## Stolen from http://doc.pytest.org/en/latest/example/simple.html#incremental-testing-test-steps
import pytest
from os import path
import shutil

HERE = path.abspath(path.dirname(__file__))
ROOT = path.abspath(path.join(path.dirname(HERE), 'TODO'))

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

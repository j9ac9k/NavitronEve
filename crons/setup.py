#!/usr/bin/env python3
from codecs import open
import importlib
from os import path, listdir
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand

HERE = path.abspath(path.dirname(__file__))

def get_version(package_name):
    """find __version__ for making package

    Args:
        package_path (str): path to _version.py folder (abspath > relpath)

    Returns:
        (str) __version__ value

    """
    module = package_name + '._version'
    package = importlib.import_module(module)

    version = package.__version__

    return version

class PyTest(TestCommand):
    """PyTest cmdclass hook for test-at-buildtime functionality

    http://doc.pytest.org/en/latest/goodpractices.html#manual-integration

    """
    user_options = [('pytest-args=', 'a', 'Arguments to pass to pytest')]

    def initialize_options(self):
        """declare pytest CLI command"""
        TestCommand.initialize_options(self)
        self.pytest_args = [
            'Tests',
            '-rx',
            '-v',
            '--cov=' + __package_name__,
            '--cov-report=term-missing',
            '--cov-config=.coveragerc'
        ]

    def run_tests(self):
        """load commands to execute pytest

        Note:
            Import here because outside the eggs are not loaded

        """
        import shlex
        import pytest
        pytest_commands = []
        try:
            pytest_commands = shlex.split(self.pytest_args)
        except AttributeError:
            pytest_commands = self.pytest_args
        errno = pytest.main(pytest_commands)
        exit(errno)

with open('README.rst', 'r', 'utf-8') as f:
    README = f.read()

__package_name__ = 'navitron_crons'
__version__ = get_version(__package_name__)

setup(
    name=__package_name__,
    author='Ogi Moore',
    author_email='TODO',
    description='cron jobs for fetching data that feeds NavitronEve backend',
    long_description=README,
    url='https://github.com/j9ac9k/NavitronEve',
    download_url='TODO',
    version=__version__,
    license='TODO',
    classifiers=[
        'Programming Language :: Python :: 3.6'
    ],
    keywords='EVE eveonline map cron',
    packages=find_packages(),
    include_package_data=True,
    data_files=[

    ],
    package_data={
        '': ['README.rst'],
        'navitron_crons': ['navitron_crons.cfg']
    },
    entry_points={
        'console_scripts': [
            'navitron_maps=navitron_crons.navitron_maps:RunMain',
        ]
    },
    install_requires=[
        'prospercommon',
        'plumbum',
        'requests',
        'esipy'

    ],
    tests_require=[
        'pytest',
        'pytest_cov'
    ],
    extras_require={
        'dev':[
            'sphinx',
            'sphinxcontrib-napoleon'
        ]
    },
    cmdclass={
        'test': PyTest,
    }
)

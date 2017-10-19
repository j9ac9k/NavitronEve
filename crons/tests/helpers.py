"""helpers.py: global test helper functions"""
from os import path, makedirs
import shutil

import prosper.common.prosper_logging as p_logging
import prosper.common.prosper_config as p_config

import navitron_crons.cli_core as app_config

HERE = path.abspath(path.dirname(__file__))
ROOT = path.abspath(path.join(path.dirname(HERE), 'navitron_crons'))

DUMP_FOLDER = path.join(HERE, '_dumps')
LOGGER = p_logging.DEFAULT_LOGGER

TEST_CONFIG = p_config.ProsperConfig(path.join(ROOT, 'navitron_crons.cfg'))

def build_logger():
    """sets up logger for test stuff"""
    global LOGGER
    log_builder = p_logging.ProsperLogger(
        'navitron_test',
        HERE
    )
    log_builder.configure_debug_logger()

    LOGGER = log_builder.logger

def setup_dirs():
    """make sure environment is set up pre-testing"""
    if path.isdir(DUMP_FOLDER):
        shutil.rmtree(DUMP_FOLDER)

    makedirs(DUMP_FOLDER, exist_ok=True)

def load_config():
    """push config into project"""
    app_config.CONFIG = TEST_CONFIG

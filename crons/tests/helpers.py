"""helpers.py: global test helper functions"""
from os import path, makedirs
import shutil
import json

import prosper.common.prosper_logging as p_logging
import prosper.common.prosper_config as p_config

import navitron_crons.cli_core as app_config

HERE = path.abspath(path.dirname(__file__))
ROOT = path.abspath(path.join(path.dirname(HERE), 'navitron_crons'))

DUMP_FOLDER = path.join(HERE, '_dumps')
LOGGER = p_logging.DEFAULT_LOGGER

ROOT_CONFIG = p_config.ProsperConfig(path.join(ROOT, 'navitron_crons.cfg'))
TEST_CONFIG = p_config.ProsperConfig(path.join(HERE, 'navitron_test.cfg'))

def build_logger():
    """sets up logger for test stuff"""
    global LOGGER
    log_builder = p_logging.ProsperLogger(
        'navitron_test',
        DUMP_FOLDER
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
    app_config.CONFIG = ROOT_CONFIG

def find_uniques(
        test_list,
        expected_list,
        logger=LOGGER
):
    """checks for unique values between two lists.

    Args:
        test_list (:obj:`list`): values found in test
        expected_list (:obj:`list`): values expected

    Returns:
        (:obj:`list`): unique_test
        (:obj:`list`): unique_expected

    """
    unique_test = list(set(test_list) - set(expected_list))
    logger.info('Unique test vals: {}'.format(unique_test))

    unique_expected = list(set(expected_list) - set(test_list))
    logger.info('Unique expected vals: {}'.format(unique_expected))

    return unique_test, unique_expected

def load_samples(
        sample_name,
        root_dir=path.join(HERE, 'samples')
):
    """loads json samples for mock testing

    Args:
        sample_name (str): name of file (with .json)
        root_dir (str, optional): path to samples dir

    Returns:
        :obj:`list`: processed JSON

    """
    sample_filepath = path.join(root_dir, sample_name)
    print('Loading sample: {}'.format(sample_filepath))
    with open(sample_filepath, 'r') as json_fh:
        data = json.load(json_fh)

    return data

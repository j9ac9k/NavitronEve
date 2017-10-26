"""cli_core.py: basic metaclass for handling generic tool layout

Acts as global namespace + parent-framework for CLI apps

"""
from os import path
import platform
from datetime import datetime
import warnings
import uuid
import time

from plumbum import cli
from requests_futures.sessions import FuturesSession
from concurrent.futures import as_completed
import prosper.common.prosper_logging as p_logger
import prosper.common.prosper_config as p_config

import navitron_crons._version as _version

DEFAULT_LOGGER = p_logger.DEFAULT_LOGGER

HERE = path.abspath(path.dirname(__file__))
CONFIG = p_config.ProsperConfig(path.join(HERE, 'navitron_crons.cfg'))

def rate_limited(requests_per_second):
    """rate limiting decorator

    Notes:
        Stolen from: https://github.com/fuzzysteve/FuzzMarket/

    """
    minimum_interval = 1.0 / float(requests_per_second)
    def decorate(func):
        last_time_called = [0.0]
        def rate_limited_func(*args,**kargs):
            elapsed = time.clock() - last_time_called[0]
            wait_remining = minimum_interval - elapsed
            if wait_remining > 0:
                time.sleep(wait_remining)
            ret = func(*args,**kargs)
            last_time_called[0] = time.clock()
            return ret
        return rate_limited_func
    return decorate


@rate_limited(200)
def async_request(
        request_obj,
        url,
        retry
):
    """rate limited async requests

    Notes:
        Stolen from: https://github.com/fuzzysteve/FuzzMarket/

    """
    future = request_obj.get(url)
    future.url = url
    future.retry = retry
    return future

def fetch_bulk_data_async(
        base_url,
        id_list,
        workers=20,
        retry=0,
        logger=DEFAULT_LOGGER
):
    """fetch bulk data from ESI using async methods

    Notes:
        Adapted from: https://github.com/fuzzysteve/FuzzMarket/

    Args:
        base_url (str): endpoint address to map onto id_list
        id_list (:obj:`list`): list of id's for requesting
        workers (int, optional): number of async workers to apply to job
        retry (int, optional): retry failure attempts
        logger (:obj:`logging.logger`, optional): logging handle

    Returns:
        :obj:`list`: data from all enpoints

    """
    session = FuturesSession(max_workers=workers)
    logger.info('--building async request queue for: %s', base_url)
    url_list = [f'{base_url}{id_val}' for id_val in id_list]
    request_queue = []
    for url in cli.terminal.Progress(url_list):
        # logger.debug(url)  # spammy AF
        request_queue.append(async_request(
            session,
            url,
            retry
        ))

    logger.info('--reading async request results')
    results = []
    for response in as_completed(request_queue):
        result = response.result()
        result.raise_for_status()
        results.append(result.json())

    return results


def generate_metadata(
        source_name,
        source_version
):
    """if you're gonna use noSQL, you gotta have provenance!  Adds reliable metadata to records

    Args:
        source_name (str): name of source script
        source_version (str): semantic version of source script

    Returns:
        :obj:`dict`: specific metadata

    """
    now = datetime.utcnow()
    write_recipt = str(uuid.uuid1())
    metadata_obj = {
        'write_recipt': write_recipt,
        'data_source': source_name,
        'machine_source': platform.node(),
        'version': source_version,
        'package_version': _version.__version__,
        'cron_datetime': now.isoformat()
    }

    return metadata_obj

def update_which_sde_data(
        current_sde_df,
        latest_esi_df,
        index_key
):
    """validate if current table needs an update

    Args:
        current_sde_df (:obj:`pandas.DataFrame`): current data (from mongodb)
        latest_esi_df (:obj:`pandas.DataFrame`): latest data from REST/ESI
        index_key (str): name of column to match on

    Returns:
        (:obj:`list`): list of keys that need to be updated

    """
    pass

class NavitronApplication(cli.Application):
    """parent metaclass for CLI applications

    Load default args and CLI environment variables here

    """
    logger = DEFAULT_LOGGER
    config = CONFIG
    conn = None

    debug = cli.Flag(
        ['d', '--debug'],
        help='debug mode: run without writing to db'
    )

    verbose = cli.Flag(
        ['v', '--verbose'],
        help='enable verbose messaging'
    )

    @cli.switch(
        ['--config'],
        str,
        help='Override default config with a local config')
    def override_config(self, config_path):
        """override config object with local version"""
        self.config = p_config.ProsperConfig(config_path)

    @cli.switch(
        ['--dump-config'],
        help='Dump global config, for easy custom setup')
    def dump_config(self):
        """dumps config file to stdout for piping into config file"""
        with open(path.join(HERE, 'navitron_crons.cfg'), 'r') as cfg_fh:
            base_config = cfg_fh.read()

        print(base_config)
        exit()

    def load_logger(self, progname):
        """build a logging object for the script to use"""
        log_builder = p_logger.ProsperLogger(
            progname,
            self.config.get('LOGGING', 'log_path'),
            config_obj=self.config
        )
        if self.verbose:
            log_builder.configure_debug_logger()
        if not self.debug:
            try:
                log_builder.configure_discord_logger()
            except Exception:
                warnings.warn('Unable to config discord logger', RuntimeWarning)

        self.logger = log_builder.logger

if __name__ == '__main__':
    NavitronApplication.run()

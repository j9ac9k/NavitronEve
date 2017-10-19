"""cli_core.py: basic metaclass for handling generic tool layout

Acts as global namespace + parent-framework for CLI apps

"""
from os import path
import platform
from datetime import datetime

from plumbum import cli
import prosper.common.prosper_logging as p_logger
import prosper.common.prosper_config as p_config

import navitron_crons._version as _version

DEFAULT_LOGGER = p_logger.DEFAULT_LOGGER

HERE = path.abspath(path.dirname(__file__))
CONFIG = p_config.ProsperConfig(path.join(HERE, 'navitron_crons.cfg'))

def append_metadata(
        data_df,
        source_name,
        source_version
):
    """if you're gonna use noSQL, you gotta have provenance!  Adds reliable metadata to records

    Args:
        data_df (:obj:`pandas.DataFrame`): raw data to be saved
        source_name (str): name of source script
        source_version (str): semantic version of source script

    Returns:
        :obj:`pandas.DataFrame`: updated data ready to export

    """
    now = datetime.utcnow()
    metadata_obj = {
        'data_source': source_name,
        'machine_source': platform.node(),
        'version': source_version,
        'package_version': _version.__version__,
        'cron_datetime': now.isoformat()
    }

    data_df['cron_datetime'] = now.isoformat()
    data_df['metadata'] = [metadata_obj] * data_df.shape[0]  # need to copy metadata onto every row

    return data_df

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
        pass

    @cli.switch(
        ['--dump-config'],
        help='Dump global config, for easy custom setup')
    def dump_config(self):
        """dumps config file to stdout for piping into config file"""
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

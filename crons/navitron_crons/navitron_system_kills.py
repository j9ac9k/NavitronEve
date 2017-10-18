"""navitron_system_kills.py: cronjob for snapshotting /universe/system_kills/"""
from os import path
from datetime import datetime
import warnings

from plumbum import cli
import pandas as pd
import requests
import prosper.common.prosper_logging as p_logger
import prosper.common.prosper_config as p_config

import navitron_crons.exceptions as exceptions
import navitron_crons.connections as connections
import navitron_crons._version as _version

HERE = path.abspath(path.dirname(__file__))
CONFIG = p_config.ProsperConfig(path.join(HERE, 'navitron_crons.cfg'))

class NavitronSystemKills(cli.Application):
    """fetch and store /universe/system_kills/"""
    PROGNAME = 'navitron_system_kills'
    VERSION = _version.__version__

    __log_builder = p_logger.ProsperLogger(
        PROGNAME,
        CONFIG.get('LOGGING', 'log_path'),
        config_obj=CONFIG
    )
    logger = p_logger.DEFAULT_LOGGER

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

    def load_logger(self):
        """build a logging object for the script to use"""
        if self.verbose:
            self.__log_builder.configure_debug_logger()
        if not self.debug:
            try:
                self.__log_builder.configure_discord_logger()
            except Exception:
                warnings.warn('Unable to config discord logger', RuntimeWarning)

        self.logger = self.__log_builder.logger

    def main(self):
        """application runtime"""
        self.load_logger()

        self.logger.info('HELLO WORLD')

def run_main():
    """hook for running entry_points"""
    NavitronSystemKills.run()

if __name__ == '__main__':
    run_main()

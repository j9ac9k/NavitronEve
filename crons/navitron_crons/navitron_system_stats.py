"""navitron_system_kills.py: cronjob for snapshotting /universe/system_kills/"""
from os import path
from datetime import datetime
import warnings

import pandas as pd

import navitron_crons.exceptions as exceptions
import navitron_crons.connections as connections
import navitron_crons._version as _version
import navitron_crons.cli_core as cli_core

HERE = path.abspath(path.dirname(__file__))


class NavitronSystemStats(cli_core.NavitronApplication):
    """fetch and store /universe/system_kills/ & /universe/system_jumps

    Feel free to add script-specific args/vars

    """
    PROGNAME = 'navitron_system_stats'
    VERSION = _version.__version__

    def main(self):
        """application runtime"""
        self.load_logger(self.PROGNAME)

        self.logger.info('HELLO WORLD')


def run_main():
    """hook for running entry_points"""
    NavitronSystemStats.run()

if __name__ == '__main__':
    run_main()

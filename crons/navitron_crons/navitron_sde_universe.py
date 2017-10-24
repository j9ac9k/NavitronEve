"""navitron_sde.py: cronjob for updating SDE data"""
from os import path
from datetime import datetime
import warnings

import pandas as pd
from plumbum import cli

import navitron_crons.exceptions as exceptions
import navitron_crons.connections as connections
import navitron_crons._version as _version
import navitron_crons.cli_core as cli_core

HERE = path.abspath(path.dirname(__file__))

__app_version__ = _version.__version__
__app_name__ = 'navitron_sde_universe'

SDE_UNIVERSE_COLLECTION = 'sde_universe'

class NavitronSDEUniverse(cli_core.NavitronApplication):
    """fetch and store traditional SDE data

    Feel free to add script-specific args/vars

    """
    PROGNAME = __app_name__
    VERSION = __app_version__

    force = cli.Flag(
        ['f', '--force'],
        help='Force a fresh upload to mongodb: NOTE deletes existing version'
    )

    def main(self):
        """application runtime"""
        self.load_logger(self.PROGNAME)
        self.conn = connections.MongoConnection(
            self.config,
            logger=self.logger  # note: order specific, logger may not be loaded yet
        )

        self.logger.info('HELLO WORLD')

        self.logger.info('Fetching base data: /universe/systems')

        try:
            update_list = connections.fetch_current_sde(
                SDE_UNIVERSE_COLLECTION,
                self.conn,
                logger=self.logger
            )
        except exceptions.NoSDEDataFound:
            #if not pd.DataFrame() is not a valid pattern.  Check if NoneType
            self.logger.info('Setting to FORCE mode')
            self.force = True
        except Exception:
            self.logger.error(
                '%s: Unable to check SDE status',
                self.PROGNAME,
                exc_info=True
            )
            raise


def run_main():
    """hook for running entry_points"""
    NavitronSDEUniverse.run()

if __name__ == '__main__':
    run_main()

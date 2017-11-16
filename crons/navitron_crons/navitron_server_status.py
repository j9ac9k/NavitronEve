"""navitron_system_kills.py: cronjob for snapshotting /status/"""
from os import path
from datetime import datetime
import warnings

import pandas as pd
import retry

import navitron_crons.exceptions as exceptions
import navitron_crons.connections as connections
import navitron_crons._version as _version
import navitron_crons.cli_core as cli_core

HERE = path.abspath(path.dirname(__file__))

__app_version__ = _version.__version__
__app_name__ = 'navitron_server_status'

@retry.retry(tries=3, delay=300)
def get_server_status(
        config,
        logger=cli_core.DEFAULT_LOGGER
):
    """fetches system jump information from ESI

    Args:
        config (:obj:`ProsperConfig`): config with [ENDPOINTS]
        logger (:obj:`logging.logger`, optional): logging handle

    Returns:
        :obj:`dict`: raw data from ESI

    """
    logger.info('--fetching data from ESI')
    raw_data = connections.get_esi(
        config.get('ENDPOINTS', 'source'),
        config.get('ENDPOINTS', 'server_status'),
        logger=logger
    )

    return raw_data


class NavitronServerStatus(cli_core.NavitronApplication):
    """fetch and store /universe/system_kills/ & /universe/system_jumps

    Feel free to add script-specific args/vars

    """
    PROGNAME = __app_name__
    VERSION = __app_version__

    def main(self):
        """application runtime"""
        self.load_logger(self.PROGNAME)
        self.conn = connections.MongoConnection(
            self.config,
            logger=self.logger  # note: order specific, logger may not be loaded yet
        )

        self.logger.info('HELLO WORLD')

        self.logger.info('Fetching server status')
        try:
            server_status = get_server_status(
                self.config,
                logger=self.logger
            )
        except Exception:
            self.logger.error(
                '%s: Unable to fetch server_info',
                self.PROGNAME,
                exc_info=True
            )
            raise

        self.logger.info('Appending metadata')
        metadata_obj = cli_core.generate_metadata(
            self.PROGNAME,
            self.VERSION
        )

        server_status['cron_datetime'] = metadata_obj['cron_datetime']
        server_status['write_recipt'] = metadata_obj['write_recipt']

        self.logger.debug(server_status)

        self.logger.info('Pushing data to database')
        try:
            connections.dump_to_db(
                [server_status],
                self.PROGNAME,
                self.conn,
                debug=self.debug,
                logger=self.logger
            )
            connections.write_provenance(
                metadata_obj,
                self.conn,
                debug=self.debug,
                logger=self.logger
            )
        except:
            self.logger.error(
                '%s: Unable to write data to database -- Attempting to write to disk',
                self.PROGNAME,
                exc_info=True
            )
            try:
                file_name = connections.dump_to_db(
                    [server_status],
                    self.PROGNAME,
                    self.conn,
                    debug=True,
                    logger=self.logger
                )
            except Exception:  # pramga: no cover
                self.logger.critical(
                    '%s: UNABLE TO SAVE DATA',
                    self.PROGNAME,
                    exc_info=True
                )
                raise
            self.logger.error(
                '%s: saved data safely to disk: %s',
                self.PROGNAME,
                file_name
            )

        self.logger.info('%s: Complete -- Have a nice day', self.PROGNAME)

def run_main():
    """hook for running entry_points"""
    NavitronServerStatus.run()

if __name__ == '__main__':
    run_main()

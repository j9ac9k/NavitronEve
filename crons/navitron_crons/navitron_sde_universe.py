"""navitron_sde.py: cronjob for updating SDE data"""
from os import path
from datetime import datetime
import warnings
import time
from enum import Enum

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

class UniverseEndpoint(Enum):
    """enumerated types for system info"""
    systems = 'systems'
    constellations = 'constellations'
    regions = 'regions'
    stargates = 'stargates'


def get_universe_systems(
        config,
        system_id=None,
        logger=cli_core.DEFAULT_LOGGER
):
    """fetch universe/systems/ information

    Args:
        config (:obj:`ProsperConfig`): config with [ENDPOINTS]
        system_id (int, optional): systemID for specific information lookup
        logger (:obj:`loging.logger`, optional): logging handle

    Returns:
        :obj:`list` or :obj:`dict`: raw results from universe/systems/

    """
    logger.info('--fetching system data from ESI')
    if system_id:
        logger.info('--fetching details for %d', int(system_id))  # WARN: could throw TypeError
    raw_data = connections.get_esi(
        config.get('ENDPOINTS', 'source'),
        config.get('ENDPOINTS', 'systems'),
        special_id=system_id
    )
    return raw_data

def get_universe_constellations(
        config,
        constellation_id=None,
        logger=cli_core.DEFAULT_LOGGER
):
    """fetch universe/constellations/ information

    Args:
        config (:obj:`ProsperConfig`): config with [ENDPOINTS]
        constellation_id (int, optional): constellationID for specific information lookup
        logger (:obj:`loging.logger`, optional): logging handle

    Returns:
        :obj:`list` or :obj:`dict`: raw results from universe/constellations/

    """
    logger.info('--fetching constellation data from ESI')
    if constellation_id:
        logger.info('--fetching details for %d', int(constellation_id))  # WARN: could throw TypeError
    raw_data = connections.get_esi(
        config.get('ENDPOINTS', 'source'),
        config.get('ENDPOINTS', 'constellations'),
        special_id=constellation_id
    )
    return raw_data

def get_universe_regions(
        config,
        region_id=None,
        logger=cli_core.DEFAULT_LOGGER
):
    """fetch universe/constellations/ information

    Args:
        config (:obj:`ProsperConfig`): config with [ENDPOINTS]
        region_id (int, optional): regionID for specific information lookup
        logger (:obj:`loging.logger`, optional): logging handle

    Returns:
        :obj:`list` or :obj:`dict`: raw results from universe/regions/

    """
    logger.info('--fetching region data from ESI')
    if region_id:
        logger.info('--fetching details for %d', int(region_id))  # WARN: could throw TypeError
    raw_data = connections.get_esi(
        config.get('ENDPOINTS', 'source'),
        config.get('ENDPOINTS', 'regions'),
        special_id=region_id
    )
    return raw_data

def get_universe_stargates(
        config,
        stargate_id,
        logger=cli_core.DEFAULT_LOGGER
):
    """fetch universe/constellations/ information

    Args:
        config (:obj:`ProsperConfig`): config with [ENDPOINTS]
        stargate_id (int): stargateID for specific information lookup
        logger (:obj:`loging.logger`, optional): logging handle

    Returns:
        :obj:`dict`: raw results from universe/stargates/

    """
    logger.info('--fetching stargate data from ESI: %d', int(stargate_id))
    raw_data = connections.get_esi(
        config.get('ENDPOINTS', 'source'),
        config.get('ENDPOINTS', 'stargates').format(stargate_id=stargate_id)
    )
    return raw_data


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

        self.logger.info('Fetching system information')
        try:
            system_list = get_universe_systems(
                config=self.config,
                logger=self.logger
            )
            base_url = '{base_url}{endpoint}'.format(
                base_url=self.config.get('ENDPOINTS', 'source'),
                endpoint=self.config.get('ENDPOINTS', 'systems')
            )
            system_info = cli_core.fetch_bulk_data_async(
                base_url,
                system_list,
                retry=1,
                logger=self.logger
            )
        except Exception:
            self.logger.error('Unable to process system details', exc_info=True)
            raise


def run_main():
    """hook for running entry_points"""
    NavitronSDEUniverse.run()

if __name__ == '__main__':
    run_main()

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

#def get_universe_stargates(
#        config,
#        stargate_id,
#        logger=cli_core.DEFAULT_LOGGER
#):
#    """fetch universe/constellations/ information
#
#    Args:
#        config (:obj:`ProsperConfig`): config with [ENDPOINTS]
#        stargate_id (int): stargateID for specific information lookup
#        logger (:obj:`loging.logger`, optional): logging handle
#
#    Returns:
#        :obj:`dict`: raw results from universe/stargates/
#
#    """
#    logger.info('--fetching stargate data from ESI: %d', int(stargate_id))
#    raw_data = connections.get_esi(
#        config.get('ENDPOINTS', 'source'),
#        config.get('ENDPOINTS', 'stargates').format(stargate_id=stargate_id)
#    )
#    return raw_data

def parse_stargates_from_systems(
        system_details_list,
        logger=cli_core.DEFAULT_LOGGER
):
    """find all the stargate_id's and return a unique list

    Notes:
        Requires `stargates` keys in system_details list

    Args:
        system_details_list (:obj:`list`): system details from /universe/systems
        logger (:obj:`logging.logger`, optional): logging handle

    Returns:
        :obj:`list`: unique list of stargate_id's to fetch data for

    """
    logger.info('--collecting stargate ids from system info')
    stargates_full_list = []
    skip_list = []
    for system_info in cli.terminal.Progress(system_details_list):
        if 'stargates' not in system_info:
            skip_list.append({
                'name': system_info['name'],
                'system_id': system_info['system_id']
            })
            continue
        stargates = system_info['stargates']
        stargates_full_list.extend(stargates)

    if skip_list:
        logger.warning('Systems found without stargates: %s', str(skip_list))

    logger.info('--parsing down to unique list')
    stargates_list = list(set(stargates_full_list))

    logger.debug(stargates_list)
    return stargates_list


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

    all_data = cli.Flag(
        ['a', '--all'],
        help='Collect all data to assemble SDE',
        #default=True
    )

    systems = cli.Flag(
        ['s', '--systems'],
        help='Collect SYSTEMS data for SDE -- DEBUG FLAG'
    )
    constellations = cli.Flag(
        ['c', '--constellations'],
        help='Collect CONSTELLATIONS data for SDE -- DEBUG FLAG'
    )
    regions = cli.Flag(
        ['r', '--regions'],
        help='Collect REGIONS data for SDE -- DEBUG FLAG'
    )
    stargates = cli.Flag(
        ['j', '--jumps'],
        help='Collect JUMPS data for SDE -- DEBUG FLAG'
        # TODO: requires `all` or `systems`
    )

    def main(self):
        """application runtime"""
        self.load_logger(self.PROGNAME)
        self.conn = connections.MongoConnection(
            self.config,
            logger=self.logger  # note: order specific, logger may not be loaded yet
        )

        self.logger.info('HELLO WORLD')
        if self.all_data or self.systems or self.stargates:
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
            self.logger.debug(system_info[0])

        if self.all_data or self.constellations:
            self.logger.info('Fetching constellation information')
            try:
                constellation_list = get_universe_constellations(
                    config=self.config,
                    logger=self.logger
                )
                base_url = '{base_url}{endpoint}'.format(
                    base_url=self.config.get('ENDPOINTS', 'source'),
                    endpoint=self.config.get('ENDPOINTS', 'constellations')
                )
                constellation_info = cli_core.fetch_bulk_data_async(
                    base_url,
                    constellation_list,
                    retry=1,
                    logger=self.logger
                )
            except Exception:
                self.logger.error('Unable to process constellation details', exc_info=True)
                raise
            self.logger.debug(constellation_info[0])

        if self.all_data or self.regions:
            self.logger.info('Fetching region information')
            try:
                region_list = get_universe_regions(
                    config=self.config,
                    logger=self.logger
                )
                base_url = '{base_url}{endpoint}'.format(
                    base_url=self.config.get('ENDPOINTS', 'source'),
                    endpoint=self.config.get('ENDPOINTS', 'regions')
                )
                region_info = cli_core.fetch_bulk_data_async(
                    base_url,
                    region_list,
                    retry=1,
                    logger=self.logger
                )
            except Exception:
                self.logger.error('Unable to process region details', exc_info=True)
                raise
            self.logger.debug(region_info[0])

        if self.all_data or self.stargates:
            self.logger.info('Fetching stargate information')
            stargate_list = parse_stargates_from_systems(
                system_info,
                logger=self.logger
            )
            try:
                base_url = '{base_url}{endpoint}'.format(
                    base_url=self.config.get('ENDPOINTS', 'source'),
                    endpoint=self.config.get('ENDPOINTS', 'stargates')
                )
                stargate_info = cli_core.fetch_bulk_data_async(
                    base_url,
                    stargate_list,
                    retry=1,
                    workers=40,  # lots of stargates, more workers plz
                    logger=self.logger
                )
            except Exception:
                self.logger.error('Unable to process stargate details', exc_info=True)
                raise
            self.logger.debug(stargate_info[0])

        self.logger.info('Combining data in Pandas')
        # TODO


def run_main():
    """hook for running entry_points"""
    NavitronSDEUniverse.run()

if __name__ == '__main__':
    run_main()

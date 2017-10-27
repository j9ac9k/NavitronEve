"""navitron_sde.py: cronjob for updating SDE data"""
from os import path
from datetime import datetime
import warnings
import time
from enum import Enum

import pandas as pd
from plumbum import cli
from contexttimer import Timer

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


def get_universe_systems_details(
        config,
        retry=1,
        workers=20,
        logger=cli_core.DEFAULT_LOGGER
):
    """fetch universe/systems/ information

    Args:
        config (:obj:`ProsperConfig`): config with [ENDPOINTS]
        retry (int, optional): number of retries allowed
        workers (int, optional): number of async workers allowed
        logger (:obj:`loging.logger`, optional): logging handle

    Returns:
        :obj:`list`: details for all systems in /universe/systems endpoint

    Raises:
        :obj:`exceptions.FatalCLIExit`: message admins, unable to resolve required data

    """
    logger.info('--fetching system list from ESI')
    try:
        system_list, address = connections.get_esi_address(
            config.get('ENDPOINTS', 'source'),
            config.get('ENDPOINTS', 'systems'),
        )
    except Exception as err:
        logger.error('Unable to fetch bulk system list from ESI', exc_info=True)
        raise exceptions.FatalCLIExit(repr(err))
    logger.debug(len(system_list))

    logger.info('--fetching system details from ESI')
    try:
        system_info = cli_core.fetch_bulk_data_async(
            address,
            system_list,
            retry=retry,
            workers=workers,
            logger=logger
        )
    except Exception as err:
        logger.error('Unable to fetch system details from ESI', exc_info=True)
        raise exceptions.FatalCLIExit(repr(err))

    return system_info

def get_universe_constellations_details(
        config,
        retry=1,
        workers=20,
        logger=cli_core.DEFAULT_LOGGER
):
    """fetch universe/constellations/ information

    Args:
        config (:obj:`ProsperConfig`): config with [ENDPOINTS]
        retry (int, optional): number of retries allowed
        workers (int, optional): number of async workers allowed
        logger (:obj:`loging.logger`, optional): logging handle

    Returns:
        :obj:`list`: details for all constellations in /universe/constellations endpoint

    Raises:
        :obj:`exceptions.FatalCLIExit`: message admins, unable to resolve required data

    """
    logger.info('--fetching constellation list from ESI')
    try:
        constellation_list, address = connections.get_esi_address(
            config.get('ENDPOINTS', 'source'),
            config.get('ENDPOINTS', 'constellations')
        )
    except Exception as err:
        logger.error('Unable to fetch bulk constellations list from ESI', exc_info=True)
        raise exceptions.FatalCLIExit(repr(err))
    logger.debug(len(constellation_list))

    logger.info('--fetching constellation details from ESI')
    try:
        constellation_info = cli_core.fetch_bulk_data_async(
            address,
            constellation_list,
            retry=retry,
            workers=workers,
            logger=logger
        )
    except Exception as err:
        logger.error('Unable to fetch constellations details from ESI', exc_info=True)
        raise exceptions.FatalCLIExit(repr(err))

    return constellation_info

def get_universe_regions_details(
        config,
        retry=1,
        workers=20,
        logger=cli_core.DEFAULT_LOGGER
):
    """fetch universe/constellations/ information

    Args:
        config (:obj:`ProsperConfig`): config with [ENDPOINTS]
        retry (int, optional): number of retries allowed
        workers (int, optional): number of async workers allowed
        logger (:obj:`loging.logger`, optional): logging handle

    Returns:
        :obj:`list`: details for all regions in /universe/regions endpoint

    Raises:
        :obj:`exceptions.FatalCLIExit`: message admins, unable to resolve required data

    """
    logger.info('--fetching region data from ESI')
    try:
        region_list, address = connections.get_esi_address(
            config.get('ENDPOINTS', 'source'),
            config.get('ENDPOINTS', 'regions')
        )
    except Exception as err:
        logger.error('Unable to fetch bulk regions list from ESI', exc_info=True)
        raise exceptions.FatalCLIExit(repr(err))
    logger.debug(len(region_list))

    logger.info('--fetching region details from ESI')
    try:
        region_info = cli_core.fetch_bulk_data_async(
            address,
            region_list,
            retry=retry,
            workers=workers,
            logger=logger
        )
    except Exception as err:
        logger.error('Unable to fetch regions details from ESI', exc_info=True)
        raise exceptions.FatalCLIExit(repr(err))

    return region_info


def get_universe_stargates_details(
        config,
        stargate_list,
        retry=1,
        workers=20,
        logger=cli_core.DEFAULT_LOGGER
):
    """find details on all stargates in systems list

    Args:
        config (:obj:`ProsperConfig`): config with [ENDPOINTS]
        system_details_list (:obj:`list`): list of unique stargates
        retry (int, optional): number of retries allowed
        workers (int, optional): number of async workers allowed
        logger (:obj:`loging.logger`, optional): logging handle

    Returns:
        :obj:`list`: details for all stargates in /universe/stargates endpoint

    Raises:
        :obj:`exceptions.FatalCLIExit`: message admins, unable to resolve required data

    """
    base_url = '{base_url}{endpoint}'.format(
        base_url=config.get('ENDPOINTS', 'source'),
        endpoint=config.get('ENDPOINTS', 'stargates')
    )
    logger.info('--fetching stargate details from ESI')
    try:
        stargate_info = cli_core.fetch_bulk_data_async(
            base_url,
            stargate_list,
            retry=1,
            workers=40,  # lots of stargates, more workers plz
            logger=logger
        )
    except Exception as err:
        logger.error('Unable to fetch stargate details from ESI', exc_info=True)
        raise exceptions.FatalCLIExit(repr(err))

    return stargate_info

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

    #if skip_list:
    #    logger.warning('Systems found without stargates: %s', str(skip_list))

    logger.info('--parsing down to unique list')
    stargates_list = list(set(stargates_full_list))

    logger.debug(len(stargates_list))
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

        ## Fetch raw data from ESI ##
        if self.all_data or self.systems or self.stargates:
            self.logger.info('Fetching system information')
            with Timer() as system_info_timer:
                system_info = get_universe_systems_details(
                    config=self.config,
                    logger=self.logger
                )
                self.logger.info('TIMER: system_info_timer -- %s', system_info_timer)
                self.logger.debug(system_info[0])


        if self.all_data or self.constellations:
            self.logger.info('Fetching constellation information')
            with Timer() as constellation_info_timer:
                constellation_info = get_universe_constellations_details(
                    config=self.config,
                    logger=self.logger
                )
                self.logger.info('TIMER: constellation_info_timer -- %s', constellation_info_timer)
                self.logger.debug(constellation_info[0])


        if self.all_data or self.regions:
            self.logger.info('Fetching region information')
            with Timer() as region_info_timer:
                region_info = get_universe_regions_details(
                    config=self.config,
                    logger=self.logger
                )
                self.logger.info('TIMER: region_info_timer -- %s', region_info_timer)
                self.logger.debug(region_info[0])


        if self.all_data or self.stargates:
            self.logger.info('Fetching stargate information')
            with Timer() as parse_stargates_timer:
                stargate_list = parse_stargates_from_systems(
                    system_info,
                    logger=self.logger
                )
                self.logger.info('TIMER: parse_stargates_timer -- %s', parse_stargates_timer)

            with Timer() as stargate_info_timer:
                stargate_info = get_universe_stargates_details(
                    self.config,
                    stargate_list,
                    workers=40,
                    logger=self.logger
                )
                self.logger.info('TIMER: stargate_info_timer -- %s', stargate_info_timer)
                self.logger.debug(stargate_info[0])


        ## Process data into Mongo-ready shape ##
        self.logger.info('Combining data in Pandas')
        # TODO


def run_main():
    """hook for running entry_points"""
    NavitronSDEUniverse.run()

if __name__ == '__main__':
    run_main()

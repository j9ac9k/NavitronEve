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
        system_info = connections.fetch_bulk_data_async(
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
        constellation_info = connections.fetch_bulk_data_async(
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
        region_info = connections.fetch_bulk_data_async(
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
        stargate_info = connections.fetch_bulk_data_async(
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

DROP_COLS = [
    'planets', 'stations', 'constellation_position', 'systems', 'constellations',
    'description', 'stargates'
]
def join_map_details(
        system_info,
        constellation_info,
        region_info,
        drop_cols=list(DROP_COLS),
        logger=cli_core.DEFAULT_LOGGER
):
    """combine all info objects into a dataframe

    Args:
        system_info (:obj:`list`): /unierse/systems/ details
        constellation_info (:obj:`list`): /universe/constellations/ details
        region_info (:obj:`list`): /universe/regions/ details
        drop_cols (:obj:`list`, optional): list of column names to drop from dataset
        logger (:obj:`logging.logger`, optional): logging handle

    Returns:
        `pandas.DataFrame`: by-system summary of map data

    """
    logger.info('--casting map data into Pandas')
    map_df = pd.DataFrame(system_info)
    map_df = map_df.rename(
        columns={'name':'solarsystem_name', 'position':'solarsystem_position'}
    )

    constellation_df = pd.DataFrame(constellation_info)
    constellation_df = constellation_df.rename(
        columns={'name':'constellation_name', 'position':'constellation_position'}
    )

    region_df = pd.DataFrame(region_info)
    region_df = region_df.rename(
        columns={'name':'region_name'}#, 'position':'regionPosition'}
    )

    logger.info('--merging dataframes')
    map_df = map_df.merge(
        constellation_df,
        on='constellation_id',
        how='left'
    )
    map_df = map_df.merge(
        region_df,
        on='region_id',
        how='left'
    )

    logger.info('--dropping redundant columns')
    map_df = map_df.drop(drop_cols, axis=1)
    return map_df

def reshape_system_location(
        map_df,
        transform_column='solarsystem_position',
        logger=cli_core.DEFAULT_LOGGER
):
    """pivot system location into a flat key shape

    Args:
        map_df (:obj:`pandas.DataFrame`): source dataframe to transform
        transform_column (str, optional): column name to pivot
        logger (:obj:`logging.logger`, optional): logging handle

    Returns:
        `pandas.DataFrame`: updated dataframe

    """
    logger.info('--splitting off column %s', transform_column)
    pivot_df = pd.DataFrame(list(map_df[transform_column]))

    logger.info('--appending column data back onto frame')
    map_df = pd.concat([map_df, pivot_df], axis=1)

    logger.info('--dropping pivot column %s', transform_column)
    map_df = map_df.drop(transform_column, axis=1)

    return map_df

def join_stargate_details(
        map_df,
        stargate_info,
        logger=cli_core.DEFAULT_LOGGER
):
    """merge stargate information into map dataframe

    Args:
        map_df (:obj:`pandas.DataFrame`): source dataframe
        stargate_info (:obj:`list`): stargate details to merge
        logger (:obj:`logging.logger`, optional): logging handle

    Returns:
        `pandas.DataFrame`: updated dataframe

    """
    logger.info('--Reshaping stargate_info')
    reworked_stargate_info = {}
    for stargate in cli.terminal.Progress(stargate_info):
        if stargate['system_id'] not in reworked_stargate_info:
            reworked_stargate_info[stargate['system_id']] = []

        reworked_stargate_info[stargate['system_id']].append(stargate['destination']['system_id'])

    logger.info('--casting stargate info into pandas')
    stargate_df = pd.DataFrame(list(reworked_stargate_info.items()))
    stargate_df.columns = ['system_id', 'stargates']

    logger.info('--merging stargates and map data')
    map_df = map_df.merge(
        stargate_df,
        on='system_id',
        how='left'
    )

    return map_df

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
                    #workers=40,
                    logger=self.logger
                )
                self.logger.info('TIMER: stargate_info_timer -- %s', stargate_info_timer)
                self.logger.debug(stargate_info[0])


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


        ## Process data into Mongo-ready shape ##
        self.logger.info('Combining data in Pandas')
        try:
            map_df = join_map_details(
                system_info,
                constellation_info,
                region_info,
                logger=self.logger
            )
            map_df = reshape_system_location(
                map_df,
                logger=self.logger
            )
            map_df = join_stargate_details(
                map_df,
                stargate_info,
                logger=self.logger
            )
            # TODO: Drop Jove, Polaris, and w-space systems
        except Exception:
            self.logger.error(
                '%s: Unable to format SDE for mongo',
                self.PROGNAME,
                exc_info=True
            )
            raise


        ## Send data into MongoDB ##
        if not self.force:
            self.logger.warning('NOT IMPLEMENTED -- SDE UPDATE ONLY')
            self.logger.info('Setting `--force`')
            self.force = True

        if self.force:
            self.logger.warning('CLEARING EXISTING DATA')
            for seconds in cli.terminal.Progress(range(1,10)):
                time.sleep(1)
            try:
                connections.clear_collection(
                    SDE_UNIVERSE_COLLECTION,
                    self.conn,
                    debug=self.debug,
                    logger=self.logger
                )
            except Exception:
                self.logger.error(
                    '%s: Unable to clear SDE data from existing MongoDB',
                    self.PROGNAME,
                    exc_info=True
                )
                raise

        metadata_obj = cli_core.generate_metadata(
            self.PROGNAME,
            self.VERSION
        )
        map_df['write_recipt'] = metadata_obj['write_recipt']
        map_df['cron_datetime'] = metadata_obj['cron_datetime']

        self.logger.info('Pushing data to database')
        try:
            connections.dump_to_db(
                map_df,
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
        except Exception:
            self.logger.error(
                '%s: Unable to write data to database',
                self.PROGNAME,
                exc_info=True
            )
            raise

        self.logger.info('%s: Complete -- Have a nice day', self.PROGNAME)

def run_main():
    """hook for running entry_points"""
    NavitronSDEUniverse.run()

if __name__ == '__main__':
    run_main()

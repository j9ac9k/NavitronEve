"""connections.py: general tools for all cronjobs: db connection and requests"""
from os import path
import warnings

import requests
import pymongo

import navitron_crons.exceptions as exceptions
import navitron_crons.cli_core as cli_core

DEFAULT_HEADER = {
    'User-Agent': 'Navitron-cron: https://github.com/j9ac9k/NavitronEve'
}

def get_esi(
        source_route,
        endpoint_route,
        params=None,
        header=DEFAULT_HEADER,
        logger=cli_core.DEFAULT_LOGGER
):
    """request wrapper for fetching ESI data

    Args:
        source_route (str): URI for ESI connection
        endpoint_route (str): endpoint information for ESI resource
        params (:obj:`dict`, optional): params for REST request
        header (:obj:`dict`, optional): header information for request
        logger (:obj:`logging.logger`, optional): logging handler
    Returns:
        :obj:`list` JSON return from endpoint

    """
    address = '{source_route}{endpoint_route}'.format(
        source_route=source_route,
        endpoint_route=endpoint_route
    )
    logger.debug('--fetching URL: %s', address)

    req = requests.get(address, params=params, header=header)
    req.raise_for_status()
    data = req.json()

    return data

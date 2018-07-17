"""launcher/wrapper for executing CLI"""
from datetime import datetime
import os
import logging
import time
import json
import uuid

import pymongo
import pandas as pd
from plumbum import cli
import prosper.common.prosper_cli as p_cli

from . import _version, connections, exceptions

HERE = os.path.abspath(os.path.dirname(__file__))
PROGNAME = 'dump_database'

def dump_increment(
        data,
        data_name,
        drop_cols=('_id'),
        folder_path='temp'
):
    """dump data to disk incrementally

    Args:
        data (list): Current collection of raw data from MongoDB
        data_name (str): name of outfile ('csv')
        drop_cols (tuple): columns to exclude from final data push
        folder_path (str): Path to dump-folder for partial records

    Returns:
        str: name of file dumped (data_name + UUID)

    """
    os.makedirs(folder_path, exist_ok=True)
    file_name = os.path.join(
        folder_path,
        os.path.basename(data_name).replace('.csv', str(uuid.uuid1()) + '.json')
    )
    df = pd.DataFrame(data).drop(drop_cols, axis=1)
    df.to_json(file_name, orient='records')

    return file_name

def zip_results(
        file_list,
        outfile,
        debug=False,
        logger=logging.getLogger(PROGNAME),
):
    """combine partial results into one big file

    Notes:
        expects `orient='records'` json format
        will delete files if `debug=False`

    Args:
        file_list (list): a list of .JSON files to combine together
        outfile (str): filepath to write results to
        debug (bool): toggle file deletion
        logger (:obj:`logging.logger`): logging handle

    """
    data = pd.DataFrame()
    for file in cli.terminal.Progress(file_list):
        data = pd.concat(
            [data, pd.read_json(file, orient='records')], ignore_index=True
        )
        if not debug:
            os.remove(file)

    logger.info('Dumping file: %s', outfile)
    data.to_csv(outfile)


class DumpDatabaseCLI(p_cli.ProsperApplication):
    PROGNAME = PROGNAME
    VERSION = _version.__version__

    config_path = os.path.join(HERE, 'navitron_crons.cfg')

    dump_rate = cli.SwitchAttr(
        ['--dump'],
        int,
        help='dump rows after how many rows',
        default=os.environ.get('NAVITRON_dump_database__dump_rate', 10000),
    )

    database = cli.SwitchAttr(
        ['--database'],
        str,
        help='Name of mongo database',
        default=os.environ.get('NAVITRON_dump_database__database', 'navitron'),
    )

    sleep = cli.SwitchAttr(
        ['--sleep'],
        int,
        help='Safety rail: time to sleep before stomping files',
        default=os.environ.get('NAVITRON_dump_database__sleep', 2),
    )

    def main(self):
        """the magic goes here"""
        self.logger.info('hello world')
        now = datetime.utcnow()
        collections = self.config.get_option(PROGNAME, 'collections').strip().splitlines()
        self.logger.debug(collections)

        self.logger.info('connecting to mongo')
        mongo_conn = pymongo.MongoClient(
            self.config.get_option('MONGO', 'hostname'),
            int(self.config.get_option('MONGO', 'port')),
            username=self.config.get_option('MONGO', 'username'),
            password=self.config.get_option('MONGO', 'password')
        )
        for collection in collections:
            self.logger.info('fetching contents from: %s', collection)
            data = mongo_conn[self.database][collection].find({})
            file_name = 'navitron_{collection}_{date}.csv'.format(
                collection=collection.replace('_', '-'),
                date=now.strftime('%Y-%m-%d')
            )
            self.logger.info('--priming dump file: %s', file_name)
            if os.path.isfile(file_name):
                self.logger.warning('--deleting file: %s', file_name)
                time.sleep(self.sleep)
                os.remove(file_name)

            count = 0
            raw = []
            results = []
            for row in cli.terminal.Progress(data, length=data.count()):
                count += 1
                raw.append(row)

                if count >= self.dump_rate:
                    results.append(dump_increment(raw, file_name))
                    count = 0
                    raw = []

            results.append(dump_increment(raw, file_name))

            self.logger.info('--zipping up results')
            zip_results(
                results,
                file_name,
                debug=self.debug,
            )

def run_main():
    """entry-point wrapper"""
    DumpDatabaseCLI.run()

if __name__ == '__main__':
    DumpDatabaseCLI.run()

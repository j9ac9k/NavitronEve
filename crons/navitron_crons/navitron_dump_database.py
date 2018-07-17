"""launcher/wrapper for executing CLI"""
from datetime import datetime
import os
import logging
import time
import json

import pymongo
import pandas as pd
from plumbum import cli
import prosper.common.prosper_cli as p_cli

from . import _version, connections, exceptions

HERE = os.path.abspath(os.path.dirname(__file__))
PROGNAME = 'dump_database'

def dump_progress(
        file_name,
        raw,
        drop_cols=('_id', 'write_recipt'),
        logger=logging.getLogger(PROGNAME),
):
    """TODO"""
    df = pd.DataFrame(raw)
    #logger.debug(df.columns.values)
    df.drop(drop_cols, axis=1, inplace=True)

    try:
        archive_df = pd.read_csv(file_name)
        archive_df.append(df, ignore_index=True)
    except FileNotFoundError:
        archive_df = df

    archive_df.to_csv(file_name, index=False)

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
            self.logger.info('priming dump file: %s', file_name)
            if os.path.isfile(file_name):
                self.logger.warning('deleting file: %s', file_name)
                time.sleep(10)
                os.remove(file_name)

            count = 0
            raw = []
            for row in cli.terminal.Progress(data, length=data.count()):
                count += 1
                raw.append(row)

                if count >= self.dump_rate:
                    #self.logger.info('dropping progress')
                    dump_progress(file_name, raw)
                    count = 0
                    raw = []
            try:
                dump_progress(file_name, raw)
            except Exception:
                self.logger.warning('Failed to dump final data blob', exc_info=True)
                with open(file_name.replace('csv', '_ERR.json')) as j_fh:
                    json.dump(raw, j_fh)

def run_main():
    """entry-point wrapper"""
    DumpDatabaseCLI.run()

if __name__ == '__main__':
    DumpDatabaseCLI.run()

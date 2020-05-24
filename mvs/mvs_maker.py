import argparse
import logging
import os

from psycopg2.extras import DictCursor

from covid_utils import logs
from covid_utils import connect
from covid_utils import credentials
from load_data import local_config

class SQLizer(object):
    def __init__(self, local=True):
        logs.configure_logging('SQLzier')
        self.logger = logging.getLogger()

        self.file_root = os.path.expanduser(local_config.path_to_this_repo)

        self.connect_to_postgres(local)

    def connect_to_postgres(self, local=True):
        self.logger.info("Connecting to postgres..")
        self.pg_creds = credentials.get_postgres_creds(local)
        self.cxn = connect.dbconn(self.pg_creds)
        self.cursor = self.cxn.cursor(cursor_factory=DictCursor)
        self.logger.info("Connected to postgres at {}.".format(self.pg_creds['host']))

    def run_sql_file(self, filepath):
        self.full_filepath = f'{self.file_root}/mvs/{filepath}'
        self.sql_statements = open(self.full_filepath, 'r').read()

        self.logger.info(f"Running the following statement:\n{self.sql_statements}")
        self.cursor.execute(self.sql_statements)
        self.cxn.commit()
        self.logger.info("Committed to database.")

if __name__ == "__main__":

    args = argparse.ArgumentParser()
    args.add_argument('-s', '--sql_file', help='SQL file to run')
    argz = args.parse_args()

    sql_file = argz.sql_file
    local = False
    if sql_file is None:
        local = True
        sql_file = f'nyt_county_geo.sql'

    sqler = SQLizer(local)
    sqler.run_sql_file(sql_file)

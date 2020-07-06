import csv
import os
import logging

import psycopg2
from psycopg2.extras import DictCursor

from covid_utils import local_config
from covid_utils import logs
from load_data import load_utils
from covid_utils import connect
from covid_utils import credentials

class DataLoader(object):
    def __init__(self, schema='nytimes', local=True):
        logs.configure_logging(f'{schema.capitalize()}DataLoader')
        self.logger = logging.getLogger()

        self.schema = schema

        self.github_path = local_config.github_paths[schema]
        self.file_root = os.path.expanduser(self.github_path)

        self.connect_to_postgres(local)

    def pull_new_data(self):
        self.logger.info("Pulling newest data.")
        os.chdir(self.file_root)
        stream = os.popen('git pull')
        self.logger.info(f'{stream.read()}')
        self.logger.info("Newest data pulled.")

    def connect_to_postgres(self, local=True):
        self.logger.info("Connecting to postgres..")
        self.pg_creds = credentials.get_postgres_creds(local)
        self.cxn = connect.dbconn(self.pg_creds)
        self.cursor = self.cxn.cursor(cursor_factory=DictCursor)
        self.logger.info("Connected to postgres at {}.".format(self.pg_creds['host']))

    def get_most_recent_date(self, table, date_column='date'):
        self.logger.info(f"Appending new data to {table}. First getting most recent data...")
        self.cursor.execute(f"SELECT max({date_column}) FROM {self.schema}.{table};")
        self.recent_date = self.cursor.fetchall()[0][0]

    def check_table_exists(self, table):
        self.cursor.execute(f"""SELECT EXISTS (
                               SELECT FROM information_schema.tables
                               WHERE  table_schema = '{self.schema}'
                               AND    table_name   = '{table}'
                               );""")
        results = self.cursor.fetchone()
        result = results[0]
        self.logger.info(f"Table already exists: {result}")
        return result

    def fully_load_table(self, data_to_load, data_header, table):
        self.logger.info(f"Initializing full load of {table}...")
        self.cursor.copy_from(data_to_load, f'{self.schema}.{table}', sep=',', null="", columns=data_header)
        self.cxn.commit()

        self.logger.info("Loaded table fully...")
        self.cursor.execute(f"SELECT count(*) FROM {self.schema}.{table};")
        cnt = self.cursor.fetchall()
        self.logger.info(f'...meaning {cnt[0][0]} rows.')

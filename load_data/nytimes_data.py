import csv
import os
import logging

import psycopg2
from psycopg2.extras import DictCursor
import pandas as pd

from covid_utils import logs
from covid_utils import connect
from covid_utils import credentials
from load_data import local_config

class NTYDataLoader(object):
    def __init__(self):
        logs.configure_logging('NTYDataLoader')
        self.logger = logging.getLogger()

        self.schema = 'nytimes'
        self.file_root = os.path.expanduser(local_config.nytimes_github_path)

        self.connect_to_postgres()

    def pull_new_data(self):
        self.logger.info("Pulling newest data.")
        os.chdir(self.file_root)
        stream = os.popen('git pull')
        self.logger.info(f'{stream.read()}')
        self.logger.info("Newest data pulled.")

    def connect_to_postgres(self):
        self.logger.info("Connecting to postgres..")
        self.pg_creds = credentials.get_postgres_creds()
        self.cxn = connect.dbconn(self.pg_creds)
        self.cursor = self.cxn.cursor(cursor_factory=DictCursor)
        self.logger.info("Connected to postgres.")

    def get_most_recent_date(self, table, date_column='date'):
        self.logger.info(f"Appending new data to {table}. First getting most recent data...")
        self.cursor.execute(f"SELECT max({date_column}) FROM {self.schema}.{table};")
        self.recent_date = self.cursor.fetchall()[0][0]

    def load_data(self, table, data_filename, date_column='date'):
        if self.recent_date is None:
            self.logger.info("Accessing full reload data..")
            data_to_load = open(f'{self.file_root}/{data_filename}', 'r')
            data_header = next(data_to_load).strip().split(',')

            self.logger.info(f"Initializing full load of {table}...")
            self.cursor.copy_from(data_to_load, f'{self.schema}.{table}', sep=',', null="", columns=data_header)
            self.cxn.commit()

            self.logger.info("Loaded table fully...")
            self.cursor.execute(f"SELECT count(*) FROM {self.schema}.{table};")
            cnt = self.cursor.fetchall()
            self.logger.info(f'...meaning {cnt[0][0]} rows.')
        else:
            self.logger.info(f"Initializing incremental load of {table} new rows beginning with the day after {self.recent_date}.")

            self.logger.info("Slicing data...")
            full_data_df = pd.read_csv(f'{self.file_root}/{data_filename}')
            insert_df = full_data_df[full_data_df[f'{date_column}'] > str(self.recent_date)]
            self.logger.info(f"Going to append a df of {len(insert_df)} rows using pandas.")

            self.logger.info("Connecting to Postgres via SQLAlchemy for pandas.")
            self.pd_cxn = connect.pandas_dbconn(self.pg_creds)

            self.logger.info("Appending rows...")
            insert_df.to_sql(f'{table}', self.pd_cxn, schema=f'{self.schema}', if_exists='append', index=False, method='multi')

            self.logger.info("Done appending.")


if __name__ == "__main__":

    tables_to_load = {
        'cases_by_county': 'us-counties.csv',
        'cases_by_state': 'us-states.csv'
    }

    nyt = NTYDataLoader()
    nyt.pull_new_data()
    for table, filename in tables_to_load.items():
        nyt.get_most_recent_date(table)
        nyt.load_data(table, filename)

import csv
import os
import logging

import psycopg2
from psycopg2.extras import DictCursor
import pandas as pd

from psycopg2.extensions import register_adapter, AsIs
import numpy

from covid_utils import logs
from covid_utils import connect
from covid_utils import credentials
from load_data import local_config

def addapt_numpy_int64(numpy_int64):
    return(AsIs(numpy_int64))

class NTYDataLoader(object):
    def __init__(self, local=True):
        logs.configure_logging('NTYDataLoader')
        self.logger = logging.getLogger()

        self.schema = 'nytimes'
        self.file_root = os.path.expanduser(local_config.nytimes_github_path)

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

    def load_data(self, table, data_filename, exists=True, date_column='date'):
        self.logger.info("Accessing full reload data..")
        full_filename = f'{self.file_root}/{data_filename}'
        data_to_load = open(full_filename, 'r')
        data_header = next(data_to_load).strip().split(',')

        if exists is False:
            self.logger.info("Connecting to Postgres via SQLAlchemy for pandas.")
            self.pd_cxn = connect.pandas_dbconn(self.pg_creds)
            self.pd_dataframe = pd.read_csv(full_filename)
            sliced_dataframe = self.pd_dataframe.truncate(after=0)
            self.logger.info(f"Creating table using this df template: {sliced_dataframe}")

            self.logger.info("Creating table...")
            sliced_dataframe.to_sql(f'{table}', self.pd_cxn, schema=f'{self.schema}', if_exists='replace', index=False, method='multi')
            self.logger.info("Created table.")

            self.logger.info("Clearing table for full reload.")
            self.cursor.execute(f"""TRUNCATE {self.schema}.{table};""")

            self.fully_load_table(data_to_load, data_header, table)

        else:
            self.get_most_recent_date(table)
            if self.recent_date is None:
                self.fully_load_table(data_to_load, data_header, table)
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

    register_adapter(numpy.int64, addapt_numpy_int64)
    tables_to_load = {
        'cases_by_county': 'us-counties.csv',
        'cases_by_state': 'us-states.csv'
    }

    nyt = NTYDataLoader(False)
    nyt.pull_new_data()
    for table, filename in tables_to_load.items():
        table_exists = nyt.check_table_exists(table)
        nyt.load_data(table, filename, exists=table_exists)

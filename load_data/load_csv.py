import csv
import os
import logging
import argparse

import psycopg2
from psycopg2.extras import DictCursor
import pandas as pd

from psycopg2.extensions import register_adapter, AsIs
import numpy

from covid_utils import logs
from covid_utils import connect
from covid_utils import credentials
from covid_utils import local_config

class StaticCSVLoader(object):
    def __init__(self, local=True):
        logs.configure_logging('StaticCSVLoader')
        self.logger = logging.getLogger()

        self.schema = 'aux'
        self.file_root = os.path.expanduser(local_config.path_to_this_repo)
        self.connect_to_postgres(local)

    def connect_to_postgres(self, local=True):
        self.logger.info("Connecting to postgres..")
        self.pg_creds = credentials.get_postgres_creds(local)
        self.cxn = connect.dbconn(self.pg_creds)
        self.cursor = self.cxn.cursor(cursor_factory=DictCursor)
        self.logger.info("Connected to postgres at {}.".format(self.pg_creds['host']))

    def configure_csv(self, filename):
        self.full_filename = f'{self.file_root}/{filename}'
        self.logger.info(f"Reading from {self.full_filename}")
        self.table_name = self.full_filename.split('/')[-1].split('.')[0]
        self.logger.info(f"Creating the table {self.schema}.{self.table_name}.")

    def load_data(self):
        self.logger.info("Loading table.")

        self.logger.info("Connecting to Postgres via SQLAlchemy for pandas.")
        self.pd_cxn = connect.pandas_dbconn(self.pg_creds)
        self.pd_dataframe = pd.read_csv(self.full_filename)

        self.logger.info("Creating table...")
        self.pd_dataframe.to_sql(f'{self.table_name}', self.pd_cxn, schema=f'{self.schema}', if_exists='replace', index=False, method='multi')
        self.logger.info("Created table.")

        self.cursor.execute(f"SELECT count(*) FROM {self.schema}.{self.table_name}")
        results = self.cursor.fetchone()[0]
        self.logger.info(f"Table now has {results} rows.")

if __name__ == "__main__":

    args = argparse.ArgumentParser()
    args.add_argument('-l', '--load_file', help="relative filename of file to load")
    argz = args.parse_args()

    load_file = argz.load_file
    if load_file is None:
        load_file = 'static/fips_to_latlng.csv'

    reader = StaticCSVLoader(local=False)

    reader.configure_csv(load_file)
    reader.load_data()

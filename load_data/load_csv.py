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

class StaticCSVLoader(object):
    def __init__(self, env='local'):
        logs.configure_logging('StaticCSVLoader')
        self.logger = logging.getLogger()

        self.env = env
        if self.env == 'local':
            from config import local as env_config
        else:
            from config import heroku as env_config

        self.data_repo_path = env_config.data_repo_path
        self.path_to_this_repo = env_config.path_to_this_repo

        self.schema = 'aux'
        self.file_root = os.path.expanduser(self.path_to_this_repo)
        self.connect_to_postgres()

    def connect_to_postgres(self):
        self.logger.info("Connecting to postgres..")
        self.pg_creds = credentials.get_postgres_creds(self.env)
        self.cxn = connect.dbconn(self.pg_creds, self.env)
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
        self.pd_cxn = connect.pandas_dbconn(self.pg_creds, self.env)
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
    args.add_argument('-e', '--env', help='prod or local')
    argz = args.parse_args()

    load_file = argz.load_file
    if load_file is None:
        load_file = 'static/fips_to_latlng.csv'

    reader = StaticCSVLoader(env=argz.env)

    reader.configure_csv(load_file)
    reader.load_data()

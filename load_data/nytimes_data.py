import argparse

import pandas as pd

from psycopg2.extensions import register_adapter, AsIs
import numpy

from covid_utils import connect
from load_data import load_utils

def addapt_numpy_int64(numpy_int64):
    return(AsIs(numpy_int64))

class NTYDataLoader(load_utils.DataLoader):
    def __init__(self, local=True):
        super().__init__(schema='nytimes')

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

    # deal with numpy interpolation
    register_adapter(numpy.int64, addapt_numpy_int64)

    args = argparse.ArgumentParser()
    args.add_argument('-t', '--table', help='table to refresh')
    args.add_argument('-f', '--filename', help='path to filename')
    argz = args.parse_args()

    table = argz.table
    filename = args.filename
    local = False
    if table is None:
        local = True
        table = f'cases_by_county'
        filename = f'us-counties.csv'

    nyt = NTYDataLoader(False)
    nyt.pull_new_data()
    table_exists = nyt.check_table_exists(table)
    nyt.load_data(table, filename, exists=table_exists)

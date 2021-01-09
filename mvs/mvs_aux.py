from covid_utils import logs
from covid_utils import connect
from covid_utils import credentials
from covid_utils import local_config
from mvs import mvs_config

import logging

from psycopg2.extras import DictCursor
import pandas as pd
import arrow

class MVSAuxTransformer(object):
    def __init__(self, mv_name, env='local'):
        logs.configure_logging('AuxTransformer')
        self.logger = logging.getLogger()

        self.env = env
        self.mv_name = mv_name
        self.logger.info(f"Transforming {self.mv_name} further.")

        self.connect_to_postgres()

    def connect_to_postgres(self):
        self.logger.info("Connecting to postgres..")
        self.pg_creds = credentials.get_postgres_creds(self.env)
        self.cxn = connect.dbconn(self.pg_creds, self.env)
        self.cursor = self.cxn.cursor(cursor_factory=DictCursor)
        self.pd_cxn = connect.pandas_dbconn(self.pg_creds, self.env)
        self.logger.info("Connected to postgres at {}.".format(self.pg_creds['host']))          

    def fetch_data(self):
        grab_table_sql = f"""SELECT * FROM {mvs_config.config[self.mv_name]['output_schema']}.{mvs_config.config[self.mv_name]['output_table']}"""
        self.initial_df = pd.read_sql(grab_table_sql, self.pd_cxn)

    def execute_transformations(self):
        getattr(self, f"{self.mv_name}_transformations")()

    def nyt_daily_by_state_transformations(self):
        self.logger.info("Executing transformations for nyt_daily_by_state table.")
        self.states_included = self.initial_df.state.unique()

        self.minimum_global_date = self.initial_df.date.min()
        self.arrowed_min_global_date = arrow.get(str(self.minimum_global_date), 'YYYY-MM-DD')
        self.logger.info(f"Min global dataset date is {self.minimum_global_date}")

        for state in self.states_included:
            minimum_state_date = self.initial_df[self.initial_df.state == state].min()
            arrowed_min_date = arrow.get(str(minimum_state_date), 'YYYY-MM-DD').shift(days=-1)
            dates_to_add = arrow.Arrow.range('day', self.arrowed_min_global_date, arrowed_min_date)
            self.logger.info(f"Filling in blank rows from {state}, up to {arrowed_min_date}")

            state_list = []
            for date in dates_to_add:
                state_list.append({
                    'state': state,
                    'date': date.format('YYYY-MM-DD'),
                    'deaths': 0,
                    'cases': 0
                })

            state_df = pd.DataFrame(state_list)

            self.logger.info(f"Appending filled in dataframe of {len(state_df)} rows to initial DF.")
            self.initial_df = pd.concat([self.initial_df, state_df])

        self.logger.info("Done transforming nyt_daily_by_state.")

        exist_already = pd.read_sql(f"SELECT date, state FROM {mvs_config.config[self.mv_name]['output_schema']}.{mvs_config.config[self.mv_name]['output_table']}", self.pd_cxn)
        merged = self.initial_df.merge(exist_already, on=['date', 'state'], how='left', indicator=True)

        to_insert = merged[merged['_merge']=='left_only'].drop(['_merge'], axis=1)
        self.logger.info(f"Will insert {len(to_insert)} new rows.")

        self.logger.info("Upserting data into the MV.")
        to_insert.to_sql(f"{mvs_config.config[self.mv_name]['output_table']}", self.pd_cxn, schema=f"{mvs_config.config[self.mv_name]['output_schema']}", if_exists='append', index=False, method='multi')

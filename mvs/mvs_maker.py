import argparse
import logging
import os

from psycopg2.extras import DictCursor

from covid_utils import logs
from covid_utils import connect
from covid_utils import credentials

from mvs import mvs_config
from mvs import mvs_aux

class SQLizer(object):
    def __init__(self, mv_name, env='local'):
        logs.configure_logging('SQLzier')
        self.logger = logging.getLogger()

        self.env = env
        if self.env == 'local':
            from config import local as env_config
        else:
            from config import heroku as env_config

        self.data_repo_path = env_config.data_repo_path
        self.path_to_this_repo = env_config.path_to_this_repo
        self.file_root = os.path.expanduser(self.path_to_this_repo)

        self.connect_to_postgres()
        self.mv_name = mv_name
        self.mv_config = mvs_config.config[self.mv_name]

    def connect_to_postgres(self,):
        self.logger.info("Connecting to postgres..")
        self.pg_creds = credentials.get_postgres_creds(self.env)
        self.cxn = connect.dbconn(self.pg_creds, self.env)
        self.cursor = self.cxn.cursor(cursor_factory=DictCursor)
        self.logger.info("Connected to postgres at {}.".format(self.pg_creds['host']))

    def run_sql_file(self):
        self.full_filepath = f"{self.file_root}/mvs/{self.mv_config['sql_file']}"
        self.sql_statements = open(self.full_filepath, 'r').read()

        self.logger.info(f"Running the following statement:\n{self.sql_statements}")
        self.cursor.execute(self.sql_statements)
        self.cxn.commit()
        self.logger.info("Committed to database.")

    def aux_transformations(self):
        if self.mv_config['aux_alterations'] is True:
            self.logger.info("Running aux transformations.")
            self.transformer = mvs_aux.MVSAuxTransformer(self.mv_name)
            self.transformer.fetch_data()
            self.transformer.execute_transformations()
        else:
            self.logger.info("No aux transformations needed.")

if __name__ == "__main__":

    args = argparse.ArgumentParser()
    args.add_argument('-m', '--mvs_to_build', help='name of MV to build')
    argz = args.parse_args()

    mv_to_build = argz.mvs_to_build
    local = True
    if mv_to_build is None:
        local = True
        mv_to_build = f'nyt_daily_by_state'

    sqler = SQLizer(mv_to_build, local)
    sqler.run_sql_file()
    sqler.aux_transformations()

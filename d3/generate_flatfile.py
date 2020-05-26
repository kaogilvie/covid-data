import logging
import argparse

from covid_utils import logs
from covid_utils import connect
from covid_utils import credentials
from load_data import local_config

LOCAL = True

class FlatFileGenerator(object):
    def __init__(self, schema, table, local=True):
        logs.configure_logging('FFGenerator')
        self.logger = logging.getLogger()

        self.schema = schema
        self.file_root = os.path.expanduser(local_config.path_to_this_repo)

        self.connect_to_postgres(local)

    def connect_to_postgres(self, local=True):
        self.logger.info("Connecting to postgres..")
        self.pg_creds = credentials.get_postgres_creds(local)
        self.cxn = connect.dbconn(self.pg_creds)
        self.cursor = self.cxn.cursor(cursor_factory=DictCursor)
        self.logger.info("Connected to postgres at {}.".format(self.pg_creds['host']))

if __name__ == "__main__":

    arguments = argparse.ArgumentParser()
    arguments.add_argument('-s', '--schema', help='schema to pull MV from')
    arguments.add_argument('-t', '--table', help='name of table')
    args = arguments.parse_args()

    schema = args.schema
    table = args.table
    if schema is None:
        schema = 'nytimes'
        table = 'cases_state_geo'

    ff = FlatFileGenerator(schema, table, LOCAL)

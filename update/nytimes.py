from load_data import nytimes_data as times
from mvs import mvs_maker as mv
from covid_utils import logs
from d3 import generate_flatfile as ff

import logging

# NYTIMES CONFIG
dynamic_tables = {
    'total_cases_by_county': 'us-counties.csv',
    'total_cases_by_state': 'us-states.csv'
}

mvs_files = [
    'nyt_county_geo.sql'
]

flat_files = [
    'totals_by_state',
    'daily_by_state'
]

LOCAL = True

if __name__ == "__main__":

    logs.configure_logging('NYTUpdater')
    logger = logging.getLogger()

    logger.info("Refreshing all NYTimes data.")

    nyt = times.NTYDataLoader(LOCAL)
    nyt.pull_new_data()
    for table, filename in dynamic_tables.items():
        logger.info(f"Working on {table}")
        table_exists = nyt.check_table_exists(table)
        nyt.load_data(table, filename, exists=table_exists)

    # refresh all NYTimes MVS
    logger.info("Refreshing all NYTimes based MVs.")

    sqler = mv.SQLizer(LOCAL)

    for file in mvs_files:
        logger.info(f"Working on {file}")
        sqler.run_sql_file(file)

    logger.info("Refreshing all NYTimes based flat files.")

    flatFiler = ff.FlatFileGenerator(LOCAL)
    for sql in flat_files:
        flatFiler.fetch_data(sql)
        flatFiler.write_csv()

from git import Repo
import os

repo_path = '/Users/kogilvie/Documents/github/local-covid-data/'

folder_exclusions = [
    '.git',
    'postgres',
    '__pycache__'
]
file_exclusions = [
    '.DS_Store'
]

file_path_list = []
for dirpath, subdirs, files in os.walk(repo_path):
    continued = False
    for exclusion in exclusions:
        if dirpath.find(exclusion) != -1:
            continued = True
    if continued is True:
        continue
    for x in files:
        if x in file_exclusions:
            continue
        file_path_list.append(os.path.join(dirpath, x))



covid_repo = Repo('/Users/kogilvie/Documents/github/local-covid-data/')
for file in filepath_list:
    covid_repo.index.add(file)

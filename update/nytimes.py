from load_data import nytimes_data as times
from mvs import mvs_maker as mv
from covid_utils import logs
from d3 import generate_flatfile as ff

import logging
from datetime import datetime
import os

from git import Repo

# NYTIMES CONFIG
dynamic_tables = {
    'total_cases_by_county': 'us-counties.csv',
    'total_cases_by_state': 'us-states.csv'
}

mvs_to_build = [
    'nyt_daily_by_state',
    'nyt_totals_by_state'
]

flat_files = [
    'totals_by_state',
    'daily_by_state'
]

folder_exclusions = [
    '.git',
    'postgres',
    '__pycache__'
]

file_exclusions = [
    '.DS_Store'
]

repo_path = '/Users/kogilvie/Documents/github/local-covid-data/'

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

    logger.info("Refreshing all NYTimes based MVs.")

    for mv_to_build in mvs_to_build:
        sqler = mv.SQLizer(mv_to_build, LOCAL)
        logger.info(f"Working on {mv_to_build}")
        sqler.run_sql_file()
        sqler.aux_transformations()

    logger.info("Refreshing all NYTimes based flat files.")

    flatFiler = ff.FlatFileGenerator(LOCAL)
    for sql in flat_files:
        flatFiler.fetch_data(sql)
        flatFiler.write_csv()

    logger.info("Getting all tracked files.")
    file_path_list = []
    for dirpath, subdirs, files in os.walk(repo_path):
        continued = False
        for exclusion in folder_exclusions:
            if dirpath.find(exclusion) != -1:
                continued = True
        if continued is True:
            continue
        for x in files:
            if x in file_exclusions:
                continue
            file_path_list.append(os.path.join(dirpath, x))

    logger.info("Adding files to index.")
    covid_repo = Repo(repo_path)
    for file in file_path_list:
        covid_repo.index.add(file)

    logger.info("Committing files.")
    covid_repo.index.commit(f"Data update {datetime.now()}")

    logger.info("Pushing updates to remote.")
    covid_origin = covid_repo.remotes.origin
    covid_origin.push()

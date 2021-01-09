from load_data import nytimes_data as times
from mvs import mvs_maker as mv
from covid_utils import logs
from covid_utils import local_config
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
    'daily_by_state',
    'daily_countrywide',
    'totals_countrywide'
]

file_path_list = [
    '/Users/kogilvie/Documents/github/kaogilvie.github.io/totals_by_state.csv',
    '/Users/kogilvie/Documents/github/kaogilvie.github.io/daily_by_state.csv',
    '/Users/kogilvie/Documents/github/kaogilvie.github.io/daily_countrywide.csv',
    '/Users/kogilvie/Documents/github/kaogilvie.github.io/totals_countrywide.csv'
]

file_exclusions = [
    '.DS_Store'
]

def run_update(env):
    logs.configure_logging('NYTUpdater')
    logger = logging.getLogger()

    logger.info(f"Refreshing all NYTimes data for env {env}.")

    nyt = times.NTYDataLoader(env)
    nyt.pull_new_github_data()
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

    logger.info("Adding files to index.")
    github_io_repo = Repo(local_config.data_repo_path)
    for file in file_path_list:
        github_io_repo.index.add(file)

    logger.info("Committing files.")
    github_io_repo.index.commit(f"Data update {datetime.now()}")

    logger.info("Pushing updates to remote.")
    github_io_origin = github_io_repo.remotes.origin
    github_io_origin.push()

if __name__ == "__main__":
    run_update()

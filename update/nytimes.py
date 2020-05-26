from load_data import nytimes_data as times
from mvs import mvs_maker as mv
from covid_utils import logs

import logging

# NYTIMES CONFIG
dynamic_tables = {
    'total_cases_by_county': 'us-counties.csv',
    'total_cases_by_state': 'us-states.csv'
}

mvs_files = [
    'nyt_county_geo.sql'
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

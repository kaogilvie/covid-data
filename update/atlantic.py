from load_data import atlantic_data as atlantic
from covid_utils import logs

import logging

# NYTIMES CONFIG
dynamic_tables = {
    'daily_state': 'daily_state.csv'
}

LOCAL = True

def run_update(env):
    logs.configure_logging('ATLTUpdater')
    logger = logging.getLogger()

    logger.info(f"Refreshing all Atlantic data for env {env}.")

    atl = atlantic.ATLDataLoader(env=env)
    for table, filename in dynamic_tables.items():
        logger.info(f"Working on {table}")
        atl.download_daily_data(filename)
        table_exists = atl.check_table_exists(table)
        atl.load_data(table, filename, exists=table_exists)

    # refresh MVs

    # refresh flatfiles

    # push flatfiles to git

if __name__ == "__main__":
    run_update()

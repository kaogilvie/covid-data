if __name__ == "__main__":
    import csv
    import os
    import logging

    import psycopg2
    from psycopg2.extras import DictCursor
    import pandas as pd

    from covid_utils import logs
    from covid_utils import connect
    from covid_utils import credentials

    ## set up logging
    logs.configure_logging('covid')
    logger = logging.getLogger()

    ## connect to postgres
    logger.info("Connecting to postgres...")
    pg_creds = credentials.get_postgres_creds()
    cxn = connect.dbconn(pg_creds)
    cursor = cxn.cursor(cursor_factory=DictCursor)
    logger.info("Connected to postgres.")

    ## data setup
    nytimes_data_root = '~/Documents/github/nytimes-covid-data'
    county_path = f'{nytimes_data_root}/us-counties.csv'
    state_path = f'{nytimes_data_root}/us-states.csv'

    logger.info("Appending new county data. First getting most recent data...")
    cursor.execute("SELECT max(date) FROM nytimes.cases_by_county;")
    fetched_data = cursor.fetchall()

    if fetched_data[0][0] is None:
        logger.info("Accessing full reload data..")
        county_data = open(os.path.expanduser(county_path), 'r')
        county_header = next(county_data).strip().split(',')

        logger.info("Initializing full load of table...")
        cursor.copy_from(county_data, 'nytimes.cases_by_county', sep=',', null="", columns=county_header)
        cxn.commit()

        logger.info("Loaded table fully...")
        cursor.execute("SELECT count(*) FROM nytimes.cases_by_county;")
        cnt = cursor.fetchall()
        logger.info(f'...meaning {cnt[0][0]} rows.')
    else:
        logger.info(f"Initializing incremental load of new rows beginning with the day after {fetched_data[0][0]}.")

        logger.info("Slicing data...")
        county_df = pd.read_csv(county_path)
        insert_df = county_df[county_df['date'] > str(fetched_data[0][0])]
        logger.info(f"Going to append a df of {len(insert_df)} rows using pandas.")

        logger.info("Connecting to Postgres via SQLAlchemy for pandas.")
        pd_cxn = connect.pandas_dbconn(pg_creds)

        logger.info("Appending rows...")
        insert_df.to_sql('cases_by_county', pd_cxn, schema='nytimes', if_exists='append', index=False, method='multi')

        logger.info("Done appending.")

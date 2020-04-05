# if __name__ == "__main__":
import csv
import os
import psycopg2
from covid_utils import logs


nytimes_data_root = '~/Documents/github/nytimes-covid-data'
county_path = f'{nytimes_data_root}/us-counties.csv'
state_path = f'{nytimes_data_root}/us-states.csv'

county_data = csv.reader(open(os.path.expanduser(county_path), 'r'))
county_header = next(county_data)

## connect to postgres

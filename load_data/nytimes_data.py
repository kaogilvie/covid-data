import argparse

from psycopg2.extensions import register_adapter, AsIs
import numpy

from load_data import load_utils

def addapt_numpy_int64(numpy_int64):
    return(AsIs(numpy_int64))

class NTYDataLoader(load_utils.DataLoader):
    def __init__(self, local=True):
        super().__init__(schema='nytimes')


if __name__ == "__main__":

    # deal with numpy interpolation
    register_adapter(numpy.int64, addapt_numpy_int64)

    args = argparse.ArgumentParser()
    args.add_argument('-t', '--table', help='table to refresh')
    args.add_argument('-f', '--filename', help='path to filename')
    argz = args.parse_args()

    table = argz.table
    filename = args.filename
    local = False
    if table is None:
        local = True
        table = f'cases_by_county'
        filename = f'us-counties.csv'

    nyt = NTYDataLoader(False)
    nyt.pull_new_github_data()
    table_exists = nyt.check_table_exists(table)
    nyt.load_data(table, filename, exists=table_exists)

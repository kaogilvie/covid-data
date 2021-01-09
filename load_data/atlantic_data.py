from load_data import load_utils

import requests
import arrow
import csv
from psycopg2.extensions import register_adapter, AsIs
import numpy
import json

from collections import OrderedDict

def addapt_numpy_int64(numpy_int64):
    return(AsIs(numpy_int64))

class ATLDataLoader(load_utils.DataLoader):
    def __init__(self, env='local'):
        super().__init__(schema='atlantic')

    def download_daily_data(self, filename):
        api_payload = requests.get('https://covidtracking.com/api/v1/states/daily.json')
        header = ['date', 'state', 'fips', 'lastupdateet', 'dataqualitygrade', 'positive',
                    'negative', 'pending', 'recovered', 'death', 'hospitalizedcurrently', 'hospitalizedcumulative',
                    'inicucurrently', 'inicucumulative', 'onventilatorcurrently', 'onventilatorcumulative',
                    'negativetestsviral', 'positivetestsviral', 'positivecasesviral', 'totaltestsviral',
                    'totaltestresults', 'positiveincrease', 'deathincrease', 'hospitalizedincrease', 'totaltestresultsincrease']

        with open(f'{self.file_root}/{filename}', 'w+') as header_file:
            header_dump = csv.writer(header_file)
            header_dump.writerow(header)
        payload_dump = csv.writer(open(f'{self.file_root}/{filename}', 'a'))

        for write_dict in json.loads(api_payload.content):
                for key, value in write_dict.items():
                    if key.lower() not in ('state', 'datequalitygrade'):
                        if value == '':
                            field = 0
                payload_dump.writerow([
                    arrow.get(str(write_dict['date']), 'YYYYMMDD').format('YYYY-MM-DD'),
                    write_dict['state'],
            		write_dict['fips'],
            		write_dict['lastUpdateEt'],
            		write_dict['dataQualityGrade'],
            		write_dict['positive'],
            		write_dict['negative'],
            		write_dict['pending'],
            		write_dict['recovered'],
            		write_dict['death'],
            		write_dict['hospitalizedCurrently'],
            		write_dict['hospitalizedCumulative'],
            		write_dict['inIcuCurrently'],
            		write_dict['inIcuCumulative'],
            		write_dict['onVentilatorCurrently'],
            		write_dict['onVentilatorCumulative'],
            		write_dict['negativeTestsViral'],
            		write_dict['positiveTestsViral'],
            		write_dict['positiveCasesViral'],
            		write_dict['totalTestsViral'],
            		write_dict['totalTestResults'],
            		write_dict['positiveIncrease'],
            		write_dict['deathIncrease'],
            		write_dict['hospitalizedIncrease'],
            		write_dict['totalTestResultsIncrease']
                ])


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
        table = f'daily_state'
        filename = f'daily_state.csv'

    atl= ATLDataLoader()
    atl.download_daily_data(filename)
    exists = atl.check_table_exists(table=table)
    atl.load_data(table, filename, exists=exists)

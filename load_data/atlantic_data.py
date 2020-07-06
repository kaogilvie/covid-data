from load_data import load_utils, download_from_api

import requests
import csv

def adapt_numpy_int64(numpy_int64):
    return(AsIs(numpy_int64))

class ATLAPIInterface(download_from_api.APIInterface):
    def __init__(self, local=True):
        super().__init__(schema='atlantic')

    def download_daily_csv(self):
        api_payload = requests.get('https://covidtracking.com//api/v1/states/daily.csv')
        payload_dump = csv.writer(open('/Users/kogilvie/Documents/github/local-covid-data/load_data/daily_state.csv', 'w+'))

        decoded_content = api_payload.content.decode('utf-8')
        read_csv = csv.reader(decoded_content.splitlines(), delimiter=',')

        for line in read_csv:
            payload_dump.writerow(line)


class ATLDataLoader(load_utils.DataLoader):
    def __init__(self, local=True):
        super().__init__(schema='atlantic')

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
        table = f'daily_states'
        filename = f'daily_states.csv'

    atl_api = ATLAPIInterface()

    # atl_api.download_state_data()

    api_payload = requests.get('https://covidtracking.com//api/v1/states/daily.csv')

    payload_dump = csv.writer(open('/Users/kogilvie/Documents/github/local-covid-data/load_data/daily_state.csv', 'w+'))

    decoded_content = api_payload.content.decode('utf-8')
    read_csv = csv.reader(decoded_content.splitlines(), delimiter=',')

    for line in read_csv:
        payload_dump.writerow(line)

    # nyt = ATLDataLoader(False)

import logging

import requests

from covid_utils import logs

class APIInterface(object):
    def __init__(self, schema='atlantic'):
        logs.configure_logging(f'{schema.capitalize()}DataLoader')
        self.logger = logging.getLogger()

        self.schema = schema

atl = APIInterface()

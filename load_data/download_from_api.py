import logging
import os

import requests

from covid_utils import logs
from covid_utils import local_config

class APIInterface(object):
    def __init__(self, schema='atlantic'):
        logs.configure_logging(f'{schema.capitalize()}DataLoader')
        self.logger = logging.getLogger()

        self.schema = schema

        self.github_path = local_config.github_paths[schema]
        self.file_root = os.path.expanduser(self.github_path)

atl = APIInterface()

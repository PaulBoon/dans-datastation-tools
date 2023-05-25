import json

import requests

from datastation.common.utils import print_dry_run_message


class DataverseApi:

    def __init__(self, alias, server_url, api_token, unblock_key, safety_latch):
        self.alias = alias
        self.server_url = server_url
        self.api_token = api_token
        self.unblock_key = unblock_key
        self.safety_latch = safety_latch

    def get_storage_size(self, dry_run: bool = False):
        """ Get dataverse storage size (bytes). """
        url = f'{self.server_url}/api/dataverses/{self.alias}/storagesize'
        headers = {'X-Dataverse-key': self.api_token}
        if dry_run:
            print_dry_run_message(method='GET', url=url, headers=headers)
            return None
        else:
            r = requests.get(url, headers=headers)
        r.raise_for_status()
        return r.json()['data']['message']

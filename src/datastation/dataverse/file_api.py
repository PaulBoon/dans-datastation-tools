import requests

from datastation.common.utils import print_dry_run_message, raise_for_status_after_log


class FileApi:

    def __init__(self, id, server_url, api_token, unblock_key, safety_latch):
        self.id = id
        self.server_url = server_url
        self.api_token = api_token
        self.unblock_key = unblock_key
        self.safety_latch = safety_latch

    def reingest(self, dry_run=False):
        url = f'{self.server_url}/api/files/{self.id}/reingest'
        params = {'unblock-key': self.unblock_key}
        headers = {'X-Dataverse-key': self.api_token}
        if dry_run:
            print_dry_run_message(method='POST', url=url, headers=headers, params=params)
            return None
        r = requests.post(url, headers=headers, params=params)
        raise_for_status_after_log(r)
        return r

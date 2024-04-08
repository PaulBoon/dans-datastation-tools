import requests

from datastation.common.utils import raise_for_status_after_log


class MetricsApi:
    def __init__(self, server_url: str):
        self.server_url = server_url

    def get_tree(self, dry_run: bool = False):
        """ Get dataverses hierarchy (tree). """
        url = f'{self.server_url}/api/info/metrics/tree'
        if dry_run:
            print(f"Would have sent the following request: {url}")
            return
        r = requests.get(url)
        raise_for_status_after_log(r)
        return r.json()['data']

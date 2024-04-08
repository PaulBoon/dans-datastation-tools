import json

import requests

from datastation.common.utils import raise_for_status_after_log


class BannerApi:

    def __init__(self, server_url: str, api_token: str, unblock_key: str):
        self.server_url = server_url
        self.api_token = api_token
        self.unblock_key = unblock_key

    def list(self, dry_run: bool = False):
        """ List all banners. """
        url = f'{self.server_url}/api/admin/bannerMessage'
        headers = {'X-Dataverse-key': self.api_token}
        if dry_run:
            print(f"Would have sent the following request: {url}")
            return
        r = requests.get(url, headers=headers, params={'unblock-key': self.unblock_key})
        raise_for_status_after_log(r)
        return r

    def add(self, msg: str, dismissible_by_user: bool = False, lang: str = 'en', dry_run: bool = False):
        """ Add a banner. """
        banner = {
            "messageTexts": [
                {
                    "lang": lang,
                    "message": msg
                }
            ],
            "dismissibleByUser": str(dismissible_by_user).lower()
        }
        url = f'{self.server_url}/api/admin/bannerMessage'
        headers = {'X-Dataverse-key': self.api_token, 'Content-type': 'application/json'}
        if dry_run:
            print(f"Would have sent the following request: {url}")
            print(json.dumps(banner, indent=4))
            return
        r = requests.post(url, headers=headers, params={'unblock-key': self.unblock_key}, json=banner)
        raise_for_status_after_log(r)
        return r

    def remove(self, banner_id: int, dry_run: bool = False):
        """ Remove a banner. """
        url = f'{self.server_url}/api/admin/bannerMessage/{banner_id}'
        headers = {'X-Dataverse-key': self.api_token}
        if dry_run:
            print(f"Would have sent the following request: {url}")
            return
        r = requests.delete(url, headers=headers, params={'unblock-key': self.unblock_key})
        raise_for_status_after_log(r)
        return r

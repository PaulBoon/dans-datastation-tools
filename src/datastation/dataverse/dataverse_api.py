import requests


from datastation.common.utils import print_dry_run_message


class DataverseApi:
    def __init__(self, server_url, api_token):
        self.server_url = server_url
        self.api_token = api_token

    def search(self, subtree="root", query="*", dry_run=False):
        """
        Do a query via the public search API, only published datasets
        using the public search 'API', so no token needed

        Note that the current functionality of this function is very limited!

        :param subtree: This is the collection (dataverse alias)
                        it recurses into a collection and its children etc. very useful with nesting collection
        :param start: The cursor (zero based result index) indicating where the result page starts
        :param rows: The number of results returned in the 'page'
        :return: The 'paged' search results in a list of dictionaries
        """

        per_page = 25
        current_page = 0

        # always type=dataset, those have pids (disregarding pids for files)
        params = {
            "q": query,
            "subtree": subtree,
            "type": "dataset",
            "per_page": str(per_page),
            "start": str(current_page),
        }

        headers = {"X-Dataverse-key": self.api_token}
        url = f"{self.server_url}/api/search"

        if dry_run:
            print_dry_run_message(method="GET", url=url, headers=headers, params=params)
            return None

        while True:
            dv_resp = requests.get(url, headers=headers, params=params)
            dv_resp.raise_for_status()

            data = dv_resp.json()["data"]
            items = data["items"]

            if len(items) == 0:
                break

            for item in items:
                yield item

            current_page += per_page
            params["start"] = str(current_page)

    def get_contents(self, alias="root", dry_run=False):
        headers = {"X-Dataverse-key": self.api_token}
        url = f"{self.server_url}/api/dataverses/{alias}/contents"

        if dry_run:
            print_dry_run_message(method="GET", url=url, headers=headers)
            return None

        dv_resp = requests.get(url, headers=headers)
        dv_resp.raise_for_status()

        resp_data = dv_resp.json()["data"]
        return resp_data

    def get_storage_size(self, alias="root", dry_run=False):
        """ Get dataverse storage size (bytes). """
        url = f'{self.server_url}/api/dataverses/{alias}/storagesize'
        headers = {'X-Dataverse-key': self.api_token}
        if dry_run:
            print_dry_run_message(method='GET', url=url, headers=headers)
            return None
        else:
            r = requests.get(url, headers=headers)
        r.raise_for_status()
        return r.json()['data']['message']

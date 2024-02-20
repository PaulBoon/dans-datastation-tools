import logging

import requests

from datastation.common.utils import print_dry_run_message, raise_for_status_after_log


class SearchApi:

    def __init__(self, server_url, api_token):
        self.url = f"{server_url}/api/search"
        self.api_token = api_token

    def search(self, query="*", subtree="root", object_type="dataset", dry_run=False, rows=0, start=0):
        """
        Do a query via the public search API, only published datasets
        using the public search 'API', so no token needed

        Note that the current functionality of this function is very limited!

        :param query:
        :param subtree: This is the collection (dataverse alias)
                        it recurses into a collection and its children etc. very useful with nesting collection
        :param object_type:
        :param dry_run: Do not perform the action, but show what would be done.
        :param start: The cursor (zero based result index) indicating where the result page starts
        :param rows: The number of results returned in the 'page'
                     if zero: 25 rows at a time are read until no more rows are found
        :return: The search results in a list of dictionaries.
                 Make sure the result is not transformed to an array before feeding it to a batch processor,
                 otherwise all pages are read before processing starts. In other words:
                 don't: [record['global_id'] for record in dataverse_client.search_api().search()]
                 but: map(lambda rec: rec['global_id'], dataverse_client.search_api().search())
        """

        if rows == 0:
            per_page = 25
        else:
            per_page = rows

        params = {
            "q": query,
            "subtree": subtree,
            "type": object_type,
            "per_page": str(per_page),
            "start": str(start),
        }

        headers = {"X-Dataverse-key": self.api_token}

        if dry_run:
            print_dry_run_message(method="GET", url=self.url, headers=headers, params=params)
            return None

        while True:
            dv_resp = requests.get(self.url, headers=headers, params=params)
            raise_for_status_after_log(dv_resp)

            data = dv_resp.json()["data"]
            items = data["items"]
            logging.debug(f"{len(items)} items, {params}")

            for item in items:
                logging.debug(f"ITEM: {item}")
                yield item

            if len(items) < per_page or rows != 0:
                break

            start += per_page
            params["start"] = str(start)

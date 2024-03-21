from datastation.common.database import Database
from datastation.dataverse.banner_api import BannerApi
from datastation.dataverse.builtin_users import BuiltInUsersApi
from datastation.dataverse.dataset_api import DatasetApi
from datastation.dataverse.dataverse_api import DataverseApi
from datastation.dataverse.file_api import FileApi
from datastation.dataverse.metrics_api import MetricsApi
from datastation.dataverse.search_api import SearchApi


class DataverseClient:
    """ A client for the Dataverse API. """

    def __init__(self, config: dict):
        self.server_url = config['server_url']
        self.api_token = config['api_token']
        self.unblock_key = config['unblock_key'] if 'unblock_key' in config else None
        self.safety_latch = config['safety_latch']
        self.db_config = config['db']

    def banner(self):
        return BannerApi(self.server_url, self.api_token, self.unblock_key)

    def search_api(self):
        return SearchApi(self.server_url, self.api_token)

    def dataset(self, pid):
        return DatasetApi(pid, self.server_url, self.api_token, self.unblock_key, self.safety_latch)

    def dataverse(self, alias=None):
        return DataverseApi(self.server_url, self.api_token, alias)

    def file(self, file_id):
        return FileApi(file_id, self.server_url, self.api_token, self.unblock_key, self.safety_latch)

    def built_in_users(self, builtin_users_key):
        return BuiltInUsersApi(self.server_url, self.api_token, builtin_users_key, self.unblock_key)

    def database(self):
        return Database(self.db_config)

    def metrics(self):
        return MetricsApi(self.server_url)

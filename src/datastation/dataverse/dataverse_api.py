import requests
import json


from datastation.common.utils import print_dry_run_message, raise_for_status_after_log


class DataverseApi:
    def __init__(self, server_url, api_token, alias):
        self.server_url = server_url
        self.api_token = api_token
        self.alias = alias  # Methods should use this one if specified

    def get_alias(self):
        return self.alias

    # get json data for a specific dataverses API endpoint using an API token
    def get_resource_data(self, resource, dry_run=False):
        headers = {"X-Dataverse-key": self.api_token}
        url = f"{self.server_url}/api/dataverses/{self.alias}/{resource}"

        if dry_run:
            print_dry_run_message(method="GET", url=url, headers=headers)
            return None

        dv_resp = requests.get(url, headers=headers)
        raise_for_status_after_log(dv_resp)

        resp_data = dv_resp.json()["data"]
        return resp_data

    def get_contents(self, dry_run=False):
        return self.get_resource_data("contents", dry_run)

    def get_roles(self, dry_run=False):
        return self.get_resource_data("roles", dry_run)

    def get_role_assignments(self, dry_run=False):
        return self.get_resource_data("assignments", dry_run)

    def get_groups(self, dry_run=False):
        return self.get_resource_data("groups", dry_run)

    def get_storage_size(self, dry_run=False):
        """ Get dataverse storage size (bytes). """
        url = f'{self.server_url}/api/dataverses/{self.alias}/storagesize'
        headers = {'X-Dataverse-key': self.api_token}
        if dry_run:
            print_dry_run_message(method='GET', url=url, headers=headers)
            return None
        else:
            r = requests.get(url, headers=headers)
        raise_for_status_after_log(r)
        return r.json()['data']['message']

    def add_role_assignment(self, assignee, role, dry_run=False):
        url = f'{self.server_url}/api/dataverses/{self.alias}/assignments'
        headers = {'X-Dataverse-key': self.api_token, 'Content-type': 'application/json'}
        role_assignment = {"assignee": assignee, "role": role}
        if dry_run:
            print_dry_run_message(method='POST', url=url, headers=headers,
                                  data=json.dumps(role_assignment))
            return None
        else:
            r = requests.post(url, headers=headers, json=role_assignment)
            raise_for_status_after_log(r)
            return r

    def remove_role_assignment(self, assignment_id, dry_run=False):
        url = f'{self.server_url}/api/dataverses/{self.alias}/assignments/{assignment_id}'
        headers = {'X-Dataverse-key': self.api_token, 'Content-type': 'application/json'}
        if dry_run:
            print_dry_run_message(method='DELETE', url=url, headers=headers)
            return None
        else:
            r = requests.delete(url, headers=headers)
        raise_for_status_after_log(r)
        return r

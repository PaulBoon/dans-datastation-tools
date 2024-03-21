import json
import time

import requests

from datastation.common.utils import print_dry_run_message, raise_for_status_after_log


class DatasetApi:

    def __init__(self, pid, server_url, api_token, unblock_key, safety_latch):
        self.pid = pid
        self.server_url = server_url
        self.api_token = api_token
        self.unblock_key = unblock_key
        self.safety_latch = safety_latch

    def get_pid(self):
        return self.pid
    
    def get(self, version=":latest", dry_run=False):
        url = f'{self.server_url}/api/datasets/:persistentId/versions/{version}'
        headers = {'X-Dataverse-key': self.api_token}
        params = {'persistentId': self.pid}
        
        if dry_run:
            print_dry_run_message(method='GET', url=url, headers=headers, params=params)
            return None
        
        dv_resp = requests.get(url, headers=headers, params=params)
        raise_for_status_after_log(dv_resp)

        resp_data = dv_resp.json()['data']
        return resp_data

    def get_role_assignments(self, dry_run=False):
        url = f'{self.server_url}/api/datasets/:persistentId/assignments'
        params = {'persistentId': self.pid}
        headers = {'X-Dataverse-key': self.api_token}
        if dry_run:
            print_dry_run_message(method='GET', url=url, headers=headers, params=params)
            return None
        else:
            r = requests.get(url, headers=headers, params=params)
        raise_for_status_after_log(r)
        return r.json()['data']

    def add_role_assignment(self, assignee, role, dry_run=False):
        url = f'{self.server_url}/api/datasets/:persistentId/assignments'
        params = {'persistentId': self.pid}
        headers = {'X-Dataverse-key': self.api_token, 'Content-type': 'application/json'}
        role_assignment = {"assignee": assignee, "role": role}
        if dry_run:
            print_dry_run_message(method='POST', url=url, headers=headers, params=params,
                                  data=json.dumps(role_assignment))
            return None
        else:
            r = requests.post(url, headers=headers, params=params, json=role_assignment)
            raise_for_status_after_log(r)
            return r

    def remove_role_assignment(self, assignment_id, dry_run=False):
        url = f'{self.server_url}/api/datasets/:persistentId/assignments/{assignment_id}'
        params = {'persistentId': self.pid}
        headers = {'X-Dataverse-key': self.api_token, 'Content-type': 'application/json'}
        if dry_run:
            print_dry_run_message(method='DELETE', url=url, headers=headers, params=params)
            return None
        else:
            r = requests.delete(url, headers=headers, params=params)
        raise_for_status_after_log(r)
        return r

    def is_draft(self, dry_run=False):
        url = f'{self.server_url}/api/datasets/:persistentId'
        params = {'persistentId': self.pid}
        headers = {'X-Dataverse-key': self.api_token}
        if dry_run:
            print_dry_run_message(method='GET', url=url, headers=headers, params=params)
            return None
        else:
            r = requests.get(url, headers=headers, params=params)
        raise_for_status_after_log(r)
        return r.json()['data']['latestVersion']['versionState'] == 'DRAFT'

    def delete_draft(self, dry_run=False):
        url = f'{self.server_url}/api/datasets/:persistentId'
        params = {'persistentId': self.pid}
        headers = {'X-Dataverse-key': self.api_token}
        if dry_run:
            print_dry_run_message(method='DELETE', url=url, headers=headers, params=params)
            return None
        else:
            r = requests.delete(url, headers=headers, params=params)
        raise_for_status_after_log(r)
        return r.json()

    def destroy(self, dry_run=False):
        url = f'{self.server_url}/api/datasets/:persistentId/destroy'
        headers = {'X-Dataverse-key': self.api_token}
        params = {'persistentId': self.pid, 'unblock-key': self.unblock_key}

        if self.safety_latch:
            print("Safety latch is on, not sending command...")
            return None
        else:
            if dry_run:
                print_dry_run_message(method='DELETE', url=url, headers=headers, params=params)
                return None
            r = requests.delete(url, headers=headers, params=params)
        raise_for_status_after_log(r)
        return r.json()

    def get_metadata(self, version=':latest', dry_run=False):
        """Get the native JSON metadata for a dataset version. Version can be a number of one of ':latest', ':draft' or
         ':latest-published'. See
         https://guides.dataverse.org/en/latest/api/native-api.html#get-json-representation-of-a-dataset
         for more information.
         """
        url = f'{self.server_url}/api/datasets/:persistentId/versions/{version}'
        params = {'persistentId': self.pid}
        headers = {'X-Dataverse-key': self.api_token}
        if dry_run:
            print_dry_run_message(method='GET', url=url, headers=headers, params=params)
            return None
        else:
            r = requests.get(url, headers=headers, params=params)
        raise_for_status_after_log(r)
        return r.json()['data']

    def get_metadata_export(self, exporter='dataverse_json', dry_run=False):
        """Get a metadata export for the latest version of the dataset.
        See https://guides.dataverse.org/en/latest/api/native-api.html#export-metadata-of-a-dataset-in-various-formats
         for more information."""
        # N.B. no :persistentId in the URL
        url = f'{self.server_url}/api/datasets/export?exporter={exporter}'
        params = {'persistentId': self.pid}
        headers = {'X-Dataverse-key': self.api_token}
        if dry_run:
            print_dry_run_message(method='GET', url=url, headers=headers, params=params)
            return None
        else:
            r = requests.get(url, headers=headers, params=params)
        raise_for_status_after_log(r)
        return r.text

    def get_locks(self, lock_type=None, dry_run=False):
        url = f'{self.server_url}/api/datasets/:persistentId/locks'
        params = {'persistentId': self.pid}
        if lock_type:
            params['type'] = lock_type
        headers = {'X-Dataverse-key': self.api_token}
        if dry_run:
            print_dry_run_message(method='GET', url=url, headers=headers, params=params)
            return None
        else:
            r = requests.get(url, headers=headers, params=params)
        raise_for_status_after_log(r)
        return r.json()['data']

    def add_lock(self, lock_type, dry_run=False):
        url = f'{self.server_url}/api/datasets/:persistentId/lock/{lock_type}'
        params = {'persistentId': self.pid}
        headers = {'X-Dataverse-key': self.api_token, 'Content-type': 'application/json'}
        if dry_run:
            print_dry_run_message(method='POST', url=url, headers=headers, params=params)
            return None
        else:
            r = requests.post(url, headers=headers, params=params)
            raise_for_status_after_log(r)
            return r.json()

    def remove_lock(self, lock_type=None, dry_run=False):
        url = f'{self.server_url}/api/datasets/:persistentId/locks'
        params = {'persistentId': self.pid}
        if lock_type:
            params['type'] = lock_type
        headers = {'X-Dataverse-key': self.api_token}
        if dry_run:
            print_dry_run_message(method='DELETE', url=url, headers=headers, params=params)
            return None
        else:
            r = requests.delete(url, headers=headers, params=params)
        raise_for_status_after_log(r)
        return r.json()

    def remove_all_locks(self, dry_run=False):
        return self.remove_lock(dry_run=dry_run)

    def publish(self, update_type='major', dry_run=False):
        url = f'{self.server_url}/api/datasets/:persistentId/actions/:publish'
        params = {'persistentId': self.pid, 'type': update_type}
        headers = {'X-Dataverse-key': self.api_token}
        if dry_run:
            print_dry_run_message(method='POST', url=url, headers=headers, params=params)
            return None
        r = requests.post(url, headers=headers, params=params)
        raise_for_status_after_log(r)
        return r.json()

    def reindex(self, dry_run=False):
        url = f'{self.server_url}/api/admin/index/dataset'
        params = {'persistentId': self.pid}
        headers = {'X-Dataverse-key': self.api_token}
        if self.unblock_key:
            params['unblock-key'] = self.unblock_key
        if dry_run:
            print_dry_run_message(method='GET', url=url, headers=headers, params=params)
            return None
        else:
            r = requests.get(url, headers=headers, params=params)
        raise_for_status_after_log(r)
        return r.json()

    def modify_registration_metadata(self, dry_run=False):
        url = f'{self.server_url}/api/datasets/:persistentId/modifyRegistrationMetadata'
        params = {'persistentId': self.pid}
        headers = {'X-Dataverse-key': self.api_token}
        if dry_run:
            print_dry_run_message(method='POST', url=url, headers=headers, params=params)
            return None
        else:
            r = requests.post(url, headers=headers, params=params)
        raise_for_status_after_log(r)
        return r.json()

    def get_files(self, version=':latest', dry_run=False):
        url = f'{self.server_url}/api/datasets/:persistentId/versions/{version}/files'
        params = {'persistentId': self.pid}
        headers = {'X-Dataverse-key': self.api_token}
        if dry_run:
            print_dry_run_message(method='GET', url=url, headers=headers, params=params)
            return None
        else:
            r = requests.get(url, headers=headers, params=params)
        raise_for_status_after_log(r)
        return r.json()['data']

    def await_unlock(self, lock_type=None, sleep_time=5, max_tries=10):
        """Wait for a lock to be removed from the dataset."""
        tries = 0
        while tries < max_tries:
            locks = self.get_locks(lock_type=lock_type)
            if len(locks) == 0:
                return
            tries += 1
            time.sleep(sleep_time)
        if lock_type is None:
            message = f'Locks not removed after {max_tries} tries.'
        else:
            message = f'Locks {lock_type} not removed after {max_tries} tries.'
        raise RuntimeError(message)

    def edit_metadata(self, data: dict, dry_run=False, replace: bool = False):
        url = f'{self.server_url}/api/datasets/:persistentId/editMetadata'
        params = {'persistentId': self.pid}
        if replace:
            params['replace'] = 'true'
        headers = {'X-Dataverse-key': self.api_token}
        if dry_run:
            print_dry_run_message(method='PUT', url=url, headers=headers, params=params, data=data)
            return None
        else:
            r = requests.put(url, headers=headers, params=params, data=data)
            raise_for_status_after_log(r)
            return r

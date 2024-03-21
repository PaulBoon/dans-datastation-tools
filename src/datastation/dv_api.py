import json

import requests

from lxml import etree

from datastation.common.utils import raise_for_status_after_log


# Thin 'client' functions for http API on the dataverse service, not using a API lib, just the requests lib
# could be placed in a class that also keeps hold of the url and token that we initialise once!
#
# Also note that here we use the PID (persistentId) instead of the internal ID form of the requests.


def search(server_url, subtree, start=0, rows=10):
    '''
    Do a query via the public search API, only published datasets
    using the public search 'API', so no token needed

    Note that the current functionality of this function is very limited!

    :param subtree: This is the collection (dataverse alias)
                    it recurses into a collection and its children etc. very useful with nesting collection
    :param start: The cursor (zero based result index) indicating where the result page starts
    :param rows: The number of results returned in the 'page'
    :return: The 'paged' search results in a list of dictionaries
    '''

    # always type=dataset, those have pids (disregarding pids for files)
    params = {
        'q': '*',
        'subtree': subtree,
        'type': 'dataset',
        'per_page': str(rows),
        'start': str(start)
    }

    # params['fq'] = ''

    dv_resp = requests.get(server_url + '/api/search', params=params)

    # give some feedback
    # print("Status code: {}".format(dv_resp.status_code))
    # print("Json: {}".format(dv_resp.json()))
    # the json result is a dictionary... so we could check for something in it
    raise_for_status_after_log(dv_resp)
    resp_data = dv_resp.json()['data']
    # print(json.dumps(resp_data, indent=2))
    return resp_data


def get_dataset_metadata_export(server_url, pid, exporter = 'dataverse_json', response_is_json = True):
    params = {'exporter': exporter, 'persistentId': pid}
    dv_resp = requests.get(server_url + '/api/datasets/export',
                           params=params)

    # give some feedback
    # print("Status code: {}".format(dv_resp.status_code))
    # print("Json: {}".format(dv_resp.json()))
    # the json result is a dictionary... so we could check for something in it
    raise_for_status_after_log(dv_resp)
    # assume json, but not all exporters have that!
    if response_is_json:
        resp_data = dv_resp.json()  # Note that the response json has no wrapper around the data
    else:
        resp_data = dv_resp.text
    return resp_data


# with a token, can also get metadata from drafts
def get_dataset_metadata(server_url, api_token, pid):
    headers = {'X-Dataverse-key': api_token}
    dv_resp = requests.get(server_url + '/api/datasets/:persistentId/versions/:latest?persistentId=' + pid,
                           headers=headers)
    # Maybe give some more feedback
    # print("Status code: {}".format(dv_resp.status_code))
    # print("Json: {}".format(dv_resp.json()))
    # the json result is a dictionary... so we could check for something in it
    raise_for_status_after_log(dv_resp)
    resp_data = dv_resp.json()['data']
    return resp_data


# note that the dataset will become a draft if it was not already
def replace_dataset_metadatafield(server_url, api_token, pid, field):
    headers = {'X-Dataverse-key': api_token}
    try:
        dv_resp = requests.put(
            server_url + '/api/datasets/:persistentId/editMetadata?persistentId=' + pid + '&replace=true',
            data=json.dumps(field, ensure_ascii=False),
            headers=headers)
        raise_for_status_after_log(dv_resp)
    except requests.exceptions.RequestException as re:
        print("RequestException: ", re)
        raise
    resp_data = dv_resp.json()['data']
    return resp_data


def get_dataset_roleassigments(server_url, api_token, pid):
    headers = {'X-Dataverse-key': api_token}
    params = {'persistentId': pid}
    try:
        dv_resp = requests.get(server_url + '/api/datasets/:persistentId/assignments',
                               params=params,
                               headers=headers)
        raise_for_status_after_log(dv_resp)
    except requests.exceptions.RequestException as re:
        print("RequestException: ", re)
        raise
    resp_data = dv_resp.json()['data']
    return resp_data


def delete_dataset_role_assignment(server_url, api_token, pid, assignment_id):
    headers = {'X-Dataverse-key': api_token}
    dv_resp = requests.delete(server_url + '/api/datasets/:persistentId/assignments/' + str(assignment_id)
                              + '?persistentId=' + pid,
                              headers=headers)
    raise_for_status_after_log(dv_resp)


def add_dataset_role_assignment(server_url, api_token, pid, assignment):
    headers = {'X-Dataverse-key': api_token, 'Content-Type': 'application/json'}
    params = {'persistentId': pid}
    dv_resp = requests.post(server_url + '/api/datasets/:persistentId/assignments/',
                            headers=headers,
                            data=json.dumps(assignment, ensure_ascii=False),
                            params=params)
    raise_for_status_after_log(dv_resp)


def get_dataset_locks(server_url: str, pid: str):
    dv_resp = requests.get(server_url + '/api/datasets/:persistentId/locks?persistentId=' + pid)
    # give some feedback
    # print("Status code: {}".format(dv_resp.status_code))
    # print("Json: {}".format(dv_resp.json()))
    # the json result is a dictionary... so we could check for something in it
    raise_for_status_after_log(dv_resp)
    resp_data = dv_resp.json()['data']
    return resp_data

def get_dataset_files(server_url: str, pid: str, version=':latest'):
    dv_resp = requests.get(server_url + '/api/datasets/:persistentId/versions/' + version + '/files?persistentId=' + pid)
    # give some feedback
    # print("Status code: {}".format(dv_resp.status_code))
    # print("Json: {}".format(dv_resp.json()))
    # the json result is a dictionary... so we could check for something in it
    raise_for_status_after_log(dv_resp)
    resp_data = dv_resp.json()['data']
    return resp_data

def reingest_file(server_url: str, api_token: str, file_id: str):
    headers = {'X-Dataverse-key': api_token, 'Content-Type': 'application/json'}
    dv_resp = requests.post(server_url + '/api/files/' + file_id + '/reingest', headers=headers)
    raise_for_status_after_log(dv_resp)
    resp_data = dv_resp.json()['data']
    return resp_data


def create_dataset_lock(server_url, api_token, pid, lock_type):
    headers = {'X-Dataverse-key': api_token}
    dv_resp = requests.post("{}/api/datasets/:persistentId/lock/{}?persistentId={}"
                            .format(server_url, lock_type, pid), headers=headers)
    raise_for_status_after_log(dv_resp)


def delete_dataset_locks_all(server_url, api_token, pid):
    headers = {'X-Dataverse-key': api_token}
    dv_resp = requests.delete(server_url + '/api/datasets/:persistentId/locks?persistentId=' + pid,
                              headers=headers)
    raise_for_status_after_log(dv_resp)


def delete_dataset_lock(server_url, api_token, pid, lock_type):
    headers = {'X-Dataverse-key': api_token}
    dv_resp = requests.delete("{}/api/datasets/:persistentId/locks?persistentId={}&type={}"
                              .format(server_url, pid, lock_type), headers=headers)
    raise_for_status_after_log(dv_resp)


def publish_dataset(server_url, api_token, pid, version_upgrade_type="major"):
    # version_upgrade_type must be 'major' or 'minor', indicating which part of next version to increase
    headers = {'X-Dataverse-key': api_token}
    dv_resp = requests.post(server_url + '/api/datasets/:persistentId/actions/:publish?persistentId='
                            + pid + '&type=' + version_upgrade_type,
                            headers=headers)
    raise_for_status_after_log(dv_resp)


# This is via the admin api and does not use the token,
# but instead will need to be run on localhost or via an SSH tunnel for instance!
def reindex_dataset(server_url, pid):
    dv_resp = requests.get(server_url + '/api/admin/index/dataset?persistentId=' + pid)
    raise_for_status_after_log(dv_resp)
    resp_data = dv_resp.json()['data']
    return resp_data


# Warning: this deletes information that might be difficult to restore
# Use mostly while developing and testing with non-production data.
def delete_dataset_draft(server_url, api_token, pid):
    headers = {'X-Dataverse-key': api_token}
    dv_resp = requests.delete(server_url + '/api/datasets/:persistentId/versions/:draft?persistentId=' + pid,
                              headers=headers)
    raise_for_status_after_log(dv_resp)


# Warning: this deletes also a PUBLISHED dataset!
# Use mostly while developing and testing with non-production data.
# The reasons to destroy on production should be extremely rare!
def destroy_dataset(server_url, api_token, pid):
    headers = {'X-Dataverse-key': api_token}
    dv_resp = requests.delete(server_url + '/api/datasets/:persistentId/destroy/?persistentId=' + pid,
                              headers=headers)
    raise_for_status_after_log(dv_resp)


# Remember to get info on the OAI endpoint you can do:
# oai?verb=Identify
# oai?verb=ListSets
# oai?verb=ListMetadataFormats
# we could add function for that if we wanted
#
# Default there is no set specified and you get just all
# also no date range (with from, until)
def get_oai_records(server_url, format, set=None):
    params = {'verb': 'ListRecords', 'metadataPrefix': format}
    if set is not None:
        params['set'] = set
    dv_resp = requests.get(server_url + '/oai',
                           params=params)

    raise_for_status_after_log(dv_resp)
    # assume XML
    xml_doc = etree.fromstring(dv_resp.content)
    # alternatively we could use the parse directly and not requests.get
    # xml_doc = etree.parse(url).getroot()

    return xml_doc


def get_oai_records_resume(server_url, token):
    params = {'verb': 'ListRecords', 'resumptionToken': token}
    dv_resp = requests.get(server_url + '/oai',
                           params=params)

    raise_for_status_after_log(dv_resp)
    # assume XML
    xml_doc = etree.fromstring(dv_resp.content)
    return xml_doc


def change_access_request(server_url, api_token, pid, makeRestricted):
    headers = {'X-Dataverse-key': api_token}
    try:
        dv_resp = requests.put(
            server_url + '/api/access/:persistentId/allowAccessRequest?persistentId=' + pid ,
            data=json.dumps(makeRestricted),
            headers=headers)
        raise_for_status_after_log(dv_resp)
    except requests.exceptions.RequestException as re:
        print("RequestException: ", re)
        raise
    resp_data = dv_resp.json()['data']
    return resp_data
# curl -H "X-Dataverse-key:$API_TOKEN" -X PUT -d true http://$SERVER/api/access/:persistentId/allowAccessRequest?persistentId={pid}


def change_file_restrict(server_url, api_token, file_id, makeRestricted):
    headers = {'X-Dataverse-key': api_token}
    try:
        dv_resp = requests.put(
            server_url + '/api/files/{}/restrict'.format(file_id),
            data=json.dumps(makeRestricted),
            headers=headers)
        raise_for_status_after_log(dv_resp)
    except requests.exceptions.RequestException as re:
        print("RequestException: ", re)
        raise
    resp_data = dv_resp.json()['data']
    return resp_data


def replace_dataset_metadata(server_url, api_token, pid, json_data):
    headers = {'X-Dataverse-key': api_token}
    try:
        dv_resp = requests.put(
            server_url + '/api/datasets/:persistentId/metadata?persistentId=' + pid + '&replace=true',
            data=json_data,
            headers=headers)
        raise_for_status_after_log(dv_resp)
    except requests.exceptions.RequestException as re:
        print("RequestException: ", re)
        raise
    resp_data = dv_resp.json()['data']
    return resp_data

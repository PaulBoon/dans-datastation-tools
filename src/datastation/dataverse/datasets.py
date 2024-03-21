import json
import logging
import re

from datastation.dataverse.dataverse_client import DataverseClient


class Datasets:

    def __init__(self, dataverse_client: DataverseClient, dry_run: bool = False):
        self.dataverse_client = dataverse_client
        self.dry_run = dry_run

    def update_metadata(self, data: dict, replace: bool = False):
        logging.debug(data)

        type_names = [key for key in data.keys() if key != 'PID' and data[key] is not None]

        compound_multi_value_fields = {}
        simple_fields = {}  # both single and multi-value
        for type_name in type_names:
            match = re.match('([-a-z]+)((\\[([0-9]+)])|@)?([-a-z]+)?$', type_name, re.IGNORECASE)
            if match is None:
                raise Exception(f"Invalid typeName [{type_name}] : {data}")
            if '@' in type_name:
                raise Exception(f"Single-value compound fields [{type_name}] are not supported : {data}")
            if '[' not in type_name:
                simple_fields[type_name] = data[type_name]
                if not replace:
                    raise Exception(f"Single-value fields [{type_name}] must be replaced : {data}")
            else:
                parent = match.group(1)
                child = match.group(5)
                index = int(match.group(4))
                if child is None:
                    if parent not in simple_fields.keys():
                        simple_fields[parent] = []
                    while index >= len(simple_fields[parent]):
                        simple_fields[parent].append(None)
                    simple_fields[parent][index] = data[type_name]
                else:
                    if parent not in compound_multi_value_fields.keys():
                        compound_multi_value_fields[parent] = [{}]
                    while index >= len(compound_multi_value_fields[parent]):
                        compound_multi_value_fields[parent].append({})
                    compound_multi_value_fields[parent][index][child] = data[type_name]
        logging.debug(simple_fields)
        logging.debug(compound_multi_value_fields)

        all_fields = []
        for key in simple_fields:
            all_fields.append({'typeName': key, 'value': simple_fields[key]})
        for key in compound_multi_value_fields.keys():
            compound_multi_value = []
            for i in range(len(compound_multi_value_fields[key])):
                subfields = {}
                for sub_key in compound_multi_value_fields[key][i].keys():
                    sub_value = compound_multi_value_fields[key][i][sub_key]
                    if sub_value is not None:
                        subfields[sub_key] = ({'typeName': sub_key, 'value': sub_value})
                compound_multi_value.append(subfields)
            all_fields.append({'typeName': key, 'value': compound_multi_value})

        logging.debug(all_fields)
        dataset_api = self.dataverse_client.dataset(data['PID'])
        match = dataset_api.edit_metadata(data=(json.dumps({'fields': all_fields})), replace=replace, dry_run=self.dry_run)
        logging.info(match)
        return match

    def get_dataset_attributes(self, pid: str,  storage: bool = False, user_with_role: str = None):
        logging.debug(f"pid={pid}")
        attributes = {"pid": pid}

        dataset_api = self.dataverse_client.dataset(pid)
        if storage:
            dataset = dataset_api.get(dry_run=self.dry_run)
            attributes["storage"] = sum(
                f["dataFile"]["filesize"] for f in dataset["files"]
            )

        if user_with_role is not None:
            role_assignments = dataset_api.get_role_assignments(dry_run=self.dry_run)
            attributes["users"] = [
                user["assignee"].replace("@", "")
                for user in role_assignments
                if user["_roleAlias"] == user_with_role
            ]

        return attributes

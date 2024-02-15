from datastation.common.result_writer import CsvResultWriter, YamlResultWriter, JsonResultWriter
from datastation.dataverse.dataverse_client import DataverseClient
import logging
import re
import csv
import sys
import json
import rich
from datetime import timedelta


class PermissionsCollect:

    def __init__(self, dataverse_client: DataverseClient, output_file, output_format, dry_run: bool = False):
        self.dataverse_client = dataverse_client
        self.output_file = output_file
        self.output_format = output_format
        self.dry_run = dry_run

        self.writer = None
        self.is_first = True  # Would be nicer if the Writer does the bookkeeping
        self.vpath_delimiter = ' > '  # Note that'/' would tempt people us use it as a real path

    def create_result_writer(self, out_stream):
        logging.info(f'Writing output: {self.output_file}, with format : {self.output_format}')
        csv_columns = ['depth', 'parentalias', 'alias', 'name', 'id', 'vpath', 'groups', 'roles', 'assignments']
        if self.output_format == 'csv':
            return CsvResultWriter(headers=csv_columns, out_stream=out_stream)
        else:
            return JsonResultWriter(out_stream)

    def write_result_row(self, row):
        self.writer.write(row, self.is_first)
        self.is_first = False  # Only the first time it can be True

    def get_result_row(self, parent_alias, child_alias, child_name, id, vpath, depth):
        logging.info(f'Retrieving permission info for dataverse: {parent_alias} / {child_alias} ...')
        group_info = self.get_group_info(child_alias)
        role_info = self.get_role_info(child_alias)
        assignment_info = self.get_assignment_info(child_alias)
        row = {'depth': depth, 'parentalias': parent_alias, 'alias': child_alias, 'name': child_name,
               'id': id, 'vpath': vpath, 'groups': group_info, 'roles': role_info, 'assignments': assignment_info}
        return row

    def get_group_info(self, alias):
        resp_data = self.dataverse_client.dataverse(alias).get_groups()
        # flatten and compact it... no list comprehension though
        result_list = []
        for group in resp_data:
            #  append the number of assignees in braces
            result_list.append(group['identifier'] + ' (' + str(len(group['containedRoleAssignees'])) + ')')
        return ', '.join(result_list)

    def get_role_info(self, alias):
        resp_data = self.dataverse_client.dataverse(alias).get_roles()
        # flatten and compact it... no list comprehension though
        result_list = []
        for role in resp_data:
            #  append the number of permissions in braces
            result_list.append(role['alias'] + ' (' + str(len(role['permissions'])) + ')')
        return ', '.join(result_list)

    def get_assignment_info(self, alias):
        resp_data = self.dataverse_client.dataverse(alias).get_role_assignments()
        # flatten and compact it... no list comprehension though
        result_list = []
        for assignment in resp_data:
            #  append the role alias in braces
            result_list.append(assignment['assignee'] + ' (' + (assignment['_roleAlias']) + ')')
        return ', '.join(result_list)

    def collect_permissions_info(self, tree_data, parent_vpath, parent_alias, depth=1):
        alias = tree_data['alias']
        name = tree_data['name']
        id = tree_data['id']
        vpath = parent_vpath + self.vpath_delimiter + alias
        row = self.get_result_row(parent_alias, alias, name, id, vpath, depth)
        self.write_result_row(row)
        # only direct descendants (children)
        if 'children' in tree_data:
            for child_tree_data in tree_data['children']:
                self.collect_permissions_info(child_tree_data, vpath, alias, depth + 1)  # recurse

    def find_child(self, parent, alias):
        result = None
        for child in parent['children']:
            if child["alias"] == alias:
                result = child
                break
        return result

    def collect_permissions_info_overview(self, selected_dataverse=None):
        out_stream = sys.stdout
        if self.output_file != '-':
            try:
                out_stream = open(self.output_file, "w")
            except:
                logging.error(f"Could not open file: {self.output_file}")
                raise

        self.writer = self.create_result_writer(out_stream)

        logging.info(f'Extracting tree for server: {self.dataverse_client.server_url} ...')
        tree_data = self.dataverse_client.metrics().get_tree()
        alias = tree_data['alias']
        name = tree_data['name']
        id = tree_data['id']
        vpath = alias
        logging.info(f'Extracted the tree for the toplevel dataverse: {name} ({alias})')
        logging.info("Retrieving the info for this dataverse instance...")

        if selected_dataverse is None:
            # do whole tree
            logging.info("Retrieving the info for all the dataverse collections...")
            self.collect_permissions_info(tree_data, vpath, alias, 1)
        else:
            # always the 'root' dataverse
            row = self.get_result_row("-", alias, name, id, vpath, 0)  # The root has no parent
            self.write_result_row(row)
            # then the selected sub-verse tree
            logging.info("Retrieving the info for a selected dataverse collection sub-tree...")
            selected_tree_data = self.find_child(tree_data, selected_dataverse)
            if selected_tree_data is not None:
                self.collect_permissions_info(selected_tree_data, vpath, alias, 1)
            else:
                logging.error(f"Could not find the selected dataverse: {selected_dataverse}")

        self.writer.close()
        self.is_first = True

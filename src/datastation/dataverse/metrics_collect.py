from datastation.common.result_writer import CsvResultWriter, YamlResultWriter, JsonResultWriter
from datastation.dataverse.dataverse_client import DataverseClient
import logging
import re
import csv
import sys
import json
import rich
from datetime import timedelta


def extract_size_str(msg):
    # Example message: "Total size of the files stored in this dataverse: 43,638,426,561 bytes"
    # try parsing the size from this string.
    size_found = re.search('dataverse: (.+?) bytes', msg).group(1)
    # remove those ',' delimiters and optionally '.' as well,
    # depending on locale (there is no fractional byte!).
    # Delimiters are probably there to improve readability of those large numbers,
    # but calculating with it is problematic.
    clean_size_str = size_found.translate({ord(i): None for i in ',.'})
    return clean_size_str


class MetricsCollect:

    def __init__(self, dataverse_client: DataverseClient, output_file, output_format, dry_run: bool = False):
        self.dataverse_client = dataverse_client
        self.output_file = output_file
        self.output_format = output_format
        self.dry_run = dry_run


    # Traverses the tree and collects sizes for each dataverse using recursion.
    # Note that storing the parents size if all children sizes are also stored is redundant.
    def get_children_sizes(self, parent_data, max_depth, depth=1):
        parent_alias = parent_data['alias']
        child_result_list = []
        # only direct descendants (children)
        if 'children' in parent_data:
            for child_data in parent_data['children']:
                child_alias = child_data['alias']
                logging.info(f'Retrieving size for dataverse: {parent_alias} / {child_alias} ...')
                msg = self.dataverse_client.dataverse().get_storage_size(child_alias)
                storage_size = extract_size_str(msg)
                row = {'depth': depth, 'parentalias': parent_alias, 'alias': child_alias, 'name': child_data['name'],
                       'storagesize': storage_size}
                child_result_list.append(row)
                logging.info(f'size: {storage_size}')
                if depth < max_depth:
                    child_result_list.extend(
                        self.get_children_sizes(child_data, max_depth, depth + 1))  # recurse
        return child_result_list


    def create_result_writer(self, out_stream):
        logging.info(f'Writing output: {self.output_file}, with format : {self.output_format}')
        csv_columns = ['depth', 'parentalias', 'alias', 'name', 'storagesize']
        if self.output_format == 'csv':
            return CsvResultWriter(headers=csv_columns, out_stream=out_stream)
        else:
            return JsonResultWriter(out_stream)

    def write_output_with_writer(self, result_list):
        out_stream = sys.stdout
        if self.output_file != '-':
            out_stream = open(self.output_file, "w")
        # what if opening fails?

        # now create the writer
        writer = self.create_result_writer(out_stream)
        is_first = True
        # but why can't the writer do the bookkeeping?
        for row in result_list:
            writer.write(row, is_first)
            is_first = False

    def collect_storage_usage(self, max_depth=1, include_grand_total: bool = False):
        result_list = []
        logging.info(f'Extracting tree for server: {self.dataverse_client.server_url} ...')
        tree_data = self.dataverse_client.metrics().get_tree()

        alias = tree_data['alias']
        name = tree_data['name']
        logging.info(f'Extracted the tree for the toplevel dataverse: {name} ({alias})')

        if include_grand_total:
            logging.info("Retrieving the total size for this dataverse instance...")
            msg = self.dataverse_client.dataverse().get_storage_size(alias)
            storage_size = extract_size_str(msg)
            row = {'depth': 0, 'parentalias': alias, 'alias': alias, 'name': name,
                   'storagesize': storage_size}
            result_list.append(row)
            logging.info(f'size: {storage_size}')
        result_list.extend(self.get_children_sizes(tree_data, max_depth, 1))
        #self.write_output(result_list)
        self.write_output_with_writer(result_list)

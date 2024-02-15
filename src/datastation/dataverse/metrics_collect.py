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

        self.writer = None
        self.is_first = True  # Would be nicer if the Writer does the bookkeeping

    def create_result_writer(self, out_stream):
        logging.info(f'Writing output: {self.output_file}, with format : {self.output_format}')
        csv_columns = ['depth', 'parentalias', 'alias', 'name', 'storagesize']
        if self.output_format == 'csv':
            return CsvResultWriter(headers=csv_columns, out_stream=out_stream)
        else:
            return JsonResultWriter(out_stream)

    def write_result_row(self, row):
        self.writer.write(row, self.is_first)
        self.is_first = False  # Only the first time it can be True

    def get_result_row(self, parent_alias, child_alias, child_name, depth):
        logging.info(f'Retrieving size for dataverse: {parent_alias} / {child_alias} ...')
        msg = self.dataverse_client.dataverse(child_alias).get_storage_size()
        storage_size = extract_size_str(msg)
        logging.info(f'size: {storage_size}')
        row = {'depth': depth, 'parentalias': parent_alias, 'alias': child_alias, 'name': child_name,
               'storagesize': storage_size}
        return row

    # Traverses the tree and collects sizes for each dataverse using recursion.
    # Note that storing the parents size if all children sizes are also stored is redundant.
    def collect_children_sizes(self, parent_data, max_depth, depth=1):
        parent_alias = parent_data['alias']
        # Only direct descendants (children)
        if 'children' in parent_data:
            for child_data in parent_data['children']:
                row = self.get_result_row(parent_alias, child_data['alias'], child_data['name'], depth)
                self.write_result_row(row)

                if depth < max_depth:
                    self.collect_children_sizes(child_data, max_depth, depth + 1)  # Recurse

    def collect_storage_usage(self, max_depth=1, include_grand_total: bool = False):
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
        logging.info(f'Extracted the tree for the toplevel dataverse: {name} ({alias})')

        if include_grand_total:
            logging.info("Retrieving the total size for this dataverse instance...")
            row = self.get_result_row("-", alias, name, 0)  # The root has no parent
            self.write_result_row(row)

        self.collect_children_sizes(tree_data, max_depth, 1)
        self.writer.close()
        self.is_first = True

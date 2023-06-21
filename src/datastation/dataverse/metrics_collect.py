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

    def write_storage_usage_to_csv(self, out, result_list):
        csv_columns = ['depth', 'parentalias', 'alias', 'name', 'storagesize']
        writer = csv.DictWriter(out, fieldnames=csv_columns, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for row in result_list:
            writer.writerow(row)

    def write_output(self, result_list):
        logging.info(f'Writing output: {self.output_file}, with format : {self.output_format}')
        if self.output_file == '-':
            if self.output_format == 'csv':
                self.write_storage_usage_to_csv(sys.stdout, result_list)
            else:
                rich.print_json(data=result_list)
        else:
            if self.output_format == 'csv':
                with open(self.output_file, 'w') as f:
                    self.write_storage_usage_to_csv(f, result_list)
            else:
                with open(self.output_file, "w") as f:
                    f.write(json.dumps(result_list, indent=4))

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
        self.write_output(result_list)

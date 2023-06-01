import logging
import argparse
import json
import re
import csv
import sys

import rich

from datastation.common.config import init
from datastation.common.utils import add_dry_run_arg
from datastation.dataverse.dataverse_client import DataverseClient


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


# Traverses the tree and collects sizes for each dataverse using recursion.
# Note that storing the parents size if all children sizes are also stored is redundant.
def get_children_sizes(args, dataverse_client: DataverseClient, parent_data, max_depth, depth=1):
    parent_alias = parent_data['alias']
    child_result_list = []
    # only direct descendants (children)
    if 'children' in parent_data:
        for i in parent_data['children']:
            child_alias = i['alias']
            logging.info(f'Retrieving size for dataverse: {parent_alias} / {child_alias} ...')
            msg = dataverse_client.dataverse(child_alias).get_storage_size()
            storage_size = extract_size_str(msg)
            row = {'depth': depth, 'parentalias': parent_alias, 'alias': child_alias, 'name': i['name'],
                   'storagesize': storage_size}
            child_result_list.append(row)
            logging.info(f'size: {storage_size}')
            if depth < max_depth:
                child_result_list.extend(get_children_sizes(args, dataverse_client, i, max_depth, depth +1))  # recurse
    return child_result_list


def write_storage_usage_to_csv(out, result_list):
    csv_columns = ['depth', 'parentalias', 'alias', 'name', 'storagesize']
    writer = csv.DictWriter(out, fieldnames=csv_columns, quoting=csv.QUOTE_ALL)
    writer.writeheader()
    for row in result_list:
        writer.writerow(row)


def write_output(args, result_list):
    logging.info(f'Writing output: {args.output_file}, with format : {args.format}')
    if args.output_file == '-':
        if args.format == 'csv':
            write_storage_usage_to_csv(sys.stdout, result_list)
        else:
            rich.print_json(data=result_list)
    else:
        if args.format == 'csv':
            with open(args.output_file, 'w') as f:
                write_storage_usage_to_csv(f, result_list)
        else:
            with open(args.output_file, "w") as f:
                f.write(json.dumps(result_list, indent=4))


def collect_storage_usage(args, dataverse_client: DataverseClient):
    result_list = []
    logging.info(f'Extracting tree for server: {dataverse_client.server_url} ...')
    tree_data = dataverse_client.metrics().get_tree()

    alias = tree_data['alias']
    name = tree_data['name']
    logging.info(f'Extracted the tree for the toplevel dataverse: {name} ({alias})')

    if args.include_grand_total:
        logging.info("Retrieving the total size for this dataverse instance...")
        msg = dataverse_client.dataverse(alias).get_storage_size()
        storage_size = extract_size_str(msg)
        row = {'depth': 0, 'parentalias': alias, 'alias': alias, 'name': name,
               'storagesize': storage_size}
        result_list.append(row)
        logging.info(f'size: {storage_size}')
    result_list.extend(get_children_sizes(args, dataverse_client, tree_data, args.max_depth, 1))
    write_output(args, result_list)


def main():
    config = init()

    parser = argparse.ArgumentParser(description='Collect the storage usage for the dataverses')
    parser.add_argument('-m', '--max_depth', dest='max_depth', type=int, default=1,
                        help='the max depth of the hierarchy to traverse')
    parser.add_argument('-g', '--include-grand-total', dest='include_grand_total', action='store_true',
                        help='whether to include the grand total, which almost doubles server processing time')
    parser.add_argument('-o', '--output-file', dest='output_file', default='-',
                        help='the file to write the output to or - for stdout')
    parser.add_argument('-f', '--format', dest='format',
                        help='Output format, one of: csv, json (default: json)')

    add_dry_run_arg(parser)
    args = parser.parse_args()

    dataverse_client = DataverseClient(config['dataverse'])
    collect_storage_usage(args, dataverse_client)


if __name__ == '__main__':
    main()

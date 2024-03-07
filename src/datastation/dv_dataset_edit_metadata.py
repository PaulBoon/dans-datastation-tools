from argparse import ArgumentParser
from csv import DictReader
from os.path import expanduser, isfile

from datastation.common.batch_processing import BatchProcessor
from datastation.common.config import init
from datastation.common.utils import add_batch_processor_args, add_dry_run_arg
from datastation.dataverse.datasets import Datasets
from datastation.dataverse.dataverse_client import DataverseClient


def main():
    config = init()
    parser = ArgumentParser(description='Edits one or more, potentially published, datasets. Requires an API token.')
    parser.add_argument('-r', '--replace', dest="replace", action='store_true',
                        help="Replace existing metadata fields with the new metadata. "
                             "Required for single-value fields. "
                             "Note that without 'replace' an existing value of a multi-value field is not duplicated. "
                        )
    parser.add_argument('pid_or_file',
                        help="Either a CSV file or the PID of the dataset to edit. "
                             "One column of the file MUST have title 'PID'. "
                             "The other columns MUST have a <typeName>, as for the --value argument.")
    parser.add_argument('-v', '--value', action='append',
                        help="At least once in combination with a PID, none in combination with a CSV file. "
                             "The new values for fields must be formatted as <typeName>=<value>. "
                             "For example: title='New title'. "
                             "A subfield in a compound field must be prefixed with the <typeName> of the compound field "
                             "and an index (single-value compound fields are not implemented), for example: "
                             "--value 'author[0]authorName=the name' "
                             "--value 'author[0]authorAffiliation=the organization'. "
                             "An attempt to update a protected field will result in '403 Client Error: Forbidden'. "
                             "You may also get a 403 when updating author details without updating the authorName. "
                             "The server logs will show the details of the error. ")
    add_batch_processor_args(parser, report=False)
    add_dry_run_arg(parser)
    args = parser.parse_args()

    def run(obj_list):
        client = DataverseClient(config['dataverse'])
        datasets = Datasets(client, dry_run=args.dry_run)
        batch_processor = BatchProcessor(wait=args.wait, fail_on_first_error=args.fail_fast)
        batch_processor.process_entries(obj_list, lambda obj: datasets.update_metadata(data=obj, replace=args.replace))

    def parse_value_args():
        obj = {'PID': args.pid_or_file}
        for kv in args.value:
            key_value = kv.split('=', 2)
            obj[key_value[0]] = key_value[1]
        return obj

    if isfile(expanduser(args.pid_or_file)):
        if args.value is not None:
            parser.error("-v/--value arguments not allowed in combination with CSV file: " + args.pid_or_file)
        with open(args.pid_or_file, newline='') as csvfile:
            # restkey must be an invalid <typeName> to prevent it from being processed
            reader = DictReader(csvfile, skipinitialspace=True, restkey='rest.column')
            if reader is None or reader.fieldnames is None or len(reader.fieldnames) == 0:
                parser.error(f"{args.pid_or_file} is empty or not a CSV file.")
                return
            if 'PID' not in reader.fieldnames or len(reader.fieldnames) == 0:
                parser.error(f"No column 'PID' (or no other columns) found in " + args.pid_or_file)
                return
            run(reader)
    else:
        run([parse_value_args()])


if __name__ == '__main__':
    main()

import argparse
import json

from datastation.common.batch_processing import get_entries, BatchProcessor
from datastation.common.config import init
from datastation.common.utils import add_batch_processor_args, add_dry_run_arg
from datastation.dataverse.datasets import Datasets
from datastation.dataverse.dataverse_client import DataverseClient


def main():
    config = init()
    parser = argparse.ArgumentParser(description="Retrieves attributes of a dataset")

    attr_group = parser.add_argument_group()
    attr_group.add_argument("--user-with-role", dest="user_with_role",
                            help="List users with a specific role on the dataset",)
    attr_group.add_argument("--storage", dest="storage", action="store_true",
                            help="The storage in bytes",)

    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument('pid_or_pids_file', nargs="?",
                       help="The dataset pid, or a file with a list of pids", )
    group.add_argument("--all", dest="all_datasets", action="store_true", required=False,
                       help="All datasets in the dataverse", )

    add_batch_processor_args(parser, report=False)
    add_dry_run_arg(parser)

    args = parser.parse_args()

    attribute_options = {
        'storage': args.storage,
        'user_with_role': args.user_with_role,
    }
    if set(attribute_options.values()) == {None, False}:
        parser.error(f"Add at least one of the arguments: {', '.join(attribute_options.keys())}")

    dataverse_client = DataverseClient(config["dataverse"])

    datasets = Datasets(dataverse_client, dry_run=args.dry_run)
    if args.all_datasets:
        search_result = dataverse_client.search_api().search(dry_run=args.dry_run)
        pids = map(lambda rec: rec['global_id'], search_result)  # lazy iterator
    else:
        pids = get_entries(args.pid_or_pids_file)
    BatchProcessor(wait=args.wait, fail_on_first_error=args.fail_fast).process_entries(
        pids,
        lambda pid: print(json.dumps(datasets.get_dataset_attributes(pid, **attribute_options), skipkeys=True)))


if __name__ == "__main__":
    main()

import argparse

from datastation.common.batch_processing import get_pids, BatchProcessorWithReport
from datastation.common.config import init
from datastation.common.utils import add_batch_processor_args, add_dry_run_arg
from datastation.dataverse.dataverse_client import DataverseClient
from datastation.dataverse.destroy_placeholder_dataset import destroy_placeholder_dataset


def main():
    config = init()
    dataverse = DataverseClient(config['dataverse'])

    parser = argparse.ArgumentParser(
        description='Destroys a dataset that is a placeholder for a dataset that has not yet been migrated. In order '
                    'to validate that the dataset is a placeholder, the dataset must be published. Furthermore, '
                    'the description of the dataset must match the pattern configured in '
                    'migration_placeholders.description_text_pattern. Note, that the safety latch must also be OFF.')
    parser.add_argument('pid_or_pids_file', help='The pid of the dataset to destroy, or a file with a list of pids')
    add_batch_processor_args(parser)
    add_dry_run_arg(parser)
    args = parser.parse_args()

    batch_processor = BatchProcessorWithReport(wait=args.wait, report_file=args.report_file,
                                               headers=['PID', 'Destroyed', 'Messages'])
    pids = get_pids(args.pid_or_pids_file)
    description_text_pattern = config['migration_placeholders']['description_text_pattern']
    batch_processor.process_pids(pids,
                                 callback=lambda pid, csv_report: destroy_placeholder_dataset(dataverse.dataset(pid),
                                                                                              description_text_pattern,
                                                                                              csv_report))


if __name__ == '__main__':
    main()

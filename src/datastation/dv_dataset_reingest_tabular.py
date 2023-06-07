import argparse
import logging
from datetime import datetime

import requests

from datastation.common.batch_processing import get_pids, BatchProcessorWithReport
from datastation.common.config import init
from datastation.common.utils import add_batch_proccessor_args, add_dry_run_arg
from datastation.dataverse.dataverse_client import DataverseClient


def reingest_tabular_files_in_datasets(args, dataverse_client: DataverseClient):
    pids = get_pids(args.pid_or_pid_file)
    batch_processor = BatchProcessorWithReport(report_file=args.report_file, wait=args.wait,
                                               fail_on_first_error=args.fail_fast,
                                               headers=['DOI', 'Modified', 'Change', 'Messages'])
    batch_processor.process_pids(pids,
                                 lambda pid, csv_report: reingest_tabular_files_in_dataset(pid, dataverse_client,
                                                                                           csv_report=csv_report,
                                                                                           dry_run=args.dry_run))


def reingest_tabular_files_in_dataset(pid, dataverse_client: DataverseClient, csv_report, dry_run: bool = False):
    files = dataverse_client.dataset(pid).get_files()
    try:
        for file in files:
            file_id = file['dataFile']['id']
            try:
                dataverse_client.dataset(pid).await_unlock()
                dataverse_client.file(file_id).reingest(dry_run=dry_run)
                dataverse_client.dataset(pid).await_unlock()
                csv_report.write({'DOI': pid, 'Modified': datetime.now(), 'Change': 'Re-ingested',
                                  'Messages': 'Re-ingest was requested.'})
                logging.info(f"Re-ingested file {file_id} in dataset {pid}")
            except requests.exceptions.RequestException as re:
                # if the requests throws an exception, it might be to just tell us that the file cannot be ingested
                # as tabular, or some other reason that is a valid response. In that case, we just log it and move on
                try:
                    message = re.response.json()["message"]
                    logging.info(
                        f'[{pid}] Re-ingest not completed for file id {file_id}, reason is "{message}".')
                    csv_report.write({'DOI': pid, 'Modified': datetime.now(), 'Change': 'None', 'Messages': message})
                except:
                    pass
    except Exception as e:
        csv_report.write({'DOI': pid, 'Modified': datetime.now(), 'Change': 'Error', 'Messages': str(e)})
        logging.warning(f"Error re-ingesting files in dataset {pid}: {e}. Moving on to next dataset.")
        return


def main():
    config = init()
    dataverse = DataverseClient(config['dataverse'])

    parser = argparse.ArgumentParser(description='Re-ingest the tabular data files in one or more datasets.')
    parser.add_argument('pid_or_pid_file',
                        help='the persistent identifier of the dataset or a file containing a list of '
                             'persistent identifiers.')
    add_batch_proccessor_args(parser)
    add_dry_run_arg(parser)
    args = parser.parse_args()
    reingest_tabular_files_in_datasets(args, dataverse)

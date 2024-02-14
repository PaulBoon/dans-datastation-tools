import logging
from re import match

from datastation.common.csv import CsvReport
from datastation.dataverse.dataset_api import DatasetApi


def description_object_matches(description_text_pattern):
    def matches(description):
        value = description['dsDescriptionValue']['value']
        return match(description_text_pattern, value)

    return matches


def is_migration_file(file_metadata):
    return ('directoryLabel' in file_metadata and file_metadata['directoryLabel'] == 'easy-migration') or \
        ('directoryLabel' not in file_metadata and file_metadata['label'] == 'easy-migration.zip')


def destroy_placeholder_dataset(dataset_api: DatasetApi, description_text_pattern, csv_report: CsvReport,
                                dry_run: bool = False):
    logging.debug("Entering destroy_placeholder_dataset")
    blocker = False
    messages = []
    try:
        dataset_metadata = dataset_api.get_metadata()
        ds_description = next(
            filter(lambda m: m['typeName'] == 'dsDescription',
                   dataset_metadata['metadataBlocks']['citation']['fields']))
        descriptions = ds_description['value']
        logging.debug(f"Descriptions: {descriptions}")
        if len(list(filter(description_object_matches(description_text_pattern), descriptions))) == 0:
            blocker = True
            messages.append(f"No description found matching configured pattern: BLOCKER")
        else:
            messages.append("Description with text pattern found: OK")

        files = dataset_metadata['files']
        logging.debug(f"Files: {files}")

        if len(files) > 4:
            blocker = True
            messages.append(f"More than 4 files found: {len(files)}: BLOCKER")
        else:
            messages.append(f"Found {len(files)} files <= 4: OK")

        non_easy_migration_files = list(
            filter(lambda m: not is_migration_file(m), files))
        logging.debug(f"Non easy-migration files: {non_easy_migration_files}")

        if len(non_easy_migration_files) > 0:
            blocker = True
            messages.append(f"Files other than 'easy-migration/*' or 'easy-migration.zip' found: {len(non_easy_migration_files)}: BLOCKER")
        else:
            messages.append("Only found easy-migration files: OK")

    except Exception as e:
        blocker = True
        messages.append(f"Could not perform checks on dataset {dataset_api.get_pid()}: BLOCKER")

    if blocker:
        csv_report.write({'PID': dataset_api.get_pid(), 'Destroyed': False, 'Messages': '; '.join(messages)})
        logging.warning(f"BLOCKERS FOUND, NOT PERFORMING DESTROY FOR {dataset_api.get_pid()}")
    else:
        dataset_api.destroy(dry_run=dry_run)
        csv_report.write({'PID': dataset_api.get_pid(), 'Destroyed': True, 'Messages': '; '.join(messages)})

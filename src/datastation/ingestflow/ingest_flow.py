import json
import logging
import os
import shutil
import stat
from pathlib import Path

import requests

from datastation.common.utils import has_file_pred, has_dirtree_pred, is_sub_path_of, get_size, sizeof_fmt, \
    set_permissions, expand_path, have_subdirs_pred


def is_deposit(path):
    if os.path.exists(os.path.join(path, 'deposit.properties')):
        return True
    else:
        logging.error(f'ERROR: {path} is not a deposit')
        return False


def is_file_writeable_to_group(f):
    return os.stat(f).st_mode & stat.S_IWGRP > 0


class IngestFlow:

    def __init__(self, config, dry_run: bool = False):
        self.service_baseurl = config['service_baseurl']
        self.ingest_areas = config['ingest_areas']
        self.deposits_file_mode = config['deposits_mode']['file']
        self.deposits_dir_mode = config['deposits_mode']['directory']
        self.deposits_group = config['deposits_group']
        self.dry_run = dry_run

    def set_dry_run(self, dry_run: bool):
        self.dry_run = dry_run

    def start_ingest(self, deposit_path, is_single_deposit, continue_previous, is_migration):
        abs_deposit_path = expand_path(deposit_path)

        if is_single_deposit:
            if not has_file_pred(abs_deposit_path, is_deposit):
                print('ERROR: %s is not a deposit' % deposit_path)
                return
        else:
            if not have_subdirs_pred(abs_deposit_path, is_deposit):
                print('ERROR: %s is not a batch of deposits' % deposit_path)
                return

        if not has_dirtree_pred(abs_deposit_path, is_file_writeable_to_group):
            chmod_command = 'chmod -R g+w %s' % deposit_path
            print(f'Some files in the import batch do not give the owner group write permissions. '
                  f'Executing "{chmod_command}" to fix it')
            status = os.system(chmod_command)
            if status != 0:
                print('Could not give owner group write permissions. Possibly your account is not the owner user '
                      'of the files.')
                return

        command = {
            'inputPath': abs_deposit_path,
            'batch': not is_single_deposit,
            'continue': continue_previous
        }
        if self.dry_run:
            logging.info("DRY-RUN: only printing command, not sending it...")
            print(json.dumps(command, indent=2))
        else:
            r = requests.post(f'{self.service_baseurl}/{"migrations" if is_migration else "imports"}/:start',
                              json=command)
            print(f'Server responded: {r.text}')

    def block_target(self, target):
        url = f'{self.service_baseurl}/blocked-targets/{target}'

        if self.dry_run:
            logging.info("DRY-RUN: only printing command, not sending it...")
            print(f'Request: POST {url}')
        else:
            r = requests.post(url)
            payload = r.json()

            if 'message' not in payload:
                print(f'Unexpected output format; expecting "message" property in result, not found: {payload}')

            if r.status_code != 200:
                print(f'ERROR: {r.status_code} - {payload["message"]}')
            else:
                print(f'Server responded: {r.status_code} - {payload["message"]}')

    def unblock_target(self, target):
        url = f'{self.service_baseurl}/blocked-targets/{target}'

        if self.dry_run:
            logging.info("DRY-RUN: only printing command, not sending it...")
            print(f'Request: DELETE {url}')
        else:
            r = requests.delete(url)
            payload = r.json()

            if 'message' not in payload:
                print(f'Unexpected output format; expecting "message" property in result, not found: {payload}')

            if r.status_code != 200:
                print(f'ERROR: {r.status_code} - {payload["message"]}')
            else:
                print(f'Server responded: {r.status_code} - {payload["message"]}')

    def list_events(self, source=None, deposit=None):
        url = f'{self.service_baseurl}/events'
        params = {}
        if deposit is not None:
            params = {'deposit': deposit}
        elif source is not None:
            params = {'source': source}

        if self.dry_run:
            logging.info("DRY-RUN: only printing command, not sending it...")
            print(f'Request: GET {url}')
        else:
            r = requests.get(url, params=params)
            print(r.text)

    def progress_report(self, batch_dir):
        abs_batch_dir = os.path.abspath(batch_dir)
        ingest_area = next(filter(lambda ia: is_sub_path_of(abs_batch_dir, expand_path(self.ingest_areas[ia]['inbox'])),
                                  self.ingest_areas), None)
        if ingest_area is None:
            print("ERROR: batch_dir {} does not seems to be in one of the inboxes: {}".format(batch_dir, ingest_area))
            return 1
        else:
            logging.debug("Found ingest_area: {}".format(ingest_area))
            rel_batch_dir = os.path.relpath(abs_batch_dir, self.ingest_areas[ingest_area]['inbox'])
            logging.debug("Relative batch dir: {}".format(rel_batch_dir))
            abs_out_dir = os.path.join(self.ingest_areas[ingest_area]['outbox'], rel_batch_dir)
            logging.debug("Absolute out dir : {}".format(abs_out_dir))

            todo = len(os.listdir(abs_batch_dir))
            processed = len(os.listdir(os.path.join(abs_out_dir, 'processed')))
            rejected = len(os.listdir(os.path.join(abs_out_dir, 'rejected')))
            failed = len(os.listdir(os.path.join(abs_out_dir, 'failed')))

            # Get total size of folders
            size_todo = sizeof_fmt(get_size(abs_batch_dir))
            size_processed = sizeof_fmt(get_size(os.path.join(abs_out_dir, 'processed')))
            size_rejected = sizeof_fmt(get_size(os.path.join(abs_out_dir, 'rejected')))
            size_failed = sizeof_fmt(get_size(os.path.join(abs_out_dir, 'failed')))

            os.system("date")
            print(f"todo = {todo} ({size_todo}) / processed = {processed} ({size_processed}) "
                  f"/ rejected = {rejected} ({size_rejected}) / failed = {failed} ({size_failed})")
            print()

    def copy_batch_to_ingest_area(self, source, target):
        """Copies a batch from source to target. Source must be an existing batch directory, target must be a location
        inside one of the inboxes of the ingest area. If target is a directory with the same name as source,
        the contents of source will be copied. If target is a directory with a different name, the contents of source
        will be copied into a new directory with the name of source in target.

        After copying, the mode of the copied files and directories will be set to the value configured in the ingest
        area."""

        abs_source = expand_path(source)
        abs_target = expand_path(target)
        if not os.path.isdir(abs_source):
            print(f"ERROR: source {source} is not a directory")
            return 1

        ingest_area = next(filter(lambda ia: is_sub_path_of(abs_target, expand_path(self.ingest_areas[ia]['inbox'])),
                                  self.ingest_areas), None)
        if ingest_area is None:
            print(
                f"ERROR: target {target} does not seem to be in one of the inboxes of the ingest areas: {ingest_area}")
            return 1
        else:
            logging.debug(f"Found ingest_area: {ingest_area}")

        # Check if target is a directory with the same name as source
        if os.path.basename(abs_source) == os.path.basename(abs_target):
            target_dir = abs_target
        else:
            target_dir = os.path.join(abs_target, os.path.basename(abs_source))

        if os.path.exists(target_dir):
            for f in os.listdir(abs_source):
                shutil.copy(os.path.join(abs_source, f), target_dir)
        else:
            shutil.copytree(abs_source, target_dir)

        print("Setting mode and group of copied files and directories...")
        set_permissions(target_dir, file_mode=self.deposits_file_mode,
                        dir_mode=self.deposits_dir_mode, group=self.deposits_group)

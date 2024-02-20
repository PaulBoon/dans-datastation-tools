import logging
import os
import time

from datastation.common.csv import CsvReport


def get_pids(pid_or_pids_file, search_api=None, query="*", subtree="root", object_type="dataset", dry_run=False):
    """ kept for backward compatibility"""
    return get_entries(pid_or_pids_file, search_api, query, subtree, object_type, dry_run)


def get_entries(entries, search_api=None, query="*", subtree="root", object_type="dataset", dry_run=False):
    """

    Args:
        entries:          A string (e.g. a PID for the default object_type 'dataset'),
                          or a file with a list of strings or dict objects.
        search_api:       must be provided if entries is None
        query:            passed on to search_api().search
        object_type:      passed on to search_api().search
        subtree (object): passed on to search_api().search
        dry_run:          Do not perform the action, but show what would be done.
                          Only applicable if entries is None.

    Returns: an iterator with strings or dict objects.
             if entries is not provided, it searches for all objects of object_type
             and extracts their pids, fetching the result pages lazy.
    """
    if entries is None:
        result = search_api.search(query=query, subtree=subtree, object_type=object_type, dry_run=dry_run)
        return map(lambda rec: rec['global_id'], result)
    elif os.path.isfile(os.path.expanduser(entries)):
        objects = []
        with open(os.path.expanduser(entries)) as f:
            for line in f:
                objects.append(line.strip())
        return objects
    else:
        return [entries]


class BatchProcessor:
    def __init__(self, wait=0.1, fail_on_first_error=True):
        self.wait = wait
        self.fail_on_first_error = fail_on_first_error

    def process_pids(self, entries, callback):
        """ kept for backward compatibility"""
        return self.process_entries(entries, callback)

    def process_entries(self, entries, callback):
        """ The callback is called for each entry in entries.

        Args:
            entries:  a single string (e.g. PID) or dict, or a list of string or dicts
            callback: a function that takes a single entry as argument
        Returns:
            None

        If an entry is a string or a dictionary with key 'PID',
        the value is used for progress logging.
        """
        if type(entries) is list:
            num_entries = len(entries)
            logging.info(f"Start batch processing on {num_entries} entries")
        else:
            logging.info(f"Start batch processing on unknown number of entries")
            num_entries = -1
        i = 0
        for obj in entries:
            i += 1
            try:
                if self.wait > 0 and i > 1:
                    logging.debug(f"Waiting {self.wait} seconds before processing next entry")
                    time.sleep(self.wait)
                if type(obj) is dict and 'PID' in obj.keys():
                    logging.info(f"Processing {i} of {num_entries}: {obj['PID']}")
                elif type(obj) is str:
                    logging.info(f"Processing {i} of {num_entries}: {obj}")
                else:
                    logging.info(f"Processing {i} of {num_entries}")
                callback(obj)
            except Exception as e:
                logging.exception("Exception occurred", exc_info=True)
                if self.fail_on_first_error:
                    logging.error(f"Stop processing because of an exception: {e}")
                    break
                logging.debug("fail_on_first_error is False, continuing...")


class BatchProcessorWithReport(BatchProcessor):

    def __init__(self, report_file=None, headers=None, wait=0.1, fail_on_first_error=True):
        super().__init__(wait, fail_on_first_error)
        if headers is None:
            headers = ["DOI", "Modified", "Change"]
        self.report_file = report_file
        self.headers = headers

    def process_pids(self, entries, callback):
        """ kept for backward compatibility"""
        return self.process_entries(entries, callback)

    def process_entries(self, entries, callback):
        with CsvReport(os.path.expanduser(self.report_file), self.headers) as csv_report:
            super().process_entries(entries, lambda entry: callback(entry, csv_report))

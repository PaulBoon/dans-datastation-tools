import logging
import os
import time

from datastation.common.csv import CsvReport
from datastation.common.utils import plural


# Base class for batch processing of items
class CommonBatchProcessor:
    def __init__(self, item_name="item", wait=0.1, fail_on_first_error=True):
        self.item_name = item_name
        self.wait = wait
        self.fail_on_first_error = fail_on_first_error

    def process_items(self, items, callback):
        if type(items) is list:
            num_items = len(items)
            logging.info(f"Start batch processing on {num_items} {plural(self.item_name)}")
        else:
            logging.info(f"Start batch processing on unknown number of {plural(self.item_name)}")
            num_items = -1
        i = 0
        for item in items:
            i += 1
            try:
                if self.wait > 0 and i > 1:
                    logging.debug(f"Waiting {self.wait} seconds before processing next {self.item_name}")
                    time.sleep(self.wait)
                logging.info(f"Processing {i} of {num_items}: {item}")
                callback(item)
            except Exception as e:
                logging.exception("Exception occurred", exc_info=True)
                if self.fail_on_first_error:
                    logging.error(f"Stop processing because of an exception: {e}")
                    break
                logging.debug("fail_on_first_error is False, continuing...")


def get_provided_items_iterator(item_or_items_file, item_name="item"):
    if item_or_items_file is None:
        logging.debug(f"No {plural(item_name)} provided.")
        return None
    elif os.path.isfile(os.path.expanduser(item_or_items_file)):
        items = []
        with open(os.path.expanduser(item_or_items_file)) as f:
            for line in f:
                items.append(line.strip())
        return items
    else:
        return [item_or_items_file]


def get_pids(pid_or_pids_file, search_api=None, query="*", subtree="root", object_type="dataset", dry_run=False):
    """

    Args:
        pid_or_pids_file: The dataset pid, or a file with a list of pids.
        search_api:       must be provided if pid_or_pids_file is None
        query:            passed on to search_api().search
        object_type:      passed on to search_api().search
        subtree (object): passed on to search_api().search
        dry_run:          Do not perform the action, but show what would be done.
                          Only applicable if pid_or_pids_file is None.

    Returns: an iterator with pids,
             if pid_or_pids_file is not provided, it searches for all datasets
             and extracts their pids, fetching the result pages lazy.
    """
    if pid_or_pids_file is None:
        result = search_api.search(query=query, subtree=subtree, object_type=object_type, dry_run=dry_run)
        return map(lambda rec: rec['global_id'], result)
    else:
        return get_provided_items_iterator(pid_or_pids_file, "pid")


def get_aliases(alias_or_aliases_file, dry_run=False):
    """

    Args:
        alias_or_aliases_file: The dataverse alias, or a file with a list of aliases.
        dry_run:          Do not perform the action, but show what would be done.
                          Only applicable if pid_or_pids_file is None.

    Returns: an iterator with aliases
    """
    if alias_or_aliases_file is None:
        # The tree of all (published) dataverses could be retrieved and aliases could recursively be extracted
        # from the tree, but this is not implemented yet.
        logging.warning(f"No aliases provided, nothing to do.")
        return None
    else:
        return get_provided_items_iterator(alias_or_aliases_file, "alias")


class DatasetBatchProcessor(CommonBatchProcessor):

    def __init__(self, wait=0.1, fail_on_first_error=True):
        super().__init__("pid", wait, fail_on_first_error)

    def process_pids(self, pids, callback):
        super().process_items(pids, callback)


class DatasetBatchProcessorWithReport(DatasetBatchProcessor):

    def __init__(self, report_file=None, headers=None, wait=0.1, fail_on_first_error=True):
        super().__init__(wait, fail_on_first_error)
        if headers is None:
            headers = ["DOI", "Modified", "Change"]
        self.report_file = report_file
        self.headers = headers

    def process_pids(self, pids, callback):
        with CsvReport(os.path.expanduser(self.report_file), self.headers) as csv_report:
            super().process_pids(pids, lambda pid: callback(pid, csv_report))


class DataverseBatchProcessor(CommonBatchProcessor):

    def __init__(self, wait=0.1, fail_on_first_error=True):
        super().__init__("alias", wait, fail_on_first_error)

    def process_aliases(self, aliases, callback):
        super().process_items(aliases, callback)


class DataverseBatchProcessorWithReport(DataverseBatchProcessor):

    def __init__(self, report_file=None, headers=None, wait=0.1, fail_on_first_error=True):
        super().__init__(wait, fail_on_first_error)
        if headers is None:
            headers = ["alias", "Modified", "Change"]
        self.report_file = report_file
        self.headers = headers

    def process_aliases(self, aliases, callback):
        with CsvReport(os.path.expanduser(self.report_file), self.headers) as csv_report:
            super().process_aliases(aliases, lambda alias: callback(alias, csv_report))

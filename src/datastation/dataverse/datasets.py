import json
import logging
import sys

from datastation.common.result_writer import JsonResultWriter
from datastation.dataverse.dataverse_client import DataverseClient


class Datasets:
    def __init__(self, dataverse_client: DataverseClient, dry_run: bool = False):
        self.dataverse_client = dataverse_client
        self.dry_run = dry_run

    def print_dataset_attributes(self, storage: bool, role: str, pid: str):
        def process_dataset_attributes(
                dataset: dict,
                storage: bool,
                role: str,
                result_writer: JsonResultWriter = None,
                is_first: bool = False
        ):
            # convert search result to dataset if needed
            if "global_id" in dataset:
                result = self.dataverse_client.dataset(dataset["global_id"]).get(
                    dry_run=self.dry_run
                )

                if result is None:
                    raise Exception("Dataset not found")

                dataset = result

            pid = dataset["datasetPersistentId"]
            attributes = {"pid": pid}

            if storage:
                if dataset:
                    attributes["storage"] = sum(
                        f["dataFile"]["filesize"] for f in dataset["files"]
                    )
                else:
                    raise Exception("Dataset not found")

            if role is not None:
                role_assignments = self.dataverse_client.dataset(
                    pid
                ).get_role_assignments(dry_run=self.dry_run)

                if role_assignments is not None:
                    attributes["users"] = [
                        user["assignee"].replace("@", "")
                        for user in role_assignments
                        if user["_roleAlias"] == role
                    ]

            if result_writer:
                result_writer.write(attributes, is_first)
            else:
                print(json.dumps(attributes, indent=2))

        # a single dataset
        if pid is not None:
            dataset = self.dataverse_client.dataset(pid).get(dry_run=self.dry_run)

            if dataset:
                process_dataset_attributes(dataset, storage, role)
            else:
                raise Exception("Dataset not found")

            return

        else:
            # all datasets
            contents = self.dataverse_client.dataverse().search(dry_run=self.dry_run)

            result_writer = JsonResultWriter(out_stream=sys.stdout)
            first = True

            try:
                for result in contents:
                    try:
                        process_dataset_attributes(result, storage, role, result_writer, is_first=first)
                        first = False
                    except Exception as e:
                        logging.error(f"Error processing dataset {result['global_id']}: {e}")
                        continue
            finally:
                result_writer.close()

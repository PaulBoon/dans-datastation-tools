from datastation.common.csv import CsvInput
from datastation.common.database import Database
from datastation.dataverse.builtin_users import User
from datastation.dataverse.dataverse_client import DataverseClient
import logging
from datetime import timedelta


class Notifications:

    def __init__(self, dataverse_client: DataverseClient, dry_run: bool = False):
        self.dataverse_client = dataverse_client
        self.dry_run = dry_run

    def cleanup(self, days_old):
        logging.info("Cleaning up notifications older than %s days", days_old)

        with self.dataverse_client.database() as database:
            notifications_older_than = timedelta(days=days_old)

            delete_statement = """
                delete
                from usernotification
                where senddate < now() - %s
            """

            if self.dry_run:
                logging.info(f"dry-run, not updating database with {delete_statement % (notifications_older_than,)}")
                return

            amount_deleted = database.update(delete_statement, (notifications_older_than,))
            logging.info("Deleted %s notifications", amount_deleted)




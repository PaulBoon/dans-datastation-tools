from datetime import datetime

import rich

from datastation.dataverse.dataverse_api import DataverseApi
from datastation.dataverse.dataverse_client import DataverseClient


class DataverseRole:

    def __init__(self, dataverse_client: DataverseClient, dry_run: bool = False):
        self.dataverse_client = dataverse_client
        self.dry_run = dry_run

    def list_role_assignments(self, alias):
        r = self.dataverse_client.dataverse(alias).get_role_assignments(self.dry_run)
        if r is not None:
            rich.print_json(data=r)

    def add_role_assignment(self, role_assignment, dataverse_api: DataverseApi, csv_report):
        assignee = role_assignment.split('=')[0]
        role = role_assignment.split('=')[1]
        action = "None"
        if self.in_current_assignments(assignee, role, dataverse_api):
            print("{} is already {} for dataset {}".format(assignee, role, dataverse_api.get_alias()))
        else:
            print(
                "Adding {} as {} for dataset {}".format(assignee, role, dataverse_api.get_alias()))
            dataverse_api.add_role_assignment(assignee, role, dry_run=self.dry_run)
            action = "Added"
        csv_report.write(
            {'alias': dataverse_api.get_alias(), 'Modified': datetime.now(), 'Assignee': assignee, 'Role': role,
             'Change': action})

    @staticmethod
    def in_current_assignments(assignee, role, dataverse_api: DataverseApi):
        current_assignments = dataverse_api.get_role_assignments()
        found = False
        for current_assignment in current_assignments:
            if current_assignment.get('assignee') == assignee and current_assignment.get(
                    '_roleAlias') == role:
                found = True
                break
        return found

    def remove_role_assignment(self, role_assignment, dataverse_api: DataverseApi, csv_report):
        assignee = role_assignment.split('=')[0]
        role = role_assignment.split('=')[1]
        action = "None"
        if self.in_current_assignments(assignee, role, dataverse_api):
            print("Removing {} as {} for dataverse {}".format(assignee, role, dataverse_api.get_alias()))
            all_assignments = dataverse_api.get_role_assignments()
            for assignment in all_assignments:
                if assignment.get('assignee') == assignee and assignment.get('_roleAlias') == role:
                    dataverse_api.remove_role_assignment(assignment.get('id'), dry_run=self.dry_run)
                    action = "Removed"
                    break
        else:
            print("{} is not {} for dataverse {}".format(assignee, role, dataverse_api.get_alias()))
        csv_report.write(
            {'alias': dataverse_api.get_alias(), 'Modified': datetime.now(), 'Assignee': assignee, 'Role': role,
             'Change': action})

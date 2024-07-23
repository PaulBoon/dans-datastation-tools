import argparse
from datetime import datetime

import rich

from datastation.common.batch_processing import get_pids, BatchProcessorWithReport
from datastation.common.config import init
from datastation.common.utils import add_batch_processor_args, add_dry_run_arg
from datastation.dataverse.dataset_api import DatasetApi
from datastation.dataverse.dataverse_client import DataverseClient


def add_role_assignments(args, dataverse_client: DataverseClient):
    pids = get_pids(args.pid_or_pid_file)
    batch_processor = BatchProcessorWithReport(wait=args.wait, fail_on_first_error=args.fail_fast,
                                               report_file=args.report_file,
                                               headers=['DOI', 'Modified', 'Assignee', 'Role', 'Change'])
    batch_processor.process_pids(pids,
                                 lambda pid, csv_report: add_role_assignment(args.role_assignment,
                                                                             dataset_api=dataverse_client.dataset(pid),
                                                                             csv_report=csv_report))


def add_role_assignment(role_assignment, dataset_api: DatasetApi, csv_report, dry_run: bool = False):
    assignee = role_assignment.split('=')[0]
    role = role_assignment.split('=')[1]
    action = "None"
    if in_current_assignments(assignee, role, dataset_api):
        print("{} is already {} for dataset {}".format(assignee, role, dataset_api.get_pid()))
    else:
        print(
            "Adding {} as {} for dataset {}".format(assignee, role, dataset_api.get_pid()))
        dataset_api.add_role_assignment(assignee, role, dry_run=dry_run)
        action = "Added"
    csv_report.write(
        {'DOI': dataset_api.get_pid(), 'Modified': datetime.now(), 'Assignee': assignee, 'Role': role,
         'Change': action})


def in_current_assignments(assignee, role, dataset_api: DatasetApi):
    current_assignments = dataset_api.get_role_assignments()
    found = False
    for current_assignment in current_assignments:
        if current_assignment.get('assignee') == assignee and current_assignment.get(
                '_roleAlias') == role:
            found = True
            break
    return found


def list_role_assignments(args, dataverse_client):
    r = dataverse_client.dataset(args.pid).get_role_assignments()
    if r is not None:
        rich.print_json(data=r)


def remove_role_assignments(args, dataverse_client: DataverseClient):
    pids = get_pids(args.pid_or_pid_file)
    batch_processor = BatchProcessorWithReport(wait=args.wait, report_file=args.report_file,
                                               headers=['DOI', 'Modified', 'Assignee', 'Role', 'Change'])
    batch_processor.process_pids(pids,
                                 lambda pid, csv_report: remove_role_assignment(args.role_assignment,
                                                                                dataset_api=dataverse_client.dataset(
                                                                                    pid),
                                                                                csv_report=csv_report))


def remove_role_assignment(role_assignment, dataset_api: DatasetApi, csv_report, dry_run: bool = False):
    assignee = role_assignment.split('=')[0]
    role = role_assignment.split('=')[1]
    action = "None"
    if in_current_assignments(assignee, role, dataset_api):
        print("Removing {} as {} for dataset {}".format(assignee, role, dataset_api.get_pid()))
        all_assignments = dataset_api.get_role_assignments()
        for assignment in all_assignments:
            if assignment.get('assignee') == assignee and assignment.get('_roleAlias') == role:
                dataset_api.remove_role_assignment(assignment.get('id'), dry_run=dry_run)
                action = "Removed"
                break
    else:
        print("{} is not {} for dataset {}".format(assignee, role, dataset_api.get_pid()))
    csv_report.write(
        {'DOI': dataset_api.get_pid(), 'Modified': datetime.now(), 'Assignee': assignee, 'Role': role,
         'Change': action})


def main():
    config = init()
    dataverse_client = DataverseClient(config['dataverse'])
    batch_processor = BatchProcessorWithReport(headers=['DOI', 'Modified', 'Assignee', 'Role', 'Change'])

    # Create main parser and subparsers
    parser = argparse.ArgumentParser(description='Manage role assignments on one or more datasets.')
    subparsers = parser.add_subparsers(help='subcommands', dest='subcommand')

    # Add role assignment
    parser_add = subparsers.add_parser('add', help='add role assignment to specified dataset(s)')
    parser_add.add_argument('role_assignment',
                            help='role assignee and alias (example: @dataverseAdmin=contributor) to add')
    parser_add.add_argument('pid_or_pid_file', help='the dataset pid or the input file with the dataset pids')
    add_batch_processor_args(parser_add)
    add_dry_run_arg(parser_add)

    parser_add.set_defaults(func=lambda _: add_role_assignments(_, dataverse_client))

    # Remove role assignment
    parser_remove = subparsers.add_parser('remove', help='remove role assignment from specified dataset(s)')
    parser_remove.add_argument('role_assignment',
                               help='role assignee and alias (example: @dataverseAdmin=contributor)')
    parser_remove.add_argument('pid_or_pid_file', help='The dataset pid or the input file with the dataset pids')
    add_batch_processor_args(parser_remove)
    add_dry_run_arg(parser_remove)
    parser_remove.set_defaults(func=lambda _: remove_role_assignments(_, dataverse_client))

    # List role assignments
    parser_list = subparsers.add_parser('list',
                                        help='list role assignments for specified dataset (only one pid allowed)')
    parser_list.add_argument('pid', help='the dataset pid')
    add_dry_run_arg(parser_list)
    parser_list.set_defaults(func=lambda _: list_role_assignments(_, dataverse_client))

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()

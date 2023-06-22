import argparse
import sys

from datastation.common.result_writer import CsvResultWriter, JsonResultWriter, YamlResultWriter
from datastation.common.utils import add_dry_run_arg
from datastation.common.config import init
from datastation.dans_bag.validate_dans_bag import ValidateDansBag


def create_result_writer(file_format):
    if file_format == 'csv':
        return CsvResultWriter(headers=['Bag location', 'Information package type', 'Is compliant',
                                        'Name', 'Profile version', 'Rule violations'],
                               out_stream=sys.stdout)
    elif file_format == 'yaml':
        return YamlResultWriter(out_stream=sys.stdout)
    else:
        return JsonResultWriter(out_stream=sys.stdout)


def main():
    config = init()

    default_information_package_type = config['validate_dans_bag']['default_information_package_type']
    parser = argparse.ArgumentParser(
        description='Validate one or more bags to see if they comply with the DANS BagIt Profile v1')
    parser.add_argument('path', metavar='<batch-or-deposit-or-bag>',
                        help='Directory containing a bag, a deposit or a batch of deposits')
    parser.add_argument('-t', '--information-package-type', dest='info_package_type',
                        help=f'Which information package type to validate this bag as (default: '
                             f'{default_information_package_type})',
                        choices=['DEPOSIT', 'MIGRATION'],
                        default=default_information_package_type)
    parser.add_argument('-f', '--format', dest='format',
                        help='Output format, one of: csv, json, yaml (default: json)')
    parser.add_argument('-a', '--accept', dest='accept', default='application/json',
                        help='Accept header to send to server. Note that the server only supports application/json and'
                             'text/plain, the latter will return YAML. This option is only useful for debugging '
                             'purposes. (default: application/json)')
    add_dry_run_arg(parser)

    args = parser.parse_args()
    validate_dans_bag = ValidateDansBag(config['validate_dans_bag'], args.accept)
    validate_dans_bag.validate(args.path, args.info_package_type, create_result_writer(args.format),
                               dry_run=args.dry_run)


if __name__ == '__main__':
    main()

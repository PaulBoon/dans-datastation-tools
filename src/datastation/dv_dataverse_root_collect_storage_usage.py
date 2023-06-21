import argparse


from datastation.common.config import init
from datastation.common.utils import add_dry_run_arg
from datastation.dataverse.dataverse_client import DataverseClient
from datastation.dataverse.metrics_collect import MetricsCollect


def main():
    config = init()

    parser = argparse.ArgumentParser(description='Collect the storage usage for the dataverses')
    parser.add_argument('-m', '--max-depth', dest='max_depth', type=int, default=1,
                        help='the max depth of the hierarchy to traverse')
    parser.add_argument('-g', '--include-grand-total', dest='include_grand_total', action='store_true',
                        help='whether to include the grand total, which almost doubles server processing time')
    parser.add_argument('-o', '--output-file', dest='output_file', default='-',
                        help='the file to write the output to or - for stdout')
    parser.add_argument('-f', '--format', dest='format',
                        help='Output format, one of: csv, json (default: json)')

    add_dry_run_arg(parser)
    args = parser.parse_args()

    dataverse_client = DataverseClient(config['dataverse'])
    collector = MetricsCollect(dataverse_client, args.output_file, args.format, args.dry_run)
    collector.collect_storage_usage(args.max_depth, args.include_grand_total)

if __name__ == '__main__':
    main()

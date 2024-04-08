import argparse

import yaml
import sys
import rich
from rich.console import Console
from rich.table import Table

from datastation.common.config import init
from datastation.common.version_info import get_rpm_versions, get_dataverse_version, get_dataverse_build_number, \
    get_payara_version


def get_config_version_info(config):
    if 'version_info' in config:
        return config['version_info']
    else:
        default_version_info = {
            'dans_rpm_module_prefix': 'dans.knaw.nl-',
            'dataverse_application_path': '/var/lib/payara6/glassfish/domains/domain1/applications/dataverse/',
            'payara_install_path': '/usr/local/payara6'
        }
        rich.print('WARNING: No version_info section in config file. Using default values.')
        rich.print('To get rid of this warning, add a version_info section to your config file:')
        yaml.dump({'version_info': default_version_info}, sys.stdout)
        return default_version_info


def main():
    config = init()

    parser = argparse.ArgumentParser(
        description='Gets the version of all Data Station components in this installation.')
    parser.add_argument('--json', dest='json', action='store_true', help='Output as JSON')
    args = parser.parse_args()

    version_info = get_config_version_info(config)

    components = get_rpm_versions(version_info['dans_rpm_module_prefix'])
    dataverse_version = get_dataverse_version(version_info['dataverse_application_path'])
    dataverse_build_number = get_dataverse_build_number(version_info['dataverse_application_path'])
    components['dataverse'] = f'{dataverse_version} build {dataverse_build_number}'
    payara_version = get_payara_version(version_info['payara_install_path'])
    components['payara'] = payara_version

    if args.json:
        rich.print(components)
        return
    else:
        table = Table(title="Data Station Component Versions")
        table.add_column("Component")
        table.add_column("Version")
        # Get a sorted list of keys into the components dictionary
        keys = list(components.keys())
        keys.sort()
        for key in keys:
            table.add_row(key, components[key])
        console = Console()
        console.print(table)


if __name__ == '__main__':
    main()

# Copyright 2020 EMBL - European Bioinformatics Institute
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import argparse
import json
import sys

from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from ebi_eva_common_pyutils.assembly import NCBIAssembly

logger = log_cfg.get_logger(__name__)


def get_args_from_private_config_file(private_config_file):
    with open(private_config_file) as private_config_file_handle:
        if 'json' in private_config_file:
            return json.load(private_config_file_handle)


def main(assembly_accession, species_name, output_directory, private_config_file, clear):
    private_config_args = get_args_from_private_config_file(private_config_file)
    eutils_api_key = private_config_args['eutils_api_key']
    assembly = NCBIAssembly(assembly_accession, species_name, output_directory, eutils_api_key=eutils_api_key)
    assembly.download_or_construct(overwrite=clear)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Genome downloader assembly', add_help=False)
    parser.add_argument("-a", "--assembly-accession",
                        help="Assembly for which the process has to be run, e.g. GCA_000002285.2", required=True)
    parser.add_argument("-s", "--species",
                        help="Species scientific name under which this accession should be stored. "
                             "This is only used to create the directory", required=True)
    parser.add_argument("-o", "--output-directory", help="Base directory under which all species assemblies are stored.",
                        required=True)
    parser.add_argument("-p", "--private-config-file",
                        help="Path to the configuration file with private configuration file (XML format)",
                        required=True)
    parser.add_argument("-c", "--clear", help="Flag to clear existing data in FASTA file and starting from scratch",
                        action='store_true')
    parser.add_argument('--help', action='help', help='Show this help message and exit')

    args = {}

    # Output the logs to stderr
    log_cfg.add_stderr_handler()

    try:
        args = parser.parse_args()
        main(args.assembly_accession, args.species, args.output_directory, args.private_config_file, args.clear)
    except Exception as ex:
        logger.exception(ex)
        sys.exit(1)

    sys.exit(0)

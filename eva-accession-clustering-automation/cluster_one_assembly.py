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
import os
import sys
import datetime
import logging
from create_clustering_properties import create_properties_file
from create_clustering_properties import check_valid_sources
from create_clustering_properties import check_vcf_source_requirements
from config_custom import get_args_from_private_config_file
from ebi_eva_common_pyutils.command_utils import run_command_with_output

logger = logging.getLogger(__name__)
timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

def generate_bsub_command(assembly_accession, properties_path, clustering_artifact, automation_timestamp):
    job_name = 'cluster_' + assembly_accession
    log_file = assembly_accession + '_cluster_' + timestamp + '.log'
    error_file = assembly_accession + '_cluster_' + timestamp + '.err'

    command = 'bsub -J ' + job_name + ' -o ' + log_file + ' -e ' + error_file + ' -M 8192 -R "rusage[mem=8192]" ' + \
              'java -jar ' + clustering_artifact + ' -Dspring.config.location=' + properties_path

    print(command)
    add_to_command_file(properties_path, command, automation_timestamp)
    return command


def add_to_command_file(properties_path, command, automation_timestamp):
    """
    This method writes the commands to a text file in the output folder. If it was run using the
    cluster_multiple_assemblies method the automation timestamp will be provided and used. That way the commands will
    be on the same file when ran from the automation script.
    """
    commands_path = os.path.dirname(properties_path) + '/commands_'
    if automation_timestamp:
        commands_path += automation_timestamp
    else:
        commands_path +=  timestamp
    commands_path += '.txt'

    with open(commands_path, 'a+') as commands:
        commands.write(command + '\n')


def run_clustering(source, vcf_file, project_accession, assembly_accession, private_config_file,
                   private_config_xml_file, profile, output_directory, clustering_artifact, only_printing,
                   automation_timestamp):
    preliminary_check(source, vcf_file, project_accession)
    clustering_artifact_path = get_clustering_artifact(clustering_artifact, private_config_file)
    properties_path = create_properties_file(source, vcf_file, project_accession, assembly_accession,
                                             private_config_xml_file, profile, output_directory)
    command = generate_bsub_command(assembly_accession, properties_path, clustering_artifact_path, automation_timestamp)
    if not only_printing:
        run_command_with_output('Run clustering command', command, return_process_output=True)


def preliminary_check(source, vcf_file, project_accession):
    """
    This checks must pass in order to run the script
    """
    check_valid_sources(source)
    check_vcf_source_requirements(source, vcf_file, project_accession)


def get_clustering_artifact(clustering_artifact_arg, private_config_file):
    """
    This method checks the artifact was passed to the script either using the parameter (-ca, --clustering-artifact) or
    in the private configuration file, and gets its value. The parameter is prioritized.
    """
    if clustering_artifact_arg:
        return clustering_artifact_arg

    private_config_args = get_args_from_private_config_file(private_config_file)
    if private_config_args.get('clustering_artifact'):
        return private_config_args['clustering_artifact']

    logger.error('If the clustering artifact must be provided either by the parameters (-ca, --clustering-artifact) or'
                 'in the private configuration file')
    sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cluster one assembly',
                                     add_help=False)
    parser.add_argument("--source", help="mongo database or VCF", required=True)
    parser.add_argument("--vcf-file", help="Path to the VCF file, required when the source is VCF",
                        required=False)
    parser.add_argument("--project-accession", help="Project accession, required when the source is VCF",
                        required=False)
    parser.add_argument("--assembly-accession",
                        help="Assembly for which the process has to be run, e.g. GCA_000002285.2", required=True)
    parser.add_argument("--private-config-file",
                        help="Path to the configuration file with private info (JSON/YML format)", required=True)
    parser.add_argument("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
    parser.add_argument("--profile", help="Profile to get the properties, e.g.production", required=True)
    parser.add_argument("--output-directory", help="Output directory for the properties file", required=False)
    parser.add_argument("--clustering-artifact", help="Artifact of the clustering pipeline",
                        required=False)
    parser.add_argument("--only-printing", help="Prepare and write the commands, but don't run them",
                        action="store_true", required=False)
    parser.add_argument("--automation-timestamp", help="Timestamp from the automation script (run_multiple_assemblies)",
                        required=False)
    parser.add_argument("--help", action="help", help="Show this help message and exit")

    args = {}
    try:
        args = parser.parse_args()
        run_clustering(args.source, args.vcf_file, args.project_accession, args.assembly_accession,
                       args.private_config_file, args.private_config_xml_file, args.profile, args.output_directory,
                       args.clustering_artifact,  args.only_printing, args.automation_timestamp)
    except Exception as ex:
        logger.exception(ex)
        sys.exit(1)

    sys.exit(0)

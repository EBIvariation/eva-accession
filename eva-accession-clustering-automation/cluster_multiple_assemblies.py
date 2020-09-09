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

import os
import argparse
import sys
import logging
import datetime
from create_clustering_properties import create_properties_file
from create_clustering_properties import check_valid_sources
from config_custom import get_args_from_private_config_file
from create_clustering_properties import check_valid_sources
from ebi_eva_common_pyutils.command_utils import run_command_with_output

logger = logging.getLogger(__name__)
timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")


def generate_bsub_command(assembly_accession, properties_path, clustering_artifact):
    job_name = 'cluster_' + assembly_accession
    log_file = assembly_accession + '_cluster_' + timestamp + '.log'
    error_file = assembly_accession + '_cluster_' + timestamp + '.err'

    command = 'bsub -J ' + job_name + ' -o ' + log_file + ' -e ' + error_file + ' -M 8192 -R "rusage[mem=8192]" ' + \
              'java -jar ' + clustering_artifact + ' -Dspring.config.location=' + properties_path

    print(command)
    add_to_command_file(properties_path, command)
    return command


def add_to_command_file(properties_path, command):
    """
    This method writes the commands to a text file in the output folder
    """
    commands_path = os.path.dirname(properties_path) + '/commands_' + timestamp + '.txt'
    with open(commands_path, 'a+') as commands:
        commands.write(command + '\n')


def run_clustering(source, vcf_file, project_accession, assembly_accession, private_config_file,
                   private_config_xml_file, profile, output_directory, clustering_artifact, only_printing):
    properties_path = create_properties_file(source, vcf_file, project_accession, assembly_accession,
                                             private_config_file, private_config_xml_file, profile, output_directory)
    clustering_artifact_path = get_clustering_artifact(clustering_artifact, private_config_file)
    command = generate_bsub_command(assembly_accession, properties_path, clustering_artifact_path)
    if not only_printing:
        run_command_with_output('Run clustering command', command, return_process_output=True)


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


def cluster_multiple(source, asm_vcf_prj_list, assembly_list, private_config_file, private_config_xml_file, profile,
                     output_directory, clustering_artifact, only_printing):
    """
    This method decides how to call the run_clustering method depending on the source (Mongo or VCF)
    """
    preliminary_check(source, asm_vcf_prj_list, assembly_list)

    if source.upper() == 'MONGO':
        cluster_multiple_from_mongo(source, assembly_list, private_config_file, private_config_xml_file, profile,
                                    output_directory, clustering_artifact, only_printing)

    if source.upper() == 'VCF':
        cluster_multiple_from_vcf(source, asm_vcf_prj_list, private_config_file, private_config_xml_file, profile,
                                  output_directory, clustering_artifact, only_printing)


def preliminary_check(source, asm_vcf_prj_list, assembly_list):
    """
    This checks must pass in order to run the script
    """
    check_valid_sources(source)
    check_requirements(source, asm_vcf_prj_list, assembly_list)


def cluster_multiple_from_mongo(source, assembly_list, private_config_file, private_config_xml_file, profile,
                                output_directory, clustering_artifact, only_printing):
    """
    This method splits the list of assemblies and call the run_clustering method for each assembly
    """
    for assembly in assembly_list.split(','):
        run_clustering(source, None, None, assembly, private_config_file, private_config_xml_file, profile,
                       output_directory, clustering_artifact, only_printing)


def cluster_multiple_from_vcf(source, asm_vcf_prj_list, private_config_file, private_config_xml_file, profile,
                              output_directory, clustering_artifact, only_printing):
    """
    The list will be of the form: asm1,GCA000000001.1,PRJEB1111#asm2,GCA000000002.2,PRJEB2222...
    This method splits the triplets and then call the run_clustering method for each one
    """
    for triplet in asm_vcf_prj_list.split(','):
        data = triplet.split('#')
        run_clustering(source, data[1], data[2], data[0], private_config_file, private_config_xml_file, profile,
                       output_directory, clustering_artifact, only_printing)


def check_requirements(source, asm_vcf_prj_list, assembly_list):
    """
    This method checks depending on the source, what list should have been provided.
    For VCF it is expected to have a list of one or more assembly, vcf file, project separated by #
    For Mongo it is expected to have a list of assemblies.
    """
    if source.upper() == 'VCF' and not asm_vcf_prj_list:
        logger.error('If the source is VCF a list of assembly#vcf#project mus be provided using the parameter'
                     '--asm-vcf-prj-list')
        sys.exit(1)
    if source.upper() == 'MONGO' and not assembly_list:
        logger.error('If the source is MONGO a list of assembly accessions must be provided using the parameter '
                     '--assembly-list')
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cluster multiple assemblies', add_help=False)
    parser.add_argument("--source", help="mongo database or VCF", required=True, choices=['VCF', 'MONGO'])
    parser.add_argument("--asm-vcf-prj-list", help="List of Assembly, VCF, project to be clustered, "
                                                   "e.g. GCA_000233375.4#/nfs/eva/accessioned.vcf.gz#PRJEB1111, "
                                                   "GCA_000002285.2#/nfs/eva/file.vcf.gz#PRJEB2222. "
                                                   "Required when the source is VCF",
                        required=False)
    parser.add_argument("--assembly-list",
                        help="Assembly list for which the process has to be run, e.g. GCA_000002285.2,GCA_000233375.4. "
                             "Required when the source is mongo", required=False)
    parser.add_argument("--private-config-file",
                        help="Path to the configuration file with private info (JSON/YML format)", required=False)
    parser.add_argument("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=False)
    parser.add_argument("--profile", help="Profile to get the properties, e.g.production", required=True)
    parser.add_argument("--output-directory", help="Output directory for the properties file", required=False)
    parser.add_argument("--clustering-artifact", help="Artifact of the clustering pipeline",
                        required=False)
    parser.add_argument("--only-printing", help="Prepare and write the commands, but don't run them",
                        action='store_true', required=False)
    parser.add_argument('--help', action='help', help='Show this help message and exit')

    args = {}
    try:
        args = parser.parse_args()
        cluster_multiple(args.source, args.asm_vcf_prj_list, args.assembly_list, args.private_config_file,
                         args.private_config_xml_file, args.profile, args.output_directory, args.clustering_artifact,
                         args.only_printing)
    except Exception as ex:
        logger.exception(ex)
        sys.exit(1)

    sys.exit(0)

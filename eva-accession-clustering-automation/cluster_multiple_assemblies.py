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
from ebi_eva_common_pyutils.command_utils import run_command_with_output

logger = logging.getLogger(__name__)
timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")


def generate_bsub_command(assembly_accession, properties_path, clustering_artifact, memory):
    job_name = 'cluster_{assembly_accession}'.format(assembly_accession=assembly_accession)
    log_file = '{assembly_accession}_cluster_{timestamp}.log'.format(assembly_accession=assembly_accession,
                                                                     timestamp=timestamp)
    error_file = '{assembly_accession}_cluster_{timestamp}.err'.format(assembly_accession=assembly_accession,
                                                                       timestamp=timestamp)
    memory_amount = 8192
    if memory:
        memory_amount = memory

    command = 'bsub -J {job_name} -o {log_file} -e {error_file} -M {memory_amount} -R "rusage[mem={memory_amount}]" ' \
              'java -jar {clustering_artifact} -Dspring.config.location= {properties_path}'\
        .format(job_name=job_name, log_file=log_file, error_file=error_file, memory_amount=memory_amount,
                clustering_artifact=clustering_artifact, properties_path=properties_path)

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


def run_clustering(source, vcf_file, project_accession, assembly_accession, github_token, private_config_xml_file,
                   profile, output_directory, clustering_artifact, only_printing, memory):
    properties_path = create_properties_file(source, vcf_file, project_accession, assembly_accession, github_token,
                                             private_config_xml_file, profile, output_directory)
    command = generate_bsub_command(assembly_accession, properties_path, clustering_artifact, memory)
    if not only_printing:
        run_command_with_output('Run clustering command', command, return_process_output=True)


def cluster_multiple(source, asm_vcf_prj_list, assembly_list, github_token, private_config_xml_file, profile,
                     output_directory, clustering_artifact, only_printing, memory):
    """
    This method decides how to call the run_clustering method depending on the source (Mongo or VCF)
    """
    check_requirements(source, asm_vcf_prj_list, assembly_list)

    if source == 'MONGO':
        cluster_multiple_from_mongo(source, assembly_list, github_token, private_config_xml_file, profile,
                                    output_directory, clustering_artifact, only_printing, memory)

    if source == 'VCF':
        cluster_multiple_from_vcf(source, asm_vcf_prj_list, github_token, private_config_xml_file, profile,
                                  output_directory, clustering_artifact, only_printing, memory)


def cluster_multiple_from_mongo(source, assembly_list, github_token, private_config_xml_file, profile,
                                output_directory, clustering_artifact, only_printing, memory):
    """
    This method call the run_clustering method for each assembly
    """
    for assembly in assembly_list:
        run_clustering(source, None, None, assembly, github_token, private_config_xml_file, profile,
                       output_directory, clustering_artifact, only_printing, memory)


def cluster_multiple_from_vcf(source, asm_vcf_prj_list, github_token, private_config_xml_file, profile,
                              output_directory, clustering_artifact, only_printing, memory):
    """
    The list will be of the form: GCA_000000001.1#/file1.vcf.gz#PRJEB1111 GCA_000000002.2#/file2.vcf.gz#PRJEB2222 ...
    This method splits the triplets and then call the run_clustering method for each one
    """
    for triplet in asm_vcf_prj_list:
        data = triplet.split('#')
        assembly_accession = data[0]
        vcf_file = data[1]
        project_accession = data[2]
        run_clustering(source, vcf_file, project_accession, assembly_accession, github_token, private_config_xml_file,
                       profile, output_directory, clustering_artifact, only_printing, memory)


def check_requirements(source, asm_vcf_prj_list, assembly_list):
    """
    This method checks depending on the source, what list should have been provided.
    For VCF it is expected to have a list of one or more assembly, vcf file, project separated by #
    For Mongo it is expected to have a list of assemblies.
    """
    if source == 'VCF' and not asm_vcf_prj_list:
        raise ValueError('If the source is VCF a list of assembly#vcf#project mus be provided using the parameter '
                         '--asm-vcf-prj-list')
    if source == 'MONGO' and not assembly_list:
        raise ValueError('If the source is MONGO a list of assembly accessions must be provided using the parameter '
                         '--assembly-list')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cluster multiple assemblies', add_help=False)
    parser.add_argument("--source", help="mongo database or VCF", required=True, choices=['VCF', 'MONGO'])
    parser.add_argument("--asm-vcf-prj-list", help="List of Assembly, VCF, project to be clustered, "
                                                   "e.g. GCA_000233375.4#/nfs/eva/accessioned.vcf.gz#PRJEB1111 "
                                                   "GCA_000002285.2#/nfs/eva/file.vcf.gz#PRJEB2222. "
                                                   "Required when the source is VCF", required=False, nargs='+')
    parser.add_argument("--assembly-list", help="Assembly list for which the process has to be run, "
                                                "e.g. GCA_000002285.2 GCA_000233375.4. "
                                                "Required when the source is mongo", required=False, nargs='+')
    parser.add_argument("--github-token", help="Github token to download the eva settings file", required=False)
    parser.add_argument("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=False)
    parser.add_argument("--profile", help="Profile to get the properties, e.g.production", required=True)
    parser.add_argument("--output-directory", help="Output directory for the properties file", required=False)
    parser.add_argument("--clustering-artifact", help="Artifact of the clustering pipeline", required=True)
    parser.add_argument("--only-printing", help="Prepare and write the commands, but don't run them",
                        action='store_true', required=False)
    parser.add_argument("--memory", help="Amount of memory jobs will use", required=False)
    parser.add_argument('--help', action='help', help='Show this help message and exit')

    args = {}
    try:
        args = parser.parse_args()
        cluster_multiple(args.source, args.asm_vcf_prj_list, args.assembly_list, args.github_token,
                         args.private_config_xml_file, args.profile, args.output_directory, args.clustering_artifact,
                         args.only_printing, args.memory)
    except Exception as ex:
        logger.exception(ex)
        sys.exit(1)

    sys.exit(0)

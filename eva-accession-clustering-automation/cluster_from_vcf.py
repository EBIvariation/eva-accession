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
from ebi_eva_common_pyutils.command_utils import run_command_with_output

from create_clustering_properties import create_properties_file


logger = logging.getLogger(__name__)
timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")


def generate_bsub_command(assembly_accession, properties_path, logs_directory, clustering_artifact, memory, dependency):
    job_name = get_job_name(assembly_accession)
    log_file = '{assembly_accession}_cluster_{timestamp}.log'.format(assembly_accession=assembly_accession,
                                                                     timestamp=timestamp)
    error_file = '{assembly_accession}_cluster_{timestamp}.err'.format(assembly_accession=assembly_accession,
                                                                       timestamp=timestamp)
    if logs_directory:
        log_file = os.path.join(logs_directory, log_file)
        error_file = os.path.join(logs_directory, error_file)

    memory_amount = 8192
    if memory:
        memory_amount = memory

    dependency_param = ''
    if dependency:
        dependency_param = '-w {dependency} '.format(dependency=dependency)

    command = 'bsub {dependency_param}-J {job_name} -o {log_file} -e {error_file} -M {memory_amount} ' \
              '-R "rusage[mem={memory_amount}]" java -jar {clustering_artifact} ' \
              '--spring.config.location=file:{properties_path}'\
        .format(dependency_param=dependency_param, job_name=job_name, log_file=log_file, error_file=error_file,
                memory_amount=memory_amount, clustering_artifact=clustering_artifact, properties_path=properties_path)

    print(command)
    add_to_command_file(properties_path, command)
    return command


def get_job_name(assembly_accession):
    return '{timestamp}_cluster_{assembly_accession}'.format(assembly_accession=assembly_accession, timestamp=timestamp)


def add_to_command_file(properties_path, command):
    """
    This method writes the commands to a text file in the output folder
    """
    commands_path = os.path.dirname(properties_path) + '/commands_' + timestamp + '.txt'
    with open(commands_path, 'a+') as commands:
        commands.write(command + '\n')


def cluster_one(source, vcf_file, project_accession, assembly_accession, private_config_xml_file, profile,
                output_directory, logs_directory, clustering_artifact, only_printing, memory, instance, dependency):
    properties_path = create_properties_file(source, vcf_file, project_accession, assembly_accession,
                                             private_config_xml_file, profile, output_directory, instance)
    command = generate_bsub_command(assembly_accession, properties_path, logs_directory, clustering_artifact, memory,
                                    dependency)
    if not only_printing:
        run_command_with_output('Run clustering command', command, return_process_output=True)


def cluster_multiple_from_vcf(asm_vcf_prj_list, private_config_xml_file, profile,
                              output_directory, logs_directory, clustering_artifact, only_printing, memory, instance):
    """
    The list will be of the form: GCA_000000001.1#/file1.vcf.gz#PRJEB1111 GCA_000000002.2#/file2.vcf.gz#PRJEB2222 ...
    This method splits the triplets and then call the run_clustering method for each one
    """
    dependency = None
    for triplet in asm_vcf_prj_list:
        data = triplet.split('#')
        assembly_accession = data[0]
        vcf_file = data[1]
        project_accession = data[2]
        cluster_one(vcf_file, project_accession, assembly_accession, private_config_xml_file, profile, output_directory, logs_directory, clustering_artifact, only_printing, memory, instance, dependency)
        dependency = get_job_name(assembly_accession)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cluster multiple assemblies', add_help=False)
    parser.add_argument("--asm-vcf-prj-list", help="List of Assembly, VCF, project to be clustered, "
                                                   "e.g. GCA_000233375.4#/nfs/eva/accessioned.vcf.gz#PRJEB1111 "
                                                   "GCA_000002285.2#/nfs/eva/file.vcf.gz#PRJEB2222. "
                                                   "Required when the source is VCF", required=True, nargs='+')
    parser.add_argument("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
    parser.add_argument("--profile", help="Profile to get the properties, e.g.production", required=True)
    parser.add_argument("--output-directory", help="Output directory for the properties file", required=False)
    parser.add_argument("--logs-directory", help="Directory for logs files", required=False)
    parser.add_argument("--clustering-artifact", help="Artifact of the clustering pipeline", required=True)
    parser.add_argument("--only-printing", help="Prepare and write the commands, but don't run them",
                        action='store_true', required=False)
    parser.add_argument("--memory", help="Amount of memory jobs will use", required=False, default=8192)
    parser.add_argument("--instance", help="Accessioning instance id", required=False, default=1, choices=range(1, 13))
    parser.add_argument('--help', action='help', help='Show this help message and exit')

    args = {}
    try:
        args = parser.parse_args()
        cluster_multiple_from_vcf(args.asm_vcf_prj_list, args.private_config_xml_file,
                         args.profile, args.output_directory, args.logs_directory, args.clustering_artifact,
                         args.only_printing, args.memory, args.instance)
    except Exception as ex:
        logger.exception(ex)
        sys.exit(1)

    sys.exit(0)

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
import sys
import logging
import datetime
from cluster_one_assembly import run_clustering
from create_clustering_properties import check_valid_sources

logger = logging.getLogger(__name__)
timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")


def cluster_multiple(source, asm_vcf_prj_list, assembly_list, private_config_file, output_directory,
                     clustering_artifact, only_printing):
    """
    This method decides how to call the run_clustering method depending on the source (Mongo or VCF)
    """
    preliminary_check(source, asm_vcf_prj_list, assembly_list)

    if source.upper() == 'VCF':
        cluster_multiple_from_vcf(source, asm_vcf_prj_list, private_config_file, output_directory, clustering_artifact,
                                  only_printing)

    if source.upper() == 'MONGO':
        cluster_multiple_from_mongo(source, assembly_list, private_config_file, output_directory, clustering_artifact,
                                    only_printing)


def preliminary_check(source, asm_vcf_prj_list, assembly_list):
    """
    This checks must pass in order to run the script
    """
    check_valid_sources(source)
    check_requirements(source, asm_vcf_prj_list, assembly_list)


def cluster_multiple_from_mongo(source, assembly_list, private_config_file, output_directory, clustering_artifact,
                                only_printing):
    """
    This method splits the list of assemblies and call the run_clustering method for each assembly
    """
    for assembly in assembly_list.split(','):
        run_clustering(source, None, None, assembly, private_config_file, output_directory, clustering_artifact,
                       only_printing, timestamp)


def cluster_multiple_from_vcf(source, asm_vcf_prj_list, private_config_file, output_directory, clustering_artifact,
                              only_printing):
    """
    The list will be of the form: asm1,GCA000000001.1,PRJEB1111#asm2,GCA000000002.2,PRJEB2222...
    This method splits the triplets and then call the run_clustering method for each one
    """
    for triplet in asm_vcf_prj_list.split(','):
        data = triplet.split('#')
        run_clustering(source, data[1], data[2], data[0], private_config_file, output_directory, clustering_artifact,
                       only_printing, timestamp)


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
        logger.error('If the source is MONGO a list of assembly accessions must be provided using the parameters '
                     '-al or --assembly-list')
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cluster multiple assemblies', add_help=False)
    parser.add_argument("-s", "--source", help="mongo database or VCF", required=True)
    parser.add_argument("--asm-vcf-prj-list", help="List of Assembly, VCF, project to be clustered, "
                                                   "e.g. GCA_000233375.4#/nfs/eva/accessioned.vcf.gz#PRJEB1111, "
                                                   "GCA_000002285.2#/nfs/eva/file.vcf.gz#PRJEB2222. "
                                                   "Required when the source is VCF",
                        required=False)
    parser.add_argument("-al", "--assembly-list",
                        help="Assembly list for which the process has to be run, e.g. GCA_000002285.2,GCA_000233375.4",
                        required=False)
    parser.add_argument("-p", "--private-config-file",
                        help="Path to the configuration file with private info (JSON/YML format)", required=True)
    parser.add_argument("-o", "--output-directory", help="Output directory for the properties file", required=False)
    parser.add_argument("-ca","--clustering-artifact", help="Artifact of the clustering pipeline",
                        required=False)
    parser.add_argument("--only-printing", help="Prepare and write the commands, but don't run them",
                        action='store_true', required=False)
    parser.add_argument('--help', action='help', help='Show this help message and exit')

    args = {}
    try:
        args = parser.parse_args()
        cluster_multiple(args.source, args.asm_vcf_prj_list, args.assembly_list, args.private_config_file,
                         args.output_directory, args.clustering_artifact, args.only_printing)
    except Exception as ex:
        logger.exception(ex)
        sys.exit(1)

    sys.exit(0)

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
import sys
import argparse
import json
import yaml
import logging

logger = logging.getLogger(__name__)


def create_properties_file(source, vcf_file, project_accession, assembly_accession, private_config_file,
                           output_directory):
    """
    This method creates the application properties file
    """
    preliminary_check(source, vcf_file, project_accession)
    private_config_args = get_args_from_private_config_file(private_config_file)
    path = get_properties_path(source, vcf_file, project_accession, assembly_accession, output_directory)
    with open(path, 'w') as properties:
        add_clustering_properties(assembly_accession, project_accession, properties, source, vcf_file)
        add_accessioning_properties(properties)
        add_mongo_properties(private_config_args, properties)
        add_job_tracker_properties(private_config_args, properties)
        add_spring_properties(properties)
    return path


def get_properties_path(source, vcf_file, project_accession, assembly_accession, output_directory):
    path = output_directory + '/' + assembly_accession
    if source.upper() == 'VCF':
        path += '_' + os.path.basename(vcf_file) + '_' + project_accession
    path += '.properties'
    return path


def preliminary_check(source, vcf_file, project_accession):
    """
    This checks must pass in order to run the script
    """
    check_valid_sources(source)
    check_vcf_source_requirements(source, vcf_file, project_accession)


def add_clustering_properties(assembly_accession, project_accession, properties, source, vcf_file):
    properties.write('spring.batch.job.names=' + get_job_name(source) + '\n')
    properties.write('\n')
    properties.write('parameters.assemblyAccession=' + assembly_accession + '\n')
    if vcf_file: properties.write('parameters.vcf=' + vcf_file + '\n')
    if project_accession: properties.write('parameters.projectAccession=' + project_accession + '\n')


def get_job_name(source):
    if source.upper() == 'MONGO':
        return 'CLUSTERING_FROM_MONGO_JOB'
    elif source.upper() == 'VCF':
        return 'CLUSTERING_FROM_VCF_JOB'


def add_accessioning_properties(properties):
    properties.write('\n')
    properties.write('parameters.chunkSize=100' + '\n')
    properties.write('\n')
    properties.write('accessioning.instanceId=instance-01' + '\n')
    properties.write('accessioning.submitted.categoryId=ss' + '\n')
    properties.write('accessioning.clustered.categoryId=rs' + '\n')
    properties.write('\n')
    properties.write('accessioning.monotonic.ss.blockSize=100000' + '\n')
    properties.write('accessioning.monotonic.ss.blockStartValue=5000000000' + '\n')
    properties.write('accessioning.monotonic.ss.nextBlockInterval=1000000000' + '\n')
    properties.write('accessioning.monotonic.rs.blockSize=100000' + '\n')
    properties.write('accessioning.monotonic.rs.blockStartValue=3000000000' + '\n')
    properties.write('accessioning.monotonic.rs.nextBlockInterval=1000000000' + '\n')


def add_spring_properties(properties):
    properties.write('\n')
    properties.write(
        '#See https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-2.1-Release-Notes#bean-overriding' + '\n')
    properties.write('spring.main.allow-bean-definition-overriding=true' + '\n')
    properties.write(
        '#As this is a spring batch application, disable the embedded tomcat. This is the new way to do that for spring 2.' + '\n')
    properties.write('spring.main.web-application-type=none' + '\n')
    properties.write('\n')
    properties.write(
        '# This entry is put just to avoid a warning message in the logs when you start the spring-boot application.' + '\n')
    properties.write(
        '# This bug is from hibernate which tries to retrieve some metadata from postgresql db and failed to find that and logs as a warning' + '\n')
    properties.write('# It doesnt cause any issue though.' + '\n')
    properties.write('spring.jpa.properties.hibernate.jdbc.lob.non_contextual_creation = true' + '\n')


def add_job_tracker_properties(private_config_args, properties):
    properties.write('\n')
    properties.write('spring.datasource.driver-class-name=org.postgresql.Driver' + '\n')
    job_tracker_host = private_config_args['job_tracker_host']
    job_tracker_port = str(private_config_args['job_tracker_port'])
    job_tracker_db = private_config_args['job_tracker_db']
    job_tracker_url = 'postgresql://' + job_tracker_host + ':' + job_tracker_port + '/' + job_tracker_db
    properties.write('spring.datasource.url=jdbc:' + job_tracker_url + '\n')
    job_tracker_user = private_config_args['job_tracker_user']
    properties.write('spring.datasource.username=' + job_tracker_user + '\n')
    job_tracker_password = private_config_args['job_tracker_password']
    properties.write('spring.datasource.password=' + job_tracker_password + '\n')
    properties.write('spring.datasource.tomcat.max-active=3' + '\n')


def add_mongo_properties(private_config_args, properties):
    properties.write('\n')
    mongo_host = private_config_args['mongo_host']
    mongo_port = str(private_config_args['mongo_port'])
    mongo_uri = 'mongodb://' + mongo_host + ':' + mongo_port
    properties.write('spring.data.mongodb.uri=' + mongo_uri + '\n')
    mongo_database = private_config_args['mongo_acc_db']
    properties.write('spring.data.mongodb.database=' + mongo_database + '\n')
    properties.write('mongodb.read-preference=primary' + '\n')


def get_args_from_private_config_file(private_config_file):
    with open(private_config_file) as private_config_file_handle:
        if 'json' in private_config_file:
            return json.load(private_config_file_handle)
        else:
            if 'yml' in private_config_file:
                return yaml.safe_load(private_config_file_handle)
            else:
                raise TypeError('Configuration file should be either json or yaml')


def check_valid_sources(source):
    """
    This method checks that ony MONGO and VCF are used as sources
    """
    if source.upper() not in ('MONGO', 'VCF'):
        logger.error("Wrong source specified. Please choose between MONGO and VCF")
        sys.exit(1)


def check_vcf_source_requirements(source, vcf_file, project_accession):
    """
    This method checks that if the source is VCF the VCF file and project accession are provided
    """
    if source.upper() == 'VCF' and not (vcf_file and project_accession):
        logger.error('If the source is VCF the file path and project accession must be provided')
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create clustering properties file', add_help=False)
    parser.add_argument("-s", "--source", help="mongo database or VCF", required=True)
    parser.add_argument("-vcf", "--vcf-file", help="Path to the VCF file, required when the source is VCF", required=False)
    parser.add_argument("-pj", "--project-accession", help="Project accession, required when the source is VCF",
                        required=False)
    parser.add_argument("-a", "--assembly-accession",
                        help="Assembly for which the process has to be run, e.g. GCA_000002285.2", required=True)
    parser.add_argument("-p", "--private-config-file",
                        help="Path to the configuration file with private info (JSON/YML format)", required=True)
    parser.add_argument("-o", "--output-directory", help="Output directory for the properties file", required=False)
    parser.add_argument('--help', action='help', help='Show this help message and exit')

    args = {}
    try:
        args = parser.parse_args()
        create_properties_file(args.source, args.vcf_file, args.project_accession, args.assembly_accession,
                               args.private_config_file, args.output_directory)
    except Exception as ex:
        logger.exception(ex)
        sys.exit(1)

    sys.exit(0)
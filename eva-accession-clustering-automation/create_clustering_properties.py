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
import logging
from config_custom import get_properties

logger = logging.getLogger(__name__)


def create_properties_file(source, vcf_file, project_accession, assembly_accession, private_config_xml_file, profile,
                           output_directory):
    """
    This method creates the application properties file
    """
    preliminary_check(source, vcf_file, project_accession)
    properties = get_properties(profile, private_config_xml_file)
    path = get_properties_path(source, vcf_file, project_accession, assembly_accession, output_directory)
    with open(path, 'w') as property_line:
        add_clustering_properties(property_line, assembly_accession, project_accession, source, vcf_file)
        add_accessioning_properties(property_line)
        add_mongo_properties(property_line, properties)
        add_job_tracker_properties(property_line, properties)
        add_spring_properties(property_line)
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


def add_clustering_properties(property_line, assembly_accession, project_accession, source, vcf_file):
    property_line.write('spring.batch.job.names=' + get_job_name(source) + '\n')
    property_line.write('\n')
    property_line.write('parameters.assemblyAccession=' + assembly_accession + '\n')
    if vcf_file: property_line.write('parameters.vcf=' + vcf_file + '\n')
    if project_accession: property_line.write('parameters.projectAccession=' + project_accession + '\n')


def get_job_name(source):
    if source.upper() == 'MONGO':
        return 'CLUSTERING_FROM_MONGO_JOB'
    elif source.upper() == 'VCF':
        return 'CLUSTERING_FROM_VCF_JOB'


def add_accessioning_properties(property_line):
    property_line.write('\n')
    property_line.write('parameters.chunkSize=100' + '\n')
    property_line.write('\n')
    property_line.write('accessioning.instanceId=instance-01' + '\n')
    property_line.write('accessioning.submitted.categoryId=ss' + '\n')
    property_line.write('accessioning.clustered.categoryId=rs' + '\n')
    property_line.write('\n')
    property_line.write('accessioning.monotonic.ss.blockSize=100000' + '\n')
    property_line.write('accessioning.monotonic.ss.blockStartValue=5000000000' + '\n')
    property_line.write('accessioning.monotonic.ss.nextBlockInterval=1000000000' + '\n')
    property_line.write('accessioning.monotonic.rs.blockSize=100000' + '\n')
    property_line.write('accessioning.monotonic.rs.blockStartValue=3000000000' + '\n')
    property_line.write('accessioning.monotonic.rs.nextBlockInterval=1000000000' + '\n')


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


def add_mongo_properties(property_line, properties):
    property_line.write('\n')
    mongo_hosts_and_ports = properties['eva.mongo.host']
    mongo_uri = 'mongodb://' + mongo_hosts_and_ports
    property_line.write('spring.data.mongodb.uri=' + mongo_uri + '\n')
    property_line.write('spring.data.mongodb.database=' + properties['eva.accession.mongo.database'] + '\n')
    property_line.write('spring.data.mongodb.username=' + properties['eva.mongo.user'] + '\n')
    property_line.write('spring.data.mongodb.password=' + properties['eva.mongo.passwd'] + '\n')
    property_line.write('mongodb.read-preference=primary' + '\n')


def add_job_tracker_properties(property_line, properties):
    property_line.write('\n')
    property_line.write('spring.datasource.driver-class-name=org.postgresql.Driver' + '\n')
    property_line.write('spring.datasource.url=' + properties['eva.accession.jdbc.url'] + '\n')
    property_line.write('spring.datasource.username=' + properties['eva.accession.user'] + '\n')
    property_line.write('spring.datasource.password=' + properties['eva.accession.password'] + '\n')
    property_line.write('spring.datasource.tomcat.max-active=3' + '\n')


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
    parser.add_argument("--source", help="mongo database or VCF", required=True)
    parser.add_argument("--vcf-file", help="Path to the VCF file, required when the source is VCF", required=False)
    parser.add_argument("--project-accession", help="Project accession, required when the source is VCF",
                        required=False)
    parser.add_argument("--assembly-accession",
                        help="Assembly for which the process has to be run, e.g. GCA_000002285.2", required=True)
    parser.add_argument("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
    parser.add_argument("--profile", help="Profile to get the properties, e.g.production", required=True)
    parser.add_argument("--output-directory", help="Output directory for the properties file", required=False)
    parser.add_argument('--help', action='help', help='Show this help message and exit')

    args = {}
    try:
        args = parser.parse_args()
        create_properties_file(args.source, args.vcf_file, args.project_accession, args.assembly_accession,
                               args.private_config_xml_file, args.profile, args.output_directory)
    except Exception as ex:
        logger.exception(ex)
        sys.exit(1)

    sys.exit(0)
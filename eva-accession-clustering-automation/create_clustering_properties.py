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
from ebi_eva_common_pyutils.config_utils import get_properties_from_xml_file

logger = logging.getLogger(__name__)


def create_properties_file(source, vcf_file, project_accession, assembly_accession, private_config_xml_file, profile,
                           output_directory):
    """
    This method creates the application properties file
    """
    check_vcf_source_requirements(source, vcf_file, project_accession)
    properties = get_properties_from_xml_file(profile, private_config_xml_file)
    path = get_properties_path(source, vcf_file, project_accession, assembly_accession, output_directory)
    with open(path, 'w') as properties_file:
        add_clustering_properties(properties_file, assembly_accession, project_accession, source, vcf_file)
        add_accessioning_properties(properties_file)
        add_mongo_properties(properties_file, properties)
        add_job_tracker_properties(properties_file, properties)
        add_spring_properties(properties_file)
    return path


def get_properties_path(source, vcf_file, project_accession, assembly_accession, output_directory):
    path = output_directory + '/' + assembly_accession
    if source.upper() == 'VCF':
        path += '_' + os.path.basename(vcf_file) + '_' + project_accession
    path += '.properties'
    return path


def add_clustering_properties(properties_file, assembly_accession, project_accession, source, vcf_file):
    job_name = get_job_name(source)

    clustering_properties = ("""
spring.batch.job.names={job_name}

parameters.assemblyAccession={assembly_accession}    
    """).format(job_name=job_name, assembly_accession=assembly_accession)
    properties_file.write(clustering_properties)

    if vcf_file:
        properties_file.write('parameters.vcf=' + vcf_file + '\n')
    if project_accession:
        properties_file.write('parameters.projectAccession=' + project_accession + '\n')


def get_job_name(source):
    if source.upper() == 'MONGO':
        return 'CLUSTERING_FROM_MONGO_JOB'
    elif source.upper() == 'VCF':
        return 'CLUSTERING_FROM_VCF_JOB'


def add_accessioning_properties(properties_file):
    properties_file.write("""
parameters.chunkSize=100

accessioning.instanceId=instance-01
accessioning.submitted.categoryId=ss
accessioning.clustered.categoryId=rs

accessioning.monotonic.ss.blockSize=100000
accessioning.monotonic.ss.blockStartValue=5000000000 
accessioning.monotonic.ss.nextBlockInterval=1000000000
accessioning.monotonic.rs.blockSize=100000'
accessioning.monotonic.rs.blockStartValue=3000000000 
accessioning.monotonic.rs.nextBlockInterval=1000000000
    """)


def add_spring_properties(properties_file):
    properties_file.write("""
#See https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-2.1-Release-Notes#bean-overriding
spring.main.allow-bean-definition-overriding=true
#As this is a spring batch application, disable the embedded tomcat. This is the new way to do that for spring 2.
spring.main.web-application-type=none

# This entry is put just to avoid a warning message in the logs when you start the spring-boot application.
# This bug is from hibernate which tries to retrieve some metadata from postgresql db and failed to find that and logs as a warning
# It doesnt cause any issue though.
spring.jpa.properties.hibernate.jdbc.lob.non_contextual_creation = true""")


def add_mongo_properties(properties_file, properties):
    mongo_hosts_and_ports = str(properties['eva.mongo.host'])
    mongo_uri = 'mongodb://' + mongo_hosts_and_ports
    mongo_database = str(properties['eva.accession.mongo.database'])
    mongo_username = str(properties['eva.mongo.user'])
    mongo_password = str(properties['eva.mongo.passwd'])

    mongo_properties = ("""
spring.data.mongodb.uri={uri}
spring.data.mongodb.database={database}
spring.data.mongodb.username={username}
spring.data.mongodb.password={password}
mongodb.read-preference=primary
    """).format(uri=mongo_uri, database=mongo_database, username=mongo_username, password=mongo_password)
    properties_file.write(mongo_properties)


def add_job_tracker_properties(properties_file, properties):
    postgres_url = str(properties['eva.accession.jdbc.url'])
    postgres_username = str(properties['eva.accession.user'])
    postgres_password = str(properties['eva.accession.password'])

    postgres_properties = ("""
spring.datasource.driver-class-name=org.postgresql.Driver
spring.datasource.url={postgres_url}
spring.datasource.username={postgres_username}
spring.datasource.password={postgres_password}
spring.datasource.tomcat.max-active=3    
    """).format(postgres_url=postgres_url, postgres_username=postgres_username, postgres_password=postgres_password)
    properties_file.write(postgres_properties)


def check_vcf_source_requirements(source, vcf_file, project_accession):
    """
    This method checks that if the source is VCF the VCF file and project accession are provided
    """
    if source == 'VCF' and not (vcf_file and project_accession):
        raise ValueError('If the source is VCF the file path and project accession must be provided')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create clustering properties file', add_help=False)
    parser.add_argument("--source", help="mongo database or VCF", required=True, choices=['VCF', 'MONGO'])
    parser.add_argument("--vcf-file", help="Path to the VCF file, required when the source is VCF", required=False)
    parser.add_argument("--project-accession", help="Project accession, required when the source is VCF",
                        required=False)
    parser.add_argument("--assembly-accession", help="Assembly for which the process has to be run, "
                                                     "e.g. GCA_000002285.2", required=True)
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

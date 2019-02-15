# Copyright 2018 EMBL - European Bioinformatics Institute
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

import logging
import sys
import argparse
import data_ops
import pgpasslib
from os.path import expanduser


def main(args):
    complete_args = query_missing_parameters(args)
    fill_and_write_template(complete_args)


def query_missing_parameters(args):
    species_db_info = \
        list(filter(lambda db_info: db_info["database_name"] == args.species,
               data_ops.get_species_pg_conn_info(args.metadb,
                                                 args.metauser,
                                                 args.metahost)
               ))[0]

    complete_args = args

    if args.assembly_name is None:
        complete_args.assembly_name = data_ops.get_assembly_name(species_db_info, args.build)

    if args.latest_build:
        complete_args.optional_build_line = ""
    else:
        complete_args.optional_build_line = "parameters.buildNumber={}".format(args.build)

    complete_args.taxonomy = args.species.split('_')[-1]
    complete_args.dbsnp_host = species_db_info["pg_host"]
    complete_args.dbsnp_port = species_db_info["pg_port"]
    complete_args.dbsnp_build = species_db_info["dbsnp_build"]
    complete_args.database_name = args.species
    complete_args.dbsnp_password = pgpasslib.getpass(host=complete_args.dbsnp_host,
                                                     port=complete_args.dbsnp_port,
                                                     dbname="*",
                                                     user=complete_args.dbsnp_user)
    complete_args.job_tracker_password = pgpasslib.getpass(host=complete_args.job_tracker_host,
                                                           port=complete_args.job_tracker_port,
                                                           dbname=complete_args.job_tracker_db,
                                                           user=complete_args.job_tracker_user)
    return complete_args


def fill_and_write_template(args):
    template = \
"""spring.batch.job.names=IMPORT_DBSNP_VARIANTS_JOB

accessioning.instanceId=instance-import
accessioning.variant.categoryId=ss
accessioning.monotonic.ss.nextBlockInterval=10000
accessioning.monotonic.ss.blockStartValue=10000
accessioning.monotonic.ss.blockSize=1000

parameters.assemblyAccession={args.assembly_accession}
parameters.assemblyName={args.assembly_name}
parameters.assemblyReportUrl=file:{args.assembly_report}
parameters.taxonomyAccession={args.taxonomy}
parameters.chunkSize=1000
parameters.pageSize=100
#parameters.forceRestart=true
#parameters.forceImport=false
parameters.fasta={args.fasta}
{args.optional_build_line}

dbsnp.datasource.driver-class-name=org.postgresql.Driver
dbsnp.datasource.url=jdbc:postgresql://{args.dbsnp_host}:{args.dbsnp_port}/dbsnp_{args.dbsnp_build}?currentSchema=dbsnp_{args.database_name}
dbsnp.datasource.username={args.dbsnp_user}
dbsnp.datasource.password={args.dbsnp_password}
dbsnp.datasource.tomcat.max-active=3

spring.datasource.driver-class-name=org.postgresql.Driver
spring.datasource.url=jdbc:postgresql://{args.job_tracker_host}:{args.job_tracker_port}/{args.job_tracker_db}
spring.datasource.username={args.job_tracker_user}
spring.datasource.password={args.job_tracker_password}
spring.datasource.tomcat.max-active=3
spring.jpa.generate-ddl=true

spring.data.mongodb.host={args.mongo_host}
spring.data.mongodb.port={args.mongo_port}
spring.data.mongodb.database={args.mongo_acc_db}
spring.data.mongodb.username={args.mongo_user}
spring.data.mongodb.password={args.mongo_password}
spring.data.mongodb.authentication-database={args.mongo_auth_db}
mongodb.read-preference=primaryPreferred

spring.main.web-environment=false

logging.level.uk.ac.ebi.eva.accession.dbsnp=DEBUG
logging.level.uk.ac.ebi.eva.accession.dbsnp.listeners=INFO
logging.level.org.springframework.jdbc.datasource=DEBUG
"""
    properties_filename = "{}_b{}.properties".format(args.assembly_accession, args.build)
    with open(properties_filename, "w") as properties_file:
        properties_file.write(template.format(args=args))


def init_logger():
    logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(levelname)s %(message)s')
    result_logger = logging.getLogger(__name__)
    return result_logger


if __name__ == "__main__":
    logger = init_logger()

    parser = argparse.ArgumentParser(
        description='Generate a properties file that can be used for eva-accession-import',
        add_help=False)
    parser.add_argument("-b", "--build", help="dbSNP build number, e.g. 151", required=True)
    parser.add_argument("-l", "--latest-build",
                        help="Flag that this build is the latest (relevant for dbsnp table name)",
                        action='store_true')
    parser.add_argument("-n", "--assembly-name",
                        help="Assembly name for which the process has to be run, e.g. Gallus_gallus-5.0"
                             ". (Can be ommited if there is only one assembly name in the build)")
    parser.add_argument("-s", "--species",
                        help="Species for which the process has to be run, e.g. chicken_9031",
                        required=True)
    parser.add_argument("-a", "--assembly-accession",
                        help="Assembly for which the process has to be run, e.g. GCA_000002315.3",
                        required=True)
    parser.add_argument("-r", "--assembly-report", help="File with contig synonyms", required=True)
    parser.add_argument("-f", "--fasta", help="FASTA file with the reference sequence",
                        required=True)

    parser.add_argument("-d", "--metadb", help="Postgres metadata DB", required=True)
    parser.add_argument("-u", "--metauser", help="Postgres metadata DB username", required=True)
    parser.add_argument("-h", "--metahost", help="Postgres metadata DB host", required=True)

    parser.add_argument("-D", "--job-tracker-db", help="Postgres DB for the job repository",
                        required=True)
    parser.add_argument("-H", "--job-tracker-host", help="Postgres host for the job repository",
                        required=True)
    parser.add_argument("-P", "--job-tracker-port", help="Postgres port for the job repository",
                        required=True, type=int, default=5432)
    parser.add_argument("-U", "--job-tracker-user", help="Postgres user for the job repository",
                        required=True)

    parser.add_argument("--dbsnp-user", help="Postgres user for the dbSNP mirror",
                        required=True)
    parser.add_argument("--dbsnp-port", help="Postgres port for the dbSNP mirror",
                        required=True, type=int)

    parser.add_argument("--mongo-acc-db", help="MongoDB accessioning database", required=True)
    parser.add_argument("--mongo-auth-db", help="MongoDB authentication database for accessioning",
                        required=True)
    parser.add_argument("--mongo-user", help="MongoDB user", required=True)
    parser.add_argument("--mongo-password", help="MongoDB password", required=True)
    parser.add_argument("--mongo-host", help="MongoDB host", required=True)
    parser.add_argument("--mongo-port", help="MongoDB port", required=True)
    parser.add_argument('--help', action='help', help='Show this help message and exit')

    try:
        args = parser.parse_args()
        main(args)
    except Exception as ex:
        logger.exception(ex)
        sys.exit(1)

    sys.exit(0)

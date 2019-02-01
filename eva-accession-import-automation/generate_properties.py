import logging
import sys
import argparse
import data_ops
from os.path import expanduser


def main(args):
    complete_args = query_missing_parameters(args)
    fill_and_write_template(complete_args)


def query_missing_parameters(args):
    species_info = data_ops.get_species_info(args.metadb,
                                             args.metauser,
                                             args.metahost,
                                             args.assembly_accession)

    species_db_info = \
    filter(lambda db_info: db_info["database_name"] == species_info["database_name"],
           data_ops.get_species_pg_conn_info(args.metadb,
                                             args.metauser,
                                             args.metahost)
           )[0]

    dbsnp_user, dbsnp_password, unused_dbsnp_port = \
        get_user_and_password_and_port_from_pgpass_for_host(species_db_info["pg_host"])

    jt_user, jt_password, jt_port = get_user_and_password_and_port_from_pgpass_for_host(
        args.job_tracker_host)

    complete_args = args

    if args.assembly_name == None:
        complete_args.assembly_name = data_ops.get_assembly_name(species_db_info, args.build)

    if args.latest_build:
        complete_args.optional_build_line = ""
    else:
        complete_args.optional_build_line = "parameters.buildNumber={}".format(args.build)

    complete_args.taxonomy = species_info["taxonomy"]
    complete_args.dbsnp_host = species_db_info["pg_host"]
    complete_args.dbsnp_port = species_db_info["pg_port"]
    complete_args.dbsnp_build = species_db_info["dbsnp_build"]
    complete_args.database_name = species_info["database_name"]
    complete_args.dbsnp_user = dbsnp_user
    complete_args.dbsnp_password = dbsnp_password
    complete_args.job_tracker_port = jt_port
    complete_args.job_tracker_user = jt_user
    complete_args.job_tracker_password = jt_password
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
spring.data.mongodb.database={args.mongo_db}
spring.data.mongodb.username={args.mongo_user}
spring.data.mongodb.password={args.mongo_password}
spring.data.mongodb.authentication-database={args.mongo_auth_db}
mongodb.read-preference=primaryPreferred

spring.main.web-environment=false

logging.level.uk.ac.ebi.eva.accession.dbsnp=DEBUG
logging.level.uk.ac.ebi.eva.accession.dbsnp.listeners=INFO
logging.level.org.springframework.jdbc.datasource=DEBUG
"""
    properties_filename = "{}.properties".format(args.assembly_accession)
    with open(properties_filename, "w") as properties_file:
        properties_file.write(template.format(args=args))


def get_user_and_password_and_port_from_pgpass_for_host(pg_host):
    with open(expanduser("~/.pgpass")) as pgpass_file:
        for line in pgpass_file:
            if line.split(':', 1)[0] == pg_host:
                fields = line.rstrip('\n').split(':')
                return fields[3], fields[4], fields[1]


def init_logger():
    logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(levelname)s %(message)s')
    result_logger = logging.getLogger(__name__)
    return result_logger


if __name__ == "__main__":
    logger = init_logger()

    parser = argparse.ArgumentParser(
        description='Generate a properties file that can be used for eva-accession-import',
        add_help=False)
    parser.add_argument("-b", "--build", help="dbSNP build number", required=True)
    parser.add_argument("-l", "--latest-build",
                        help="Flag that this build is the latest (relevant for dbsnp table name)",
                        action='store_true')
    parser.add_argument("-n", "--assembly-name",
                        help="Assembly name for which the process has to be run, e.g. Gallus_gallus-5.0")
    parser.add_argument("-a", "--assembly-accession",
                        help="Assembly for which the process has to be run, e.g. GCA_000002315.3",
                        required=True)
    parser.add_argument("-r", "--assembly-report",
                        help="File with GenBank equivalents for RefSeq accessions", required=True)
    parser.add_argument("-f", "--fasta", help="FASTA file with the reference sequence",
                        required=True)

    parser.add_argument("-d", "--metadb", help="Postgres metadata DB", required=True)
    parser.add_argument("-u", "--metauser", help="Postgres metadata DB username", required=True)
    parser.add_argument("-h", "--metahost", help="Postgres metadata DB host", required=True)

    parser.add_argument("-D", "--job-tracker-db", help="Postgres DB for the job repository",
                        required=True)
    parser.add_argument("-H", "--job-tracker-host", help="Postgres host for the job repository",
                        required=True)

    parser.add_argument("--mongo-db", help="MongoDB accessioning database", required=True)
    parser.add_argument("--mongo-auth-db", help="MongoDB accessioning authentication database",
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

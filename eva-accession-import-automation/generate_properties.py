import logging
import sys
import argparse
import data_ops
from os.path import expanduser


def main(parameters, evapro_credentials, job_tracker_credentials, mongo_credentials):
    template = """
spring.batch.job.names=IMPORT_DBSNP_VARIANTS_JOB

accessioning.instanceId=instance-import
accessioning.variant.categoryId=ss
accessioning.monotonic.ss.nextBlockInterval=10000
accessioning.monotonic.ss.blockStartValue=10000
accessioning.monotonic.ss.blockSize=1000

parameters.assemblyAccession={assembly_accession}
parameters.assemblyName={assembly_name}
parameters.assemblyReportUrl=file:{assembly_report}
parameters.taxonomyAccession={taxonomy}
parameters.chunkSize=1000
parameters.pageSize=100
#parameters.forceRestart=true
parameters.fasta={fasta}
{optional_build_line}

spring.batch.job.names=CREATE_SUBSNP_ACCESSION_JOB

dbsnp.datasource.driver-class-name=org.postgresql.Driver
dbsnp.datasource.url=jdbc:postgresql://{dbsnp_host}:{dbsnp_port}/dbsnp_{dbsnp_build}?currentSchema=dbsnp_{dbsnp_species}
dbsnp.datasource.username={dbsnp_user}
dbsnp.datasource.password={dbsnp_password}
dbsnp.datasource.tomcat.max-active=3

spring.datasource.driver-class-name=org.postgresql.Driver
spring.datasource.url=jdbc:postgresql://{jt_host}:{jt_port}/{jt_db}
spring.datasource.username={jt_user}
spring.datasource.password={jt_password}
spring.datasource.tomcat.max-active=3
spring.jpa.generate-ddl=true
 
spring.data.mongodb.host={mongo_host}
spring.data.mongodb.port={mongo_port}
spring.data.mongodb.database={mongo_db}
spring.data.mongodb.username={mongo_user}
spring.data.mongodb.password={mongo_password}
spring.data.mongodb.authentication-database={mongo_auth_db}
mongodb.read-preference=primaryPreferred

spring.main.web-environment=false

logging.level.uk.ac.ebi.eva.accession.dbsnp=DEBUG
logging.level.uk.ac.ebi.eva.accession.dbsnp.listeners=INFO
logging.level.org.springframework.jdbc.datasource=DEBUG
"""
    species_db_info = filter(lambda db_info: db_info["database_name"] == parameters["species"],
                             data_ops.get_species_pg_conn_info(evapro_credentials["metadb"],
                                                               evapro_credentials["metauser"],
                                                               evapro_credentials["metahost"])
                             )[0]
    pg_conn_for_species = data_ops.get_pg_conn_for_species(species_db_info)
    dbsnp_user, dbsnp_password = get_user_and_password_from_pgpass_for_host(
        species_db_info["pg_host"])
    jt_user, jt_password = get_user_and_password_from_pgpass_for_host(
        job_tracker_credentials["job_tracker_host"])
    properties_filename = "{}.properties".format(parameters["assembly_accession"])
    with open(properties_filename, "w") as properties_file:
        properties_file.write(template.format(
            assembly_accession=parameters["assembly_accession"],
            assembly_name=parameters["assembly_name"],
            assembly_report=parameters["assembly_report"],
            taxonomy=parameters["taxonomy"],
            fasta=parameters["fasta"],
            dbsnp_host=species_db_info["pg_host"],
            dbsnp_port=species_db_info["pg_port"],
            dbsnp_build=species_db_info["dbsnp_build"],
            dbsnp_species=parameters["species"],
            optional_build_line="parameters.buildNumber={}".format(parameters["build"]) if parameters["latest_build"] else "",
            dbsnp_user=dbsnp_user,
            dbsnp_password=dbsnp_password,
            jt_host=job_tracker_credentials["job_tracker_host"],
            jt_port=job_tracker_credentials["job_tracker_port"],
            jt_db=job_tracker_credentials["job_tracker_db"],
            jt_user=jt_user,
            jt_password=jt_password,
            mongo_host=mongo_credentials["mongo_host"],
            mongo_port=mongo_credentials["mongo_port"],
            mongo_db=mongo_credentials["mongo_db"],
            mongo_user=mongo_credentials["mongo_user"],
            mongo_password=mongo_credentials["mongo_password"],
            mongo_auth_db=mongo_credentials["mongo_auth_db"]))


def get_user_and_password_from_pgpass_for_host(pg_host):
    with open(expanduser("~/.pgpass")) as pgpass_file:
        for line in pgpass_file:
            if line.split(':', 1)[0] == pg_host:
                fields = line.split(':')
                return fields[3], fields[4]


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
    parser.add_argument("-s", "--species",
                        help="Species for which the process has to be run, e.g. chicken_9031",
                        required=True)
    parser.add_argument("-n", "--assembly-name",
                        help="Assembly name for which the process has to be run, e.g. Gallus_gallus-5.0",
                        required=True)
    parser.add_argument("-a", "--assembly-accession",
                        help="Assembly for which the process has to be run, e.g. GCA_000002315.3",
                        required=True)
    parser.add_argument("-r", "--assembly-report",
                        help="File with GenBank equivalents for RefSeq accessions", required=True)
    parser.add_argument("-t", "--taxonomy", help="Taxomomy of the species, e.g. 9031",
                        required=True)
    parser.add_argument("-f", "--fasta", help="FASTA file with the reference sequence",
                        required=True)
    parser.add_argument("-i", "--instance",
                        help="Accessioning instance id (don't use the same instance concurrently!)",
                        required=True)

    parser.add_argument("-d", "--metadb", help="Postgres metadata DB", required=True)
    parser.add_argument("-u", "--metauser", help="Postgres metadata DB username", required=True)
    parser.add_argument("-h", "--metahost", help="Postgres metadata DB host", required=True)

    parser.add_argument("-D", "--job-tracker-db", help="Postgres DB for the job repository",
                        required=True)
    parser.add_argument("-U", "--job-tracker-user",
                        help="Postgres username for the job repository", required=True)
    parser.add_argument("-H", "--job-tracker-host", help="Postgres host for the job repository",
                        required=True)
    parser.add_argument("-P", "--job-tracker-port", help="Postgres port for the job repository",
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

        parameters = {"assembly_name": args.assembly_name,
                      "assembly_accession": args.assembly_accession,
                      "assembly_report": args.assembly_report,
                      "taxonomy": args.taxonomy,
                      "species": args.species,
                      "build": args.build,
                      "latest_build": args.latest_build,
                      "fasta": args.fasta}

        evapro_credentials = {"metadb": args.metadb,
                              "metauser": args.metauser,
                              "metahost": args.metahost}

        job_tracker_credentials = {"job_tracker_db": args.job_tracker_db,
                                   "job_tracker_user": args.job_tracker_user,
                                   "job_tracker_host": args.job_tracker_host,
                                   "job_tracker_port": args.job_tracker_port}

        mongo_credentials = {"mongo_db": args.mongo_db,
                             "mongo_auth_db": args.mongo_auth_db,
                             "mongo_user": args.mongo_user,
                             "mongo_password": args.mongo_password,
                             "mongo_host": args.mongo_host,
                             "mongo_port": args.mongo_port}

        main(parameters, evapro_credentials, job_tracker_credentials, mongo_credentials)
    except Exception as ex:
        logger.exception(ex)
        sys.exit(1)

    sys.exit(0)

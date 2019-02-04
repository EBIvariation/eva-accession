import os
import argparse
import sys
import json
from __init__ import *


def get_args_from_private_config_file(private_config_file):
    with open(private_config_file) as private_config_file_handle:
        return json.load(private_config_file_handle)


def main(command_line_args):
    program_args = {**command_line_args, **get_args_from_private_config_file(command_line_args.private_config_file)}

    program_args["program_dir"] = os.path.dirname(os.path.realpath(__file__))
    program_args["species_assembly_folder"] = os.path.sep.join(["{eva_root_dir}", "datasources", "reference_sequences",
                                                                "{scientific_name}"]).format(**program_args)
    create_species_assembly_folder_command = "mkdir -p {species_assembly_folder}".format(**program_args)
    generate_custom_assembly_report_command = "cd {species_assembly_folder} && " \
                                              "{python3_path} {program_dir}/generate_custom_assembly_report.py " \
                                                "-d {metadadb} -u {metauser} -h {metahost} " \
                                                "-s {species} -a {assembly_accession} " \
                                                "-g {genbank_equivalents_file}".format(**program_args)
    create_fasta_file_command = "create_fasta_from_assembly_report.sh {assembly_accession} {assembly_report} " \
                                "{species_assembly_folder}".format(**program_args)
    generate_import_job_properties_file_command = "{python3_path} {program_dir}/generate_properties.py " \
                                                  "-b {build} " + ("-l" if program_args["latest_build"] else "") + \
                                                  "-n {assembly_name} -a {assembly_accession} -r {assembly_report} " \
                                                  "-f {fasta_file_path} " \
                                                  "-d {metadb} -u {metauser} -h {metahost} " \
                                                  "-D {job_tracker_db} -H {job_tracker_host} " \
                                                  "--mongo-acc-db {mongo_acc_db} --mongo-auth-db {mongo_auth_db} " \
                                                  "--mongo-user {mongo_user} --mongo-password {mongo_password} " \
                                                  "--mongo-host {mongo_host} --mongo-port {mongo_port}"\
                                                  .format(**program_args)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Run the accession import process for a given species, assembly and build', add_help=False)
    parser.add_argument("-s", "--species", help="Species for which the process has to be run", required=True)
    parser.add_argument("--scientific-name", help="Scientific name for the species", required=True)
    parser.add_argument("-a", "--assembly-accession", help="Assembly for which the process has to be run",
                        required=True)
    parser.add_argument("-b", "--build", help="dbSNP build number, e.g. 151", required=True)
    parser.add_argument("-l", "--latest-build",
                        help="Flag that this build is the latest (relevant for dbsnp table name)",
                        action='store_true')
    parser.add_argument("-n", "--assembly-name",
                        help="Assembly name for which the process has to be run, e.g. Gallus_gallus-5.0"
                             ". (Can be ommited if there is only one assembly name in the build)")
    parser.add_argument("-p", "--private-config-file",
                        help="Path to the configuration file with private connection details, credentials etc.,")
    parser.add_argument("-e", "--env", help="Environment where the process has to be run (DEV or PROD)", required=True)
    parser.add_argument('--help', action='help', help='Show this help message and exit')

    args = {}
    try:
        args = parser.parse_args()
        main(args)
    except Exception as ex:
        logger.error(ex.message)
        sys.exit(1)

    sys.exit(0)

import argparse
from argparse import RawTextHelpFormatter
import datetime
from run_accession_import import get_args_from_private_config_file
import os


def assembly_info(assembly_info_arg):
    assembly_name, assembly_accession, build = map(str.strip, assembly_info_arg.split(','))
    if not assembly_accession.upper().startswith("GC"):
        raise argparse.ArgumentTypeError("'assembly_info' argument was not in the format:"
                                         "<Assembly Name>,<Assembly Accession>,<Build> "
                                         "e.g., Sscrofa11.1,GCA_000003025.6,150")
    return assembly_name, assembly_accession.upper(), build[:3]


def get_import_job_command(assembly_accession, assembly_name, build, program_args):
    return (" {python3_path} -s {species} --scientific-name {scientific_name} " +
            "-a {0} -b {1} -n {2} ".format(assembly_accession, build,
                                           assembly_name) +
            "-p \"{private_config_file}\"").format(**program_args)


def run_jobs(command_line_args):
    program_args = command_line_args.copy()
    program_args.update(get_args_from_private_config_file(command_line_args["private_config_file"]))
    # Use build (info[2]) - accession (info[1]) combination as the sort key so that latest builds appear first
    command_line_args["assembly_info"].sort(key=lambda info: info[2] + "_" + info[1], reverse=True)

    first_job_assembly_info = command_line_args["assembly_info"][0]
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    first_job_assembly_name, first_job_assembly_accession, first_job_build = first_job_assembly_info
    first_job_name = "{0}_import_b{1}_{2}_{3}".format(program_args["env"],
                                                      first_job_build,
                                                      first_job_assembly_accession,
                                                      timestamp)
    first_job_bsub_command = "bsub -J {0} -o {0}.log -e {0}.err".format(first_job_name)
    first_job_bsub_command += get_import_job_command(first_job_assembly_accession, first_job_assembly_name,
                                                     first_job_build, program_args)
    # First job will always have the latest build
    first_job_bsub_command += " -l"

    print("Triggering job: " + first_job_bsub_command)
    os.system(first_job_bsub_command)

    prev_job_name = first_job_name
    for assembly_name, assembly_accession, build in command_line_args["assembly_info"][1:]:
        curr_job_name = "{0}_import_b{1}_{2}_{3}".format(program_args["env"], build, assembly_accession, timestamp)

        job_bsub_command = "bsub -w {0} -J {1} -o {1}.log -e {1}.err".format(prev_job_name, curr_job_name)
        job_bsub_command += get_import_job_command(assembly_accession, assembly_name, build, program_args)
        # All jobs except the first job can skip the first 3 steps
        # which create folders, custom assembly report and the FASTA file
        job_bsub_command += " --step 4"
        job_bsub_command += " -l" if build == str(command_line_args["latest_build"]) else ""

        print("Triggering job: " + job_bsub_command)
        os.system(job_bsub_command)

        prev_job_name = curr_job_name


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Run the accession import process for a given species', add_help=False,
        formatter_class=RawTextHelpFormatter)
    parser.add_argument("-s", "--species", help="Species for which the process has to be run", required=True)
    parser.add_argument("--scientific-name", help="Scientific name for the species (e.g. daucus_carota)", required=True)
    parser.add_argument("-a", "--assembly-info", help="One or more assembly name,accession,build combinations "
                                                      "for which the process has to be run "
                                                      "(GCA preferred for assembly name) "
                                                      "e.g. Sscrofa11.1,GCA_000003025.6,150",
                        nargs='+', type=assembly_info, required=True)
    parser.add_argument("-l", "--latest-build", help="Latest build for this species (relevant for dbsnp table name)",
                        type=int, required=True)
    parser.add_argument("-p", "--private-config-file",
                        help="Path to the configuration file with private connection details, credentials etc.,",
                        required=True)
    parser.add_argument('--help', action='help', help='Show this help message and exit')

    args = {}
    try:
        args = parser.parse_args()
        run_jobs(vars(args))
    except Exception as ex:
        print(ex)

# Copyright 2019 EMBL - European Bioinformatics Institute
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
from argparse import RawTextHelpFormatter
import datetime
from run_accession_import import get_args_from_private_config_file
import os


def assembly_info(assembly_info_arg):
    build, assembly_name, assembly_accession = map(str.strip, assembly_info_arg.split(','))
    if not assembly_accession.upper().startswith("GC"):
        raise argparse.ArgumentTypeError("'assembly_info' argument was not in the format:"
                                         "<Build>,<Assembly Name>,<Assembly Accession> "
                                         "e.g., 150,Sscrofa11.1,GCA_000003025.6")
    return build[:3], assembly_name, assembly_accession.upper()


def get_bsub_mem(Xmx_value):
    # Add a GB to the bsub job
    return eval(Xmx_value.lower().replace("g", "*1024").replace("m", "*1") + " + 1024")


def get_import_job_command(build, assembly_name, assembly_accession, program_args):
    program_args["program_dir"] = os.path.dirname(os.path.realpath(__file__)) + os.path.sep
    program_args["bsub_mem"] = get_bsub_mem(program_args["Xmx"])
    return (" -M {bsub_mem} -R \"rusage[mem={bsub_mem}]\" " +
            "{python3_path} {program_dir}run_accession_import.py -s {species} --scientific-name {scientific_name} " +
            "-a {0} -b {1} -n {2} ".format(assembly_accession, build,
                                           assembly_name) +
            "-p \"{private_config_file}\" --Xmx {Xmx}").format(**program_args)


def run_jobs(command_line_args):
    program_args = command_line_args.copy()
    program_args.update(get_args_from_private_config_file(command_line_args["private_config_file"]))
    job_launch_script_file_name = "{species}_import_job_commands.sh".format(**program_args)
    with open(job_launch_script_file_name, "w") as job_launch_script_file:
        # Use build (info[0]) - accession (info[2]) combination as the sort key so that latest builds appear first
        command_line_args["assembly_info"].sort(key=lambda info: info[0] + "_" + info[2], reverse=True)
        latest_build = max([int(info[0]) for info in command_line_args["assembly_info"]])

        first_job_assembly_info = command_line_args["assembly_info"][0]
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        first_job_build, first_job_assembly_name, first_job_assembly_accession = first_job_assembly_info
        first_job_name = "{0}_import_b{1}_{2}_{3}".format(program_args["env"],
                                                          first_job_build,
                                                          first_job_assembly_accession,
                                                          timestamp)
        first_job_bsub_command = "bsub -J {0} -o {0}.log -e {0}.err".format(first_job_name)
        first_job_bsub_command += get_import_job_command(first_job_build, first_job_assembly_name,
                                                         first_job_assembly_accession, program_args)
        # First job will always have the latest build
        first_job_bsub_command += " -l"

        job_launch_script_file.write(first_job_bsub_command + os.linesep)
        if program_args["only_printing"]:
            print("Writing job command without launching: " + first_job_bsub_command)
        else:
            print("Triggering job: " + first_job_bsub_command)
            os.system(first_job_bsub_command)

        prev_job_name = first_job_name
        for build, assembly_name, assembly_accession in command_line_args["assembly_info"][1:]:
            curr_job_name = "{0}_import_b{1}_{2}_{3}".format(program_args["env"], build, assembly_accession, timestamp)

            job_bsub_command = "bsub -w {0} -J {1} -o {1}.log -e {1}.err".format(prev_job_name, curr_job_name)
            job_bsub_command += get_import_job_command(build, assembly_name, assembly_accession, program_args)
            job_bsub_command += " -l" if build == str(latest_build) else ""

            job_launch_script_file.write(job_bsub_command + os.linesep)
            if program_args["only_printing"]:
                print("Writing job command without launching: " + job_bsub_command)
            else:
                print("Triggering job: " + job_bsub_command)
                os.system(job_bsub_command)

            prev_job_name = curr_job_name

        print("Please make sure to check rs/ss count validation results after all the jobs have completed!")
        print("NOTE: For future reference, bsub commands for this species have been stored in: " +
              job_launch_script_file_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Run the accession import process for a given species', add_help=False,
        formatter_class=RawTextHelpFormatter)
    parser.add_argument("-s", "--species",
                        help="Species for which the process has to be run (e.g. red_sheep_469796)", required=True)
    parser.add_argument("--scientific-name",
                        help="Filesystem-friendly scientific name for the species (e.g. ovis_orientalis)", required=True)
    parser.add_argument("-a", "--assembly-info", help="One or more build,assembly name,accession combinations "
                                                      "for which the process has to be run "
                                                      "(GCA preferred for assembly name) "
                                                      "e.g. 143,Oar_v3.1,GCA_000298735.1",
                        nargs='+', type=assembly_info, required=True)
    parser.add_argument("-p", "--private-config-file",
                        help="Path to the configuration file with private connection details, credentials etc.,",
                        required=True)
    parser.add_argument("--Xmx", help="Memory allocation for the import pipeline (optional)", default="3g")
    parser.add_argument("--only-printing", help="Prepare and write the commands, but don't run them",
                        action='store_true', required=False)
    parser.add_argument('--help', action='help', help='Show this help message and exit')

    args = {}
    try:
        args = parser.parse_args()
        run_jobs(vars(args))
    except Exception as ex:
        print(ex)

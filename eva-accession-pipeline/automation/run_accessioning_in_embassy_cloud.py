#!/usr/bin/env python3

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


import click
import glob
import json
import subprocess
import os
import configparser
import sys
from __init__ import logger

sys.path.append("../../eva-accession-import-automation")
from get_assembly_report_url import get_assembly_report_url


def run_command(command_description, command):
    logger.info("Starting process: " + command_description)
    logger.info("Running command: " + command)
    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1, universal_newlines=True,
                          shell=True) as process:
        for line in process.stdout:
            print(line, end='')
        errors = os.linesep.join(process.stderr.readlines())
    if process.returncode != 0:
        logger.error(command_description + " failed!" + os.linesep + errors)
        raise subprocess.CalledProcessError(process.returncode, process.args)
    else:
        logger.info(command_description + " completed successfully")
    return process.returncode


def get_args_from_private_config_file(private_config_file):
    with open(private_config_file) as private_config_file_handle:
        return json.load(private_config_file_handle)


def create_properties_file_with_new_config(properties_file, accessioning_common_props):
    parser = configparser.ConfigParser()
    parser.optionxform = str
    modified_properties_file = os.path.splitext(properties_file)[0] + "_eva1370_embassy.properties"

    with open(properties_file, "r") as properties_file_handle, \
            open(modified_properties_file, "w") as modified_properties_file_handle:
        # Dummy section is needed because
        # ConfigParser is not clever enough to read config files without section headers
        properties_section_name = "accessioning_properties"
        properties_string = '[{0}]\n{1}'.format(properties_section_name, properties_file_handle.read())
        parser.read_string(properties_string)
        parser.set(section=properties_section_name, option="spring.datasource.url",
                   value="jdbc:postgresql://localhost:{job_tracker_port}/{job_tracker_db}"
                   .format(**accessioning_common_props))
        parser.set(section=properties_section_name, option="spring.data.mongodb.host",
                   value=str(accessioning_common_props["ACCESSIONING_MONGO_HOST"]))
        parser.set(section=properties_section_name, option="spring.data.mongodb.database", value="eva_accession")
        parser.set(section=properties_section_name, option="parameters.assemblyReportUrl",
                   value=get_assembly_report_url(parser.get(section=properties_section_name,
                                                            option="parameters.assemblyAccession")))
        parser.remove_option(section=properties_section_name, option="spring.data.mongodb.username")
        parser.remove_option(section=properties_section_name, option="spring.data.mongodb.password")
        parser.remove_option(section=properties_section_name, option="spring.data.mongodb.authentication-database")

        for key, value in parser.items(section=properties_section_name):
            modified_properties_file_handle.write("{0}={1}{2}".format(key, value, os.linesep))
            modified_properties_file_handle.flush()

    return modified_properties_file


def get_properties_files_for_study(accessioning_common_props):
    return [filename for filename in
            glob.glob("{EVA_STUDIES_BASE_DIR}/{PROJECT_ACCESSION}/{ACCESSION_PROPERTIES_DIR}/*.properties"
                      .format(**accessioning_common_props)) if "eva1370_embassy" not in filename.lower()]


def run_accessioning_for_study_in_embassy_cloud(project_accession, accessioning_common_props):
    accessioning_common_props["PROJECT_ACCESSION"] = project_accession

    modified_properties_files = [create_properties_file_with_new_config(properties_file, accessioning_common_props)
                                 for properties_file in get_properties_files_for_study(accessioning_common_props)]

    accessioning_command = "java -Xmx4g -jar " + accessioning_common_props["ACCESSIONING_JAR_FILE"] + \
                           " --spring.config.location={0}"

    for properties_file in modified_properties_files:
        run_command("Accessioning for {0} with {1}".format(project_accession, properties_file),
                    accessioning_command.format(properties_file))


@click.option("-p", "--project-accession", required=True)
@click.option("-c", "--config-file", required=True)
@click.command()
def main(project_accession, config_file):
    run_accessioning_for_study_in_embassy_cloud(project_accession, get_args_from_private_config_file(config_file))


if __name__ == '__main__':
    main()

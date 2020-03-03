#!/usr/bin/env python3

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

import configparser
import logging
import os
import subprocess
import sys


def init_logger():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)-15s %(levelname)s %(message)s')
    result_logger = logging.getLogger(__name__)
    return result_logger


def get_args_from_properties_file(properties_file):
    parser = configparser.ConfigParser()
    parser.optionxform = str

    with open(properties_file, "r") as properties_file_handle:
        # Dummy section is needed because
        # ConfigParser is not clever enough to read config files without section headers
        properties_section_name = "pipeline_properties"
        properties_string = '[{0}]\n{1}'.format(properties_section_name, properties_file_handle.read())
        parser.read_string(properties_string)
        config = dict(parser.items(section=properties_section_name))
        return config


def get_mongo_connection_details_from_properties_file(properties_file):
    properties_file_args = get_args_from_properties_file(properties_file)
    mongo_connection_properties = {"mongo_host": properties_file_args["spring.data.mongodb.host"],
                                   "mongo_port": properties_file_args["spring.data.mongodb.port"],
                                   "mongo_db": properties_file_args["spring.data.mongodb.database"],
                                   "mongo_username": properties_file_args["spring.data.mongodb.username"],
                                   "mongo_password": properties_file_args["spring.data.mongodb.password"],
                                   "mongo_auth_db": properties_file_args["spring.data.mongodb.authentication-database"]}
    return mongo_connection_properties


def run_command_with_output(command_description, command, return_process_output=False):
    process_output = ""

    logger.info("Starting process: " + command_description)
    logger.info("Running command: " + command)

    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1, universal_newlines=True,
                          shell=True) as process:
        for line in iter(process.stdout.readline, ''):
            line = str(line).rstrip()
            logger.info(line)
            if return_process_output:
                process_output += line
        for line in iter(process.stderr.readline, ''):
            line = str(line).rstrip()
            logger.error(line)
    if process.returncode != 0:
        logger.error(command_description + " failed! Refer to the error messages for details.")
        raise subprocess.CalledProcessError(process.returncode, process.args)
    else:
        logger.info(command_description + " - completed successfully")
    if return_process_output:
        return process_output


logger = init_logger()

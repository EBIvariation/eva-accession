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
import hashlib
import subprocess
import os
import configparser
from __init__ import logger
from run_accession_import import get_args_from_private_config_file


def run_command_with_output(command_description, command):
    process_output = ""

    logger.info("Starting process: " + command_description)
    logger.info("Running command: " + command)

    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1, universal_newlines=True,
                          shell=True) as process:
        for line in process.stdout:
            process_output += line
        errors = os.linesep.join(process.stderr.readlines())
    if process.returncode != 0:
        logger.error(command_description + " failed!" + os.linesep + errors)
        raise subprocess.CalledProcessError(process.returncode, process.args)
    else:
        logger.info(command_description + " completed successfully")
    return process_output


def get_args_from_assembly_properties_file(assembly_properties_file):
    parser = configparser.ConfigParser()
    parser.optionxform = str

    with open(assembly_properties_file, "r") as properties_file_handle:
        # Dummy section is needed because
        # ConfigParser is not clever enough to read config files without section headers
        properties_section_name = "assembly_properties"
        properties_string = '[{0}]\n{1}'.format(properties_section_name, properties_file_handle.read())
        parser.read_string(properties_string)
        config = dict(parser.items(section=properties_section_name))
        return config


def validate_imported_contigs(assembly_properties_file, config_file):
    config = get_args_from_private_config_file(config_file)
    config.update(get_args_from_assembly_properties_file(assembly_properties_file))
    # We need to rename the keys because string interpolation won't work if there is a dot character in them
    config["assembly_report_path"] = config["parameters.assemblyReportUrl"].split("file:")[-1]
    config["assembly_md5"] = hashlib.md5(config["parameters.assemblyName"].encode("utf-8")).hexdigest()
    config["taxonomy_accession"] = config["parameters.taxonomyAccession"]

    config["contig_chr_mismatch_table"] = "dbsnp_ensembl_species.dbsnp_species_with_contig_chromosome_start_mismatch"
    config["contig_chr_match_table"] = "dbsnp_ensembl_species.dbsnp_species_with_contig_chromosome_start_match"
    final_formatting_genbank_accessions_cmd = r"cut -d$'\t' -f5 | sort | uniq | paste -s - | sed s/$'\t'/\",\"/g | sed s/$/\"/g | sed s/^/\"/g"
    get_contigs_start_mismatch_cmd = "psql -A -t -h {metahost} -U {metauser} -d {metadb} -c " \
                                     "\"select distinct contig_name from {contig_chr_mismatch_table} " \
                                     "where table_name like '%{assembly_md5}%'" \
                                     " and schema_name like '%{taxonomy_accession}%'\" -P pager=off " \
                                     "| grep {assembly_report_path} -f - |".format(**config) + \
                                     final_formatting_genbank_accessions_cmd
    get_contigs_start_match_cmd = "psql -A -t -h {metahost} -U {metauser} -d {metadb} -c " \
                                  "\"select distinct contig_name from {contig_chr_match_table} " \
                                  "where table_name like '%{assembly_md5}%'" \
                                  " and schema_name like '%{taxonomy_accession}%'\" -P pager=off " \
                                  "| grep {assembly_report_path} -f - | grep -v assembled-molecule |".format(**config) \
                                  + final_formatting_genbank_accessions_cmd
    mongo_run_command_template = "mongo --quiet --host {0} --port {1} --username {2} " \
                                 "--password {3} --authenticationDatabase=admin " \
                                 "{4} --eval 'db.{5}.findOne({{\"seq\": \"{6}\", " \
                                 "\"contig\": {{$in: [{7}]}}}})'"

    mismatch_contig_set = run_command_with_output("Get contigs with start mismatch against chromosome:",
                                                  get_contigs_start_mismatch_cmd).strip()
    match_contig_set = run_command_with_output("Get contigs with start match against chromosome:",
                                               get_contigs_start_match_cmd).strip()

    collections_to_check = ["dbsnpSubmittedVariantEntity", "dbsnpClusteredVariantEntity"]
    if mismatch_contig_set != '""':
        for collection in collections_to_check:
            mongo_run_command = mongo_run_command_template.format(config["mongo_host"],
                                                                  config["mongo_port"],
                                                                  config["mongo_user"],
                                                                  config["mongo_password"],
                                                                  config["mongo_acc_db"],
                                                                  collection,
                                                                  config["parameters.assemblyAccession"],
                                                                  mismatch_contig_set)
            mongo_run_command_output = run_command_with_output("Check if mismatched contigs from above " +
                                                               "are present in " + collection + " for the assembly",
                                                               mongo_run_command)
            logger.info("Mongo command output:" + os.linesep + mongo_run_command_output)
    else:
        logger.info("No mismatch contig set available!")

    if match_contig_set != '""':
        for collection in collections_to_check:
            mongo_run_command = mongo_run_command_template.format(config["mongo_host"],
                                                                  config["mongo_port"],
                                                                  config["mongo_user"],
                                                                  config["mongo_password"],
                                                                  config["mongo_acc_db"],
                                                                  collection,
                                                                  config["parameters.assemblyAccession"],
                                                                  match_contig_set)
            mongo_run_command_output = run_command_with_output("Check if matched contigs from above " +
                                                               "are present in " + collection + " for the assembly",
                                                               mongo_run_command)
            logger.info("Mongo command output:" + os.linesep + mongo_run_command_output)
    else:
        logger.info("No matched non-chromosome contig set available!")


@click.option("-p", "--assembly-properties-file", required=True)
@click.option("-c", "--config-file", required=True)
@click.command()
def main(assembly_properties_file, config_file):
    validate_imported_contigs(assembly_properties_file, config_file)


if __name__ == '__main__':
    main()

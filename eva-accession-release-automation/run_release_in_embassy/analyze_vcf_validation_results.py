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
import sys

from ebi_eva_common_pyutils.command_utils import run_command_with_output
from ebi_eva_common_pyutils.logger import logging_config as log_cfg, logging_config
from run_release_in_embassy.release_metadata import vcf_validation_output_file_pattern, asm_report_output_file_pattern

logger = log_cfg.get_logger(__name__)


def analyze_vcf_validation_files(vcf_validation_report_files):
    exit_code = 0
    vcf_validation_report_error_classes_to_ignore = ["Error: Duplicated variant",
                                                     "Warning: Reference and alternate alleles "
                                                     "do not share the first nucleotide",
                                                     "the input file is not valid",
                                                     "the input file is valid",
                                                     "not listed in a valid meta-data ALT entry",
                                                     "Chromosome is not a string without colons or whitespaces"]
    vcf_validation_error_grep_command_chain = " | ".join(['grep -v "{0}"'.format(error_class) for error_class in
                                                          vcf_validation_report_error_classes_to_ignore])
    for vcf_validation_report_file in vcf_validation_report_files:
        logger.info("Analyzing file {0} ....".format(vcf_validation_report_file))
        command_to_run = "cat {0} | {1} | wc -l".format(vcf_validation_report_file,
                                                        vcf_validation_error_grep_command_chain)
        number_of_lines_with_unusual_errors = \
            int(run_command_with_output("Checking unusual errors in {0}".format(vcf_validation_report_file),
                                        command_to_run, return_process_output=True))
        if number_of_lines_with_unusual_errors > 0:
            logger.error("Unusual error(s) found in VCF validation log: {0}. \nRun command\n {1} \nfor details."
                         .format(vcf_validation_report_file, command_to_run))
            exit_code = -1
    return exit_code


def analyze_asm_report_files(asm_report_files):
    exit_code = 0
    assembly_report_error_classes_to_ignore = ["not present in FASTA file", "does not match the reference sequence",
                                               "Multiple synonyms  found for contig"]

    asm_report_error_grep_command_chain = " | ".join(['grep -v "{0}"'.format(error_class) for error_class in
                                                      assembly_report_error_classes_to_ignore])
    for asm_report_file in asm_report_files:
        logger.info("Analyzing file {0} ....".format(asm_report_file))
        command_to_run = "cat {0} | {1} | wc -l".format(asm_report_file, asm_report_error_grep_command_chain)
        number_of_lines_with_unusual_errors = \
            int(run_command_with_output("Checking unusual errors in {0}".format(asm_report_file), command_to_run,
                                        return_process_output=True))
        if number_of_lines_with_unusual_errors > 0:
            logger.error("Unusual error(s) found in assembly report log: {0}. \nRun command\n {1} \nfor details."
                         .format(asm_report_file, command_to_run))
            exit_code = -1
    return exit_code


def analyze_vcf_validation_results(assembly_release_folder, assembly_accession):
    vcf_validation_report_files = glob.glob("{0}/{2}".format(assembly_release_folder, assembly_accession,
                                                             vcf_validation_output_file_pattern))
    exit_code = analyze_vcf_validation_files(vcf_validation_report_files)
    asm_report_files = glob.glob("{0}/{2}".format(assembly_release_folder,  asm_report_output_file_pattern))
    exit_code = exit_code or analyze_asm_report_files(asm_report_files)
    sys.exit(exit_code)


@click.option("--assembly-release-folder", required=True)
@click.option("--assembly-accession", required=True)
@click.command()
def main(assembly_release_folder, assembly_accession):
    logging_config.add_stdout_handler()
    analyze_vcf_validation_results(assembly_release_folder, assembly_accession)


if __name__ == '__main__':
    main()

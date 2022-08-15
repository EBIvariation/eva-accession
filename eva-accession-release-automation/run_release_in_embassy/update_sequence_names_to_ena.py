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


import click
import os

from run_release_in_embassy.release_common_utils import get_release_vcf_file_name, get_release_vcf_file_name_genbank
from run_release_in_embassy.release_metadata import get_release_inventory_info_for_assembly, \
    release_vcf_file_categories, vcf_validation_output_file_pattern, asm_report_output_file_pattern
from ebi_eva_common_pyutils.command_utils import run_command_with_output


def update_sequence_name(assembly_accession, species_release_folder, sequence_name_converter_path, bcftools_path):
    commands = []
    for vcf_file_category in release_vcf_file_categories:

        release_vcf_file_name = get_release_vcf_file_name_genbank(species_release_folder, assembly_accession, vcf_file_category)

        release_vcf_file_output_name = get_release_vcf_file_name(species_release_folder, assembly_accession, vcf_file_category)
        commands.append("{0} -i {1}.gz -o {2}.gz -c enaSequenceName".format(sequence_name_converter_path,
                                                                        release_vcf_file_name,
                                                                        release_vcf_file_output_name))
        commands.append("({0} index --csi {1}.gz)".format(bcftools_path, release_vcf_file_output_name))
    # We don't expect the validation commands to all pass, hence use semi-colon to run them back to back
    contig_name_change_command = " ; ".join(commands)
    run_command_with_output("Changing contig name to ENA for assembly " + assembly_accession, contig_name_change_command)


@click.option("--assembly-accession", help="ex: GCA_000003055.6", required=True)
@click.option("--species-release-folder", required=True)
@click.option("--sequence-name-converter-path", help="/path/to/vcf/sequence-name-converter", required=True)
@click.option("--bcftools-path", help="ex: /path/to/bcftools/binary", required=True)
@click.command()
def main(assembly_accession, species_release_folder, sequence_name_converter_path, bcftools_path):
    update_sequence_name(assembly_accession,species_release_folder, sequence_name_converter_path, bcftools_path)


if __name__ == "__main__":
    main()

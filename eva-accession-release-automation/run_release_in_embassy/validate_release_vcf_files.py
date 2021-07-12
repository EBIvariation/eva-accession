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
import psycopg2

from run_release_in_embassy.release_common_utils import get_release_vcf_file_name
from run_release_in_embassy.release_metadata import get_release_inventory_info_for_assembly, \
    release_vcf_file_categories, vcf_validation_output_file_pattern, asm_report_output_file_pattern
from ebi_eva_common_pyutils.command_utils import run_command_with_output
from ebi_eva_common_pyutils.config_utils import get_pg_metadata_uri_for_eva_profile


def remove_index_if_outdated(fasta_path):
    """Remove fasta index file if it's older than the fasta file, assembly checker will regenerate."""
    fasta_index_path = f'{fasta_path}.fai'
    if os.path.exists(fasta_index_path) and os.path.getmtime(fasta_index_path) < os.path.getmtime(fasta_path):
        os.remove(fasta_index_path)


def validate_release_vcf_files(private_config_xml_file, profile, taxonomy_id, assembly_accession,
                               release_species_inventory_table, release_version, species_release_folder,
                               vcf_validator_path, assembly_checker_path):
    run_command_with_output("Remove existing VCF validation and assembly report outputs...",
                            "rm -f {0}/{1}/{2} {0}/{1}/{3}".format(species_release_folder, assembly_accession,
                                                                   vcf_validation_output_file_pattern,
                                                                   asm_report_output_file_pattern))
    validate_release_vcf_files_commands = []
    with psycopg2.connect(get_pg_metadata_uri_for_eva_profile(profile, private_config_xml_file),
                          user="evadev") as \
            metadata_connection_handle:
        release_inventory_info_for_assembly = get_release_inventory_info_for_assembly(taxonomy_id, assembly_accession,
                                                                                      release_species_inventory_table,
                                                                                      release_version,
                                                                                      metadata_connection_handle)
        fasta_path = release_inventory_info_for_assembly["fasta_path"]
        assembly_report_path = release_inventory_info_for_assembly["report_path"]
        remove_index_if_outdated(fasta_path)
        if assembly_report_path.startswith("file:/"):
            assembly_report_path = assembly_report_path.replace("file:/", "/")

        for vcf_file_category in release_vcf_file_categories:

            release_vcf_file_name = get_release_vcf_file_name(species_release_folder, assembly_accession,
                                                              vcf_file_category)
            release_vcf_dir = os.path.dirname(release_vcf_file_name)
            if "multimap" not in vcf_file_category:
                validate_release_vcf_files_commands.append("({0} -i {1} -o {2}) || true".format(vcf_validator_path,
                                                                                                release_vcf_file_name,
                                                                                                release_vcf_dir))
                validate_release_vcf_files_commands.append("({0} -i {1} -f {2} -a {3} -o {4} -r text,summary) || true"
                                                           .format(assembly_checker_path, release_vcf_file_name,
                                                                   fasta_path, assembly_report_path, release_vcf_dir))

        # We don't expect the validation commands to all pass, hence use semi-colon to run them back to back
        final_validate_command = " ; ".join(validate_release_vcf_files_commands)
        run_command_with_output("Validating release files for assembly: " + assembly_accession, final_validate_command)


@click.option("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
@click.option("--profile", help="Maven profile to use, ex: internal", required=True)
@click.option("--taxonomy-id", help="ex: 9913", required=True)
@click.option("--assembly-accession", help="ex: GCA_000003055.6", required=True)
@click.option("--release-species-inventory-table", default="dbsnp_ensembl_species.release_species_inventory",
              required=False)
@click.option("--release-version", help="ex: 2", type=int, required=True)
@click.option("--species-release-folder", required=True)
@click.option("--vcf-validator-path", help="/path/to/vcf/validator/binary", required=True)
@click.option("--assembly-checker-path", help="/path/to/assembly/checker/binary", required=True)
@click.command()
def main(private_config_xml_file, profile, taxonomy_id, assembly_accession, release_species_inventory_table, release_version,
         species_release_folder, vcf_validator_path, assembly_checker_path):
    validate_release_vcf_files(private_config_xml_file, profile, taxonomy_id, assembly_accession,
                               release_species_inventory_table, release_version, species_release_folder,
                               vcf_validator_path, assembly_checker_path)


if __name__ == "__main__":
    main()

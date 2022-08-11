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

from ebi_eva_common_pyutils.command_utils import run_command_with_output
from run_release_in_embassy.release_metadata import release_vcf_file_categories, release_text_file_categories
from run_release_in_embassy.release_common_utils import get_release_vcf_file_name_genbank, \
    get_unsorted_release_vcf_file_name, get_release_text_file_name, get_unsorted_release_text_file_name, \
    get_bgzip_bcftools_index_commands_for_file


def sort_bgzip_index_release_files(bgzip_path, bcftools_path, vcf_sort_script_path, assembly_accession,
                                   species_release_folder):
    commands = []
    # These files are left behind by the sort_vcf_sorted_chromosomes.sh script
    # To be idempotent, remove such files
    commands.append("rm -f {0}/{1}/*.chromosomes".format(species_release_folder, assembly_accession))
    for vcf_file_category in release_vcf_file_categories:
        unsorted_release_file_name = get_unsorted_release_vcf_file_name(species_release_folder, assembly_accession,
                                                                        vcf_file_category)
        sorted_release_file_name = get_release_vcf_file_name_genbank(species_release_folder, assembly_accession,
                                                             vcf_file_category)
        commands.append("rm -f {2} && {0} -f {1} {2}".format(vcf_sort_script_path,
                                                             unsorted_release_file_name,
                                                             sorted_release_file_name))
        commands.extend(get_bgzip_bcftools_index_commands_for_file(bgzip_path, bcftools_path, sorted_release_file_name))
    for text_release_file_category in release_text_file_categories:
        unsorted_release_file_name = get_unsorted_release_text_file_name(species_release_folder, assembly_accession,
                                                                         text_release_file_category)
        sorted_release_file_name = get_release_text_file_name(species_release_folder, assembly_accession,
                                                              text_release_file_category)
        commands.append("(sort -V {1} | uniq > {2})".format(vcf_sort_script_path,
                                                            unsorted_release_file_name,
                                                            sorted_release_file_name))
        commands.append("(gzip < {0} > {0}.gz)".format(sorted_release_file_name))
    command = " && ".join(commands)
    run_command_with_output("Sort, bgzip and index release files for assembly: " + assembly_accession,
                            command)


@click.option("--bgzip-path", help="ex: /path/to/bgzip/binary", required=True)
@click.option("--bcftools-path", help="ex: /path/to/bcftools/binary", required=True)
@click.option("--vcf-sort-script-path", help="ex: /path/to/vcf/sort/script", required=True)
@click.option("--assembly-accession", help="ex: GCA_000003055.6", required=True)
@click.option("--species-release-folder", required=True)
@click.command()
def main(bgzip_path, bcftools_path, vcf_sort_script_path, assembly_accession, species_release_folder):
    sort_bgzip_index_release_files(bgzip_path, bcftools_path, vcf_sort_script_path, assembly_accession,
                                   species_release_folder)


if __name__ == "__main__":
    main()

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
from run_release_in_embassy.release_common_utils import get_bgzip_tabix_commands_for_file, \
    get_release_vcf_file_name, get_release_text_file_name


def compress_release_files(bgzip_path, tabix_path, assembly_accession, release_folder):
    compress_release_files_commands = []
    for vcf_file_category in release_vcf_file_categories:
        release_vcf_file_name = get_release_vcf_file_name(release_folder, assembly_accession, vcf_file_category)
        compress_release_files_commands.extend(get_bgzip_tabix_commands_for_file(bgzip_path, tabix_path,
                                                                                 release_vcf_file_name))
    for text_release_file_category in release_text_file_categories:
        text_release_file_name = get_release_text_file_name(release_folder, assembly_accession,
                                                            text_release_file_category)
        compress_release_files_commands.append("rm -f {0}.gz".format(text_release_file_name))
        compress_release_files_commands.append("(gzip < {0} > {0}.gz)".format(text_release_file_name))

    final_compress_command = " && ".join(compress_release_files_commands)
    run_command_with_output("Compressing release files for assembly: " + assembly_accession, final_compress_command)


@click.option("--bgzip-path", help="ex: /path/to/bgzip/binary", required=True)
@click.option("--tabix-path", help="ex: /path/to/tabix/binary", required=True)
@click.option("--assembly-accession", help="ex: GCA_000003055.6", required=True)
@click.option("--release-folder", required=True)
@click.command()
def main(bgzip_path, tabix_path, assembly_accession, release_folder):
    compress_release_files(bgzip_path, tabix_path, assembly_accession, release_folder)


if __name__ == "__main__":
    main()

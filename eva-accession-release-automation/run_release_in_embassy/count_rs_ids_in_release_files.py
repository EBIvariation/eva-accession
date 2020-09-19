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

from ebi_eva_common_pyutils.command_utils import run_command_with_output
from run_release_in_embassy.release_metadata import release_vcf_file_categories, release_text_file_categories
from run_release_in_embassy.release_common_utils import get_release_vcf_file_name, get_release_text_file_name


def count_rs_ids_in_release_files(count_ids_script_path, assembly_accession, release_folder):
    release_count_filename = os.path.join(release_folder, assembly_accession, "README_rs_ids_counts.txt")
    with open(release_count_filename, "w") as release_count_file_handle:
        release_count_file_handle.write("# Unique RS ID counts\n")
        for vcf_file_category in release_vcf_file_categories:
            release_vcf_file_name = get_release_vcf_file_name(release_folder, assembly_accession, vcf_file_category)
            num_ids_in_file = run_command_with_output("Counting RS IDs in file: " + release_vcf_file_name,
                                                      "{0} {1}.gz".format(count_ids_script_path, release_vcf_file_name),
                                                      return_process_output=True)
            release_count_file_handle.write(num_ids_in_file)
        for text_release_file_category in release_text_file_categories:
            text_release_file_name = get_release_text_file_name(release_folder, assembly_accession,
                                                                text_release_file_category)
            num_ids_in_file = run_command_with_output("Counting RS IDs in file: " + text_release_file_name,
                                                      "zcat {0}.gz | cut -f1 | uniq | wc -l"
                                                      .format(text_release_file_name), return_process_output=True)
            release_count_file_handle.write("{0}.gz\t{1}".format(os.path.basename(text_release_file_name),
                                                                 str(num_ids_in_file)))


@click.option("--count-ids-script-path", help="ex: /path/to/count/ids/script", required=True)
@click.option("--assembly-accession", help="ex: GCA_000003055.6", required=True)
@click.option("--release-folder", required=True)
@click.command()
def main(count_ids_script_path, assembly_accession, release_folder):
    count_rs_ids_in_release_files(count_ids_script_path, assembly_accession, release_folder)


if __name__ == "__main__":
    main()

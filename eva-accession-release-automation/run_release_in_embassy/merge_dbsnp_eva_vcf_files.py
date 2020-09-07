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
import glob
import os

from ebi_eva_common_pyutils.command_utils import run_command_with_output


def merge_dbsnp_eva_files(bgzip_path, tabix_path, bcftools_path, assembly_accession, release_folder):
    release_vcf_file_categories = ["current_ids", "merged_ids", "multimap_ids"]
    release_text_file_categories = ["deprecated_ids.unsorted", "merged_deprecated_ids.unsorted"]

    merge_commands = []
    for vcf_file_category in release_vcf_file_categories:
        merge_commands.extend(merge_dbsnp_eva_vcf_files(bgzip_path, tabix_path, bcftools_path, assembly_accession,
                                                        release_folder, vcf_file_category))
    for text_release_file_category in release_text_file_categories:
        merge_commands.extend(merge_dbsnp_eva_text_files(assembly_accession, release_folder,
                                                         text_release_file_category))
    final_merge_command = " && ".join(merge_commands)
    run_command_with_output("Merging dbSNP and EVA release files for assembly: " + assembly_accession,
                            final_merge_command)


def merge_dbsnp_eva_vcf_files(bgzip_path, tabix_path, bcftools_path, assembly_accession, release_folder,
                              vcf_file_category):
    vcf_merge_commands = []
    release_file_name = "{0}/{1}/{1}_{2}.vcf.gz".format(release_folder, assembly_accession, vcf_file_category)
    files_in_category = glob.glob("{0}/{1}/*{1}_{2}.vcf".format(release_folder, assembly_accession, vcf_file_category))

    if len(files_in_category) == 0:
        raise Exception("No VCF files found in the release folder in the {0} category".format(vcf_file_category))
    if len(files_in_category) > 2:
        raise Exception("More than two VCF files found in the release folder in the {0} category"
                        .format(vcf_file_category))
    file_prefixes = set([os.path.basename(filename).split("_")[0].lower() for filename in files_in_category])
    if "eva" in file_prefixes and "dbsnp" in file_prefixes:
        vcf_merge_commands.append("rm -f {0}.gz {1}.gz".format(files_in_category[0], files_in_category[1]))
        vcf_merge_commands.append("rm -f {0}.gz.tbi {1}.gz.tbi".format(files_in_category[0], files_in_category[1]))
        vcf_merge_commands.append("({0} < {1} > {1}.gz)".format(bgzip_path, files_in_category[0]))
        vcf_merge_commands.append("({0} < {1} > {1}.gz)".format(bgzip_path, files_in_category[1]))
        vcf_merge_commands.append("({0} -f {1}.gz)".format(tabix_path, files_in_category[0]))
        vcf_merge_commands.append("({0} -f {1}.gz)".format(tabix_path, files_in_category[1]))
        vcf_merge_commands.append("({0} merge -O z {1}.gz {2}.gz > {3})".format(bcftools_path,
                                                                                files_in_category[0],
                                                                                files_in_category[1],
                                                                                release_file_name))
    else:
        if len(files_in_category) == 2:
            raise Exception("A non-EVA or non-dbSNP VCF was found in the release folder in the {0} category"
                            .format(vcf_file_category))
        vcf_merge_commands.append("(cp {0} > {1})".format(files_in_category[0], release_file_name))

    return vcf_merge_commands


def merge_dbsnp_eva_text_files(assembly_accession, release_folder, text_release_file_category):
    text_release_file_merge_commands = []
    release_file_name = "{0}/{1}/{1}_{2}.txt".format(release_folder, assembly_accession, text_release_file_category)
    print("{0}/{1}/*_{2}.txt".format(release_folder, assembly_accession, text_release_file_category))
    files_in_category = glob.glob("{0}/{1}/*{1}_{2}.txt".format(release_folder, assembly_accession,
                                                             text_release_file_category))
    if len(files_in_category) > 2:
        raise Exception("More than two files found in the release folder in the {0} category"
                        .format(text_release_file_category))
    file_prefixes = set([os.path.basename(filename).split("_")[0].lower() for filename in files_in_category])
    if "eva" in file_prefixes and "dbsnp" in file_prefixes:
        text_release_file_merge_commands.append("(cat {0} {1} > {2})".format(files_in_category[0], files_in_category[1],
                                                                           release_file_name))
    else:
        if len(files_in_category) == 2:
            raise Exception("A non-EVA or non-dbSNP file was found in the release folder in the {0} category"
                            .format(text_release_file_category))
        text_release_file_merge_commands.append("(cp {0} > {1})".format(files_in_category[0], release_file_name))

    return text_release_file_merge_commands


@click.option("--bgzip-path", help="ex: /path/to/bgzip/binary", required=True)
@click.option("--tabix-path", help="ex: /path/to/tabix/binary", required=True)
@click.option("--bcftools-path", help="ex: /path/to/vcftools/binary", required=True)
@click.option("--assembly-accession", help="ex: GCA_000003055.6", required=True)
@click.option("--release-folder", required=True)
@click.command()
def main(bgzip_path, tabix_path, bcftools_path, assembly_accession, release_folder):
    merge_dbsnp_eva_files(bgzip_path, tabix_path, bcftools_path, assembly_accession, release_folder)


if __name__ == "__main__":
    main()

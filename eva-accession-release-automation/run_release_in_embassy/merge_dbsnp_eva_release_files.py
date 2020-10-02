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
import collections
import glob
import gzip
import os

from ebi_eva_common_pyutils.command_utils import run_command_with_output
from run_release_in_embassy.release_metadata import release_vcf_file_categories, release_text_file_categories
from run_release_in_embassy.release_common_utils import get_bgzip_tabix_commands_for_file, \
    get_release_vcf_file_name, get_unsorted_release_vcf_file_name, get_unsorted_release_text_file_name


def move_release_files_to_unsorted_category(assembly_accession, release_folder, vcf_file_category,
                                            unsorted_release_file_path):
    unsorted_release_file_name = os.path.basename(unsorted_release_file_path)
    release_file_path = get_release_vcf_file_name(release_folder, assembly_accession, vcf_file_category)
    release_file_name = os.path.basename(release_file_path)
    for variant_source in ["eva", "dbsnp"]:
        vcf_file_name = release_file_path.replace(release_file_name,
                                                  "{0}_{1}".format(variant_source, release_file_name))
        unsorted_file_name = unsorted_release_file_path.replace(unsorted_release_file_name,
                                                                "{0}_{1}".format(variant_source,
                                                                                 unsorted_release_file_name))
        if os.path.exists(vcf_file_name) and not os.path.exists(unsorted_file_name):
            os.rename(vcf_file_name, unsorted_file_name)


def get_bgzip_and_tabix_commands(bgzip_path, tabix_path, vcf_sort_script_path, files_in_category):
    commands = []
    for unsorted_file_name in files_in_category:
        sorted_file_name = unsorted_file_name.replace("_unsorted", "")
        commands.append(
            "rm -f {0}/*.chromosomes && {1} -f {2} {3}".format(os.path.dirname(unsorted_file_name),
                                                               vcf_sort_script_path,
                                                               unsorted_file_name, sorted_file_name))
        commands.extend(get_bgzip_tabix_commands_for_file(bgzip_path, tabix_path, sorted_file_name))
    return commands


# This is needed because bcftools merge messes up the order of the header meta-information lines when merging
def merge_dbsnp_eva_vcf_headers(file1, file2, output_file):
    import tempfile
    run_command_with_output("Removing output file {0} if it already exists...".format(output_file),
                            "rm -f " + output_file)
    working_folder = os.path.dirname(file1)
    # Write content for each meta info category in the header to a specific temp file
    metainfo_category_tempfile_map = collections.OrderedDict([("fileformat", None), ("info", None),
                                                              ("contig", None), ("reference", None)])
    for category in metainfo_category_tempfile_map.keys():
        metainfo_category_tempfile_map[category] = open(tempfile.mktemp(prefix=category, dir=working_folder), "a+")
    with open(file1) as file1_handle, open(file2) as file2_handle:
        for file_handle in [file1_handle, file2_handle]:
            for line in file_handle:
                if line.startswith("##"):
                    metainfo_category = line.split("=")[0].split("##")[-1].lower()
                    metainfo_category_tempfile_map[metainfo_category].write(line)
                else:
                    break
    for metainfo_category, tempfile_handle in metainfo_category_tempfile_map.items():
        tempfile_handle.flush()
        # Sorting needs to happen by ID field for the headers
        # ex: ##contig=<ID=1,accession="CM000994.2">
        run_command_with_output("Merging header section ##{0} ...".format(metainfo_category),
                                "sort -t ',' -k1 -V {0} | uniq >> {1}".format(tempfile_handle.name, output_file))
        tempfile_handle.close()
        os.remove(tempfile_handle.name)


def merge_dbsnp_eva_vcf_files(bgzip_path, tabix_path, bcftools_path, vcf_sort_script_path, assembly_accession,
                              release_folder, vcf_file_category):
    vcf_merge_commands = []
    # This is the desired post-merge output file name in the format <assembly>_<category>.vcf
    # ex: GCA_000409795.2_merged_ids.vcf
    unsorted_release_file_path = get_unsorted_release_vcf_file_name(release_folder, assembly_accession,
                                                                    vcf_file_category)
    unsorted_release_file_name = os.path.basename(unsorted_release_file_path)
    # After release pipeline is run on a species, the default VCF output files are in the formats like below
    # ex: eva_GCA_000409795.2_merged_ids.vcf and dbsnp_GCA_000409795.2_merged_ids.vcf
    # Move them to files with _unsorted suffix to avoid confusion
    move_release_files_to_unsorted_category(assembly_accession, release_folder, vcf_file_category,
                                            unsorted_release_file_path)
    eva_dbsnp_vcf_file_patterns = unsorted_release_file_path.replace(unsorted_release_file_name,
                                                                     "*_" + unsorted_release_file_name)
    files_in_category = glob.glob(eva_dbsnp_vcf_file_patterns)

    if len(files_in_category) == 0:
        raise Exception("No VCF files found in the release folder in the {0} category".format(vcf_file_category))
    if len(files_in_category) > 2:
        raise Exception("More than two VCF files found in the release folder in the {0} category"
                        .format(vcf_file_category))
    file_prefixes = set([os.path.basename(filename).split("_")[0].lower() for filename in files_in_category])
    if "eva" in file_prefixes and "dbsnp" in file_prefixes:
        merge_dbsnp_eva_vcf_headers(files_in_category[0], files_in_category[1], unsorted_release_file_path)
        # Merge commands require input VCF files to be sorted, bgzipped and tabixed!!
        vcf_merge_commands.extend(
            get_bgzip_and_tabix_commands(bgzip_path, tabix_path, vcf_sort_script_path, files_in_category))
        sorted_file_names = [name.replace("_unsorted", "") for name in files_in_category]
        vcf_merge_commands.append(
            "(({0} merge --no-version -O v {1}.gz {2}.gz | grep -v ^##) >> {3})".format(bcftools_path,
                                                                                        sorted_file_names[0],
                                                                                        sorted_file_names[1],
                                                                                        unsorted_release_file_path))
    else:
        if len(files_in_category) == 2:
            raise Exception("A non-EVA or non-dbSNP VCF was found in the release folder in the {0} category"
                            .format(vcf_file_category))
        # If there is only one source (EVA or dbSNP), just straight up copy the file to the destination
        vcf_merge_commands.append("(cp {0} {1})".format(files_in_category[0], unsorted_release_file_path))

    return vcf_merge_commands


def merge_dbsnp_eva_text_files(assembly_accession, release_folder, text_release_file_category):
    text_release_file_merge_commands = []
    unsorted_release_file_path = get_unsorted_release_text_file_name(release_folder, assembly_accession,
                                                                     text_release_file_category)
    unsorted_release_file_name = os.path.basename(unsorted_release_file_path)
    # After release is run on a species, the default text (i.e., non-vcf) output files have ".unsorted.txt" file suffix
    # ex: eva_GCA_000409795.2_merged_deprecated_ids.unsorted.txt
    eva_dbsnp_text_file_patterns = unsorted_release_file_path.replace(unsorted_release_file_name, "*_" +
                                                                      unsorted_release_file_name)
    files_in_category = glob.glob(eva_dbsnp_text_file_patterns)

    if len(files_in_category) > 2:
        raise Exception("More than two files found in the release folder in the {0} category"
                        .format(text_release_file_category))
    file_prefixes = set([os.path.basename(filename).split("_")[0].lower() for filename in files_in_category])
    if "eva" in file_prefixes and "dbsnp" in file_prefixes:
        text_release_file_merge_commands.append("(cat {0} {1} > {2})".format(files_in_category[0], files_in_category[1],
                                                                             unsorted_release_file_path))
    else:
        if len(files_in_category) == 2:
            raise Exception("A non-EVA or non-dbSNP file was found in the release folder in the {0} category"
                            .format(text_release_file_category))
        text_release_file_merge_commands.append("(cp {0} > {1})".format(files_in_category[0],
                                                                        unsorted_release_file_path))

    return text_release_file_merge_commands


def merge_dbsnp_eva_release_files(bgzip_path, tabix_path, bcftools_path, vcf_sort_script_path, assembly_accession,
                                  release_folder):
    merge_commands = []
    for vcf_file_category in release_vcf_file_categories:
        merge_commands.extend(merge_dbsnp_eva_vcf_files(bgzip_path, tabix_path, bcftools_path, vcf_sort_script_path,
                                                        assembly_accession, release_folder, vcf_file_category))
    for text_release_file_category in release_text_file_categories:
        merge_commands.extend(merge_dbsnp_eva_text_files(assembly_accession, release_folder,
                                                         text_release_file_category))
    final_merge_command = " && ".join(merge_commands)
    run_command_with_output("Merging dbSNP and EVA release files for assembly: " + assembly_accession,
                            final_merge_command)


@click.option("--bgzip-path", help="ex: /path/to/bgzip/binary", required=True)
@click.option("--tabix-path", help="ex: /path/to/tabix/binary", required=True)
@click.option("--bcftools-path", help="ex: /path/to/vcftools/binary", required=True)
@click.option("--vcf-sort-script-path", help="ex: /path/to/vcf/sort/script", required=True)
@click.option("--assembly-accession", help="ex: GCA_000003055.6", required=True)
@click.option("--release-folder", required=True)
@click.command()
def main(bgzip_path, tabix_path, bcftools_path, vcf_sort_script_path, assembly_accession, release_folder):
    merge_dbsnp_eva_release_files(bgzip_path, tabix_path, bcftools_path, vcf_sort_script_path, assembly_accession, 
                                  release_folder)


if __name__ == "__main__":
    main()

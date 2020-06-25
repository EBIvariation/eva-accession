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

# This script looks up the impact of overweight SNPs

import click
from snpmapinfo_metadata import *


def export_snpmapinfo_for_species(species_info, metadata_connection_handle, export_dir):
    exported_filenames_and_assemblies = []
    species_name = species_info["database_name"]

    weight_criteria_query = "select 'rs'||trim(cast(snp_id as text)) from dbsnp_{0}.{1} where weight > 3"
    copy_statement = "COPY ({0}) TO STDOUT WITH CSV"

    with get_db_conn_for_species(species_info) as species_db_connection_handle:
        snpmapinfo_table_names = get_snpmapinfo_table_names_for_species(species_info)
        for snpmapinfo_table_name in snpmapinfo_table_names:
            distinct_asm, type_of_asm = get_distinct_asm_with_overweight_snps_in_snpmapinfo_table(snpmapinfo_table_name,
                                                                                                  species_info)
            for asm in distinct_asm:
                if type_of_asm == "assembly_name":
                    specific_query = weight_criteria_query + " and assembly = '{0}'".format(asm)
                else:
                    specific_query = weight_criteria_query + " and asm_acc = '{0}' and asm_version = '{1}'"\
                        .format(asm.split(".")[0], asm.split(".")[1])
                specific_query = specific_query.format(species_name, snpmapinfo_table_name) + " order by 1"
                associated_GCA_assembly = lookup_GCA_assembly(species_name, snpmapinfo_table_name, asm,
                                                              metadata_connection_handle)
                output_file_name = os.path.sep.join([export_dir,
                                                     "{0}_{1}_{2}_overweight_snps.csv".format(species_name,
                                                                                              snpmapinfo_table_name,
                                                                                              associated_GCA_assembly)])
                with open(output_file_name, 'w') \
                        as output_file_handle:
                    get_result_cursor(species_db_connection_handle, specific_query)\
                        .copy_expert(copy_statement.format(specific_query), output_file_handle)
                    exported_filenames_and_assemblies += [(output_file_name
                                                           , get_build_version_from_file_name(snpmapinfo_table_name)
                                                           , associated_GCA_assembly)]

    return exported_filenames_and_assemblies


def create_lookup_result_file(species_name, rs_release_base_folder, export_dir, exported_filenames_and_assemblies):
    release_file_suffixes = ["current_ids", "merged_ids", "deprecated_ids", "merged_deprecated_ids"]
    for (exported_file_with_overweight_snps, dbsnp_build, associated_GCA_assembly) in \
            exported_filenames_and_assemblies:
        if os.stat(exported_file_with_overweight_snps).st_size == 0:
            continue
        for release_file_suffix in release_file_suffixes:
            lookup_result_file = os.path.sep.join([export_dir,
                                                   os.path.splitext(
                                                       os.path.basename(exported_file_with_overweight_snps))[0]
                                                   + "_in_{0}.txt".format(release_file_suffix)]
                                                  )

            release_file_to_lookup_against = os.path.sep.join(
                [rs_release_base_folder, associated_GCA_assembly,
                 "{0}_{1}.vcf.gz".format(associated_GCA_assembly,
                                         release_file_suffix)])

            command_to_lookup_overweight_snps_in_release_file = 'bash -c ' \
                                                                '"comm -12 ' \
                                                                '<(zcat \\"{0}\\" | grep -v ^# | cut -f3 | sort | uniq) ' \
                                                                '\\"{1}\\" ' \
                                                                '1> \\"{2}\\""'\
                .format(release_file_to_lookup_against, exported_file_with_overweight_snps, lookup_result_file)
            if release_file_suffix in ["current_ids", "merged_ids"]:
                run_command_with_output(
                    "Overweight SNP impact in {0} {1} for the {2} build {3} assembly"
                        .format(species_name, release_file_suffix, dbsnp_build,
                                associated_GCA_assembly)
                    , command_to_lookup_overweight_snps_in_release_file
                    , return_process_output=True)
                if release_file_suffix == "merged_ids":
                    lookup_result_file_for_merge_id_targets = os.path.sep.join([export_dir,
                                                           os.path.splitext(
                                                               os.path.basename(exported_file_with_overweight_snps))[0]
                                                           + "_in_merge_target_ids.txt".format(release_file_suffix)]
                                                          )
                    # Merged ID release files have the merge target RS IDs in the INFO column with CURR= prefix
                    # Check if overweight SNPs appear in this column
                    command_to_lookup_overweight_snps_in_merge_target = 'bash -c ' \
                                     '"comm -12 ' \
                                     '<(zcat \\"{0}\\" | grep -v ^# | grep -o -E "CURR=rs[0-9]+" | cut -d= -f2 | sort | uniq) ' \
                                     '\\"{1}\\" ' \
                                     '1> \\"{2}\\""'\
                        .format(release_file_to_lookup_against, exported_file_with_overweight_snps,
                                lookup_result_file_for_merge_id_targets)
                    run_command_with_output(
                        "Overweight SNP impact in {0} {1} for the {2} build {3} assembly"
                            .format(species_name, release_file_suffix, dbsnp_build,
                                    associated_GCA_assembly)
                        , command_to_lookup_overweight_snps_in_merge_target
                        , return_process_output=True)
            elif release_file_suffix in ["deprecated_ids", "merged_deprecated_ids"]:
                release_file_to_lookup_against = os.path.sep.join(
                    [rs_release_base_folder, associated_GCA_assembly,
                     "{0}_{1}.txt.gz".format(associated_GCA_assembly,
                                             release_file_suffix)])
                command_to_lookup_overweight_snps_in_release_file = \
                    'bash -c ' \
                    '"comm -12 ' \
                    '<(zcat \\"{0}\\" | cut -f1 | sort | uniq) ' \
                    '\\"{1}\\" ' \
                    '1> \\"{2}\\""'.format(release_file_to_lookup_against,
                                           exported_file_with_overweight_snps,
                                           lookup_result_file)
                run_command_with_output(
                    "Overweight SNP impact in {0} {1} for the {2} build {3} assembly"
                        .format(species_name, release_file_suffix, dbsnp_build,
                                associated_GCA_assembly)
                    , command_to_lookup_overweight_snps_in_release_file
                    , return_process_output=True)


def lookup_overweight_snps_in_release_files(metadata_db_name, metadata_db_user, metadata_db_host, species_name,
                                            export_dir, rs_release_base_folder):
    with get_connection_handle(metadata_db_name, metadata_db_user, metadata_db_host) as metadata_connection_handle:
        for species_info in get_species_info(metadata_connection_handle, species_name):
            try:
                exported_filenames_and_assemblies = export_snpmapinfo_for_species(species_info, metadata_connection_handle,
                                                                                  export_dir)
                create_lookup_result_file(species_info["database_name"], rs_release_base_folder, export_dir,
                                          exported_filenames_and_assemblies)
            except Exception as e:
                logger.error(e)
                pass


@click.option("--metadata-db-name", required=True)
@click.option("--metadata-db-user", required=True)
@click.option("--metadata-db-host", required=True)
@click.option("--species-name", default="all", required=False)
@click.option("--export-dir", required=True)
@click.option("--rs-release-base-folder", required=True)
@click.command()
def main(metadata_db_name, metadata_db_user, metadata_db_host, species_name, export_dir, rs_release_base_folder):
    lookup_overweight_snps_in_release_files(metadata_db_name, metadata_db_user, metadata_db_host, species_name,
                                            export_dir, rs_release_base_folder)


if __name__ == '__main__':
    main()

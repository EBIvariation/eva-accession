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
import datetime

from ebi_eva_internal_pyutils.pg_utils import get_all_results_for_query

release_vcf_file_categories = ["current_ids", "merged_ids"]
release_text_file_categories = ["deprecated_ids", "merged_deprecated_ids"]
vcf_validation_output_file_pattern = "*.vcf.errors_summary.*"
asm_report_output_file_pattern = "*.vcf.text_assembly_report.*"


def update_release_progress_status(metadata_connection_handle, release_species_inventory_table, taxonomy,
                                   assembly_accession, release_version, release_status):
    if release_status == 'Started':
        date_to_change = 'release_start'
    else:
        date_to_change = 'release_end'
    now = datetime.datetime.now().isoformat()
    update_status_query = (
        f"update {release_species_inventory_table} " 
        f"set release_status = '{release_status}', {date_to_change} = '{now}' " 
        f"where taxonomy = {taxonomy} " 
        f"and assembly_accession = '{assembly_accession}' " 
        f"and release_version = {release_version};"
    )
    with metadata_connection_handle.cursor() as cursor:
        cursor.execute(update_status_query)
    metadata_connection_handle.commit()


def get_assemblies_to_import_for_dbsnp_species(metadata_connection_handle, dbsnp_species_taxonomy, release_version):
    query = "select distinct assembly_accession from dbsnp_ensembl_species.release_assemblies " \
            "where release_version='{0}' " \
            "and data_source='dbSNP' and tax_id='{1}'".format(release_version, dbsnp_species_taxonomy)
    results = get_all_results_for_query(metadata_connection_handle, query)
    if len(results) > 0:
        return [result[0] for result in results]
    return []


def get_target_mongo_instance_for_assembly(taxonomy_id, assembly, release_species_inventory_table, release_version,
                                           metadata_connection_handle):
    query = (f"select distinct tempmongo_instance from {release_species_inventory_table} "
             f"where taxonomy = '{taxonomy_id}' and assembly_accession='{assembly}' and "
             f"release_version = {release_version} and should_be_released and num_rs_to_release > 0")
    results = get_all_results_for_query(metadata_connection_handle, query)
    if len(results) == 0:
        raise Exception(f"Could not find target Mongo instance in Embassy "
                        f"for taxonomy {taxonomy_id} and assembly {assembly}")

    return results[0][0]


def get_release_assemblies_for_taxonomy(taxonomy_id, release_species_inventory_table,
                                        release_version, metadata_connection_handle):
    results = get_all_results_for_query(metadata_connection_handle, "select assembly_accession from {0} "
                                                                    "where taxonomy = '{1}' "
                                                                    "and release_version = {2} and should_be_released "
                                                                    "and num_rs_to_release > 0"
                                        .format(release_species_inventory_table, taxonomy_id, release_version))
    if len(results) == 0:
        raise Exception("Could not find assemblies pertaining to taxonomy ID: " + taxonomy_id)
    return [result[0] for result in results]


def get_release_inventory_info_for_assembly(taxonomy_id, assembly_accession, release_species_inventory_table,
                                            release_version, metadata_connection_handle):
    results = get_all_results_for_query(metadata_connection_handle, "select row_to_json(row) from "
                                                                    "(select * from {0} where "
                                                                    "taxonomy = '{1}' "
                                                                    "and assembly_accession = '{2}' "
                                                                    "and release_version = {3} "
                                                                    "and should_be_released "
                                                                    "and num_rs_to_release > 0) row"
                                        .format(release_species_inventory_table, taxonomy_id, assembly_accession,
                                                release_version))
    if len(results) == 0:
        raise Exception("Could not find release inventory pertaining to taxonomy ID: {0} and assembly: {1} "
                        .format(taxonomy_id, assembly_accession))
    return results[0][0]

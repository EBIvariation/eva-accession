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

import os
from dbsnp_mirror_metadata import *


def get_build_version_from_file_name(file_name):
    return os.path.basename(file_name).split("_")[0].lower()


def lookup_GCA_assembly(species_name, snpmapinfo_table_name, asm, metadata_connection_handle):
    query = "select assembly_accession from dbsnp_ensembl_species.EVA2015_snpmapinfo_asm_lookup " \
            "where database_name = '{0}' and snpmapinfo_table_name = '{1}' and assembly = '{2}' " \
            "and assembly_accession <> 'unresolved' and assembly_accession is not null"\
        .format(species_name, snpmapinfo_table_name, asm)
    results = get_all_results_for_query(metadata_connection_handle, query)
    if len(results) == 0:
        raise Exception("Could not look up GCA accession for {0} table for the species {1}"
                        .format(snpmapinfo_table_name, species_name))
    return results[0][0]


# Overweight SNPs - SNPs with mapping weight > threshold
def get_distinct_asm_with_overweight_snps_in_snpmapinfo_table(snpmapinfo_table_name, species_info,
                                                              weight_threshold=1):
    with get_db_conn_for_species(species_info) as species_db_connection_handle:
        asm_columns = get_snpmapinfo_asm_columns(species_info, snpmapinfo_table_name)
        if "asm_acc" in asm_columns:
            distinct_asm_query = "select distinct asm_acc || '.' || asm_version as assembly " \
                                 "from dbsnp_{0}.{1}"
            type_of_asm = "assembly_accession"
        else:
            distinct_asm_query = "select distinct assembly from dbsnp_{0}.{1}"
            type_of_asm = "assembly_name"
        distinct_asm_query += " where weight > {0} and assembly is not null order by 1".format(weight_threshold)
        results = get_all_results_for_query(species_db_connection_handle,
                                            distinct_asm_query.format(species_info["database_name"],
                                                                      snpmapinfo_table_name))
        return [result[0] for result in results], type_of_asm


def get_snpmapinfo_table_names_for_species(species_info):
    query = """select table_name from information_schema.tables 
    where lower(table_name) like '%snpmapinfo%' and table_schema = 'dbsnp_{0}'
    """
    with get_db_conn_for_species(species_info) as pg_conn_for_species:
        results = get_all_results_for_query(pg_conn_for_species, query.format(species_info["database_name"]))
        if len(results) > 0:
            if len(results[0]) > 0:
                return [result[0] for result in results]

    return []


def get_snpmapinfo_asm_columns(species_info, snpmapinfo_table_name):
    accession_columns = ['asm_acc', 'asm_version']
    assembly_name_columns = ['assembly']
    with get_db_conn_for_species(species_info) as pg_conn_for_species:
        accession_columns_query = "select distinct table_schema from information_schema.columns " \
                                  "where table_schema = 'dbsnp_{0}' and table_name = '{1}' " \
                                  "and column_name in ({2});".format(species_info["database_name"],
                                                                     snpmapinfo_table_name,
                                                                     ",".join(["'{0}'".format(name)
                                                                               for name in accession_columns]))
        assembly_name_columns_query = "select distinct table_schema from information_schema.columns " \
                                      "where table_schema = 'dbsnp_{0}' and table_name = '{1}' " \
                                      "and column_name in ({2});".format(species_info["database_name"],
                                                                         snpmapinfo_table_name,
                                                                         ",".join(["'{0}'".format(name)
                                                                                   for name in assembly_name_columns]))
        available_column_names = get_all_results_for_query(pg_conn_for_species, accession_columns_query)
        if len(available_column_names) > 0:
            return [accession_columns[0], accession_columns[1]]

        available_column_names = get_all_results_for_query(pg_conn_for_species, assembly_name_columns_query)
        if len(available_column_names) > 0:
            return [assembly_name_columns[0]]
        else:
            logger.error("No assembly related column names found for the table dbsnp_{0}.{1}"
                         .format(species_info["database_name"], snpmapinfo_table_name))
            return []

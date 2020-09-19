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
import logging
import sys
import traceback
from include_mapping_weight_from_dbsnp.snpmapinfo_metadata import \
    get_snpmapinfo_tables_with_overweight_snps_for_dbsnp_species, get_snpmapinfo_asm_columns
from include_mapping_weight_from_dbsnp.dbsnp_mirror_metadata import get_db_conn_for_species, get_species_info
from ebi_eva_common_pyutils.pg_utils import get_pg_connection_handle, execute_query, create_index_on_table, \
    vacuum_analyze_table


logger = logging.getLogger(__name__)


def create_multimap_snp_table_and_indices(metadata_connection_handle, dbsnp_species_name, species_info):
    union_of_snpmapinfo_tables_query = " union all ".join(
        ["select snp_id, weight, {0} as assembly from dbsnp_{1}.{2} where weight > 1".format(
            "||'.'||".join(get_snpmapinfo_asm_columns(species_info, table_name)), dbsnp_species_name, table_name)
            for table_name in
            get_snpmapinfo_tables_with_overweight_snps_for_dbsnp_species(metadata_connection_handle,
                                                                         dbsnp_species_name)])
    if len(union_of_snpmapinfo_tables_query) > 0:
        multimap_snp_table_name = "multimap_snps"
        table_creation_query = """            
                create table if not exists dbsnp_{0}.{1} as (select distinct * from ({2}) temp);
                """.format(dbsnp_species_name, multimap_snp_table_name, union_of_snpmapinfo_tables_query)
        with get_db_conn_for_species(species_info) as species_connection_handle:
            logger.info("Executing query: " + table_creation_query)
            execute_query(species_connection_handle, table_creation_query)
            for column in ["snp_id", "weight", "assembly"]:
                create_index_on_table(species_connection_handle, "dbsnp_" + dbsnp_species_name, multimap_snp_table_name,
                                      [column])
                vacuum_analyze_table(species_connection_handle, "dbsnp_" + dbsnp_species_name, multimap_snp_table_name,
                                     [column])
                execute_query(species_connection_handle,
                              "grant select on dbsnp_{0}.{1} to dbsnp_ro".format(dbsnp_species_name,
                                                                                 multimap_snp_table_name))


def create_table_for_multimap_snps(metadata_db_name, metadata_db_user, metadata_db_host, dbsnp_species_name):
    with get_pg_connection_handle(metadata_db_name, metadata_db_user, metadata_db_host) as \
            metadata_connection_handle:
        species_info = get_species_info(metadata_connection_handle, dbsnp_species_name)[0]
        create_multimap_snp_table_and_indices(metadata_connection_handle, dbsnp_species_name, species_info)


@click.option("--metadata-db-name", required=True)
@click.option("--metadata-db-user", required=True)
@click.option("--metadata-db-host", required=True)
@click.option("--dbsnp-species-name", help="ex: cow_9913", required=True)
@click.command()
def main(metadata_db_name, metadata_db_user, metadata_db_host, dbsnp_species_name):
    try:
        create_table_for_multimap_snps(metadata_db_name, metadata_db_user, metadata_db_host, dbsnp_species_name)
    except Exception as ex:
        logger.error("Encountered an error while creating multimap SNP table for " + dbsnp_species_name + "\n" +
                     "\n".join(traceback.format_exception(type(ex), ex, ex.__traceback__)))
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()

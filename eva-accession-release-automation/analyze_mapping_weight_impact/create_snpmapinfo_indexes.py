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
from snpmapinfo_metadata import *


def get_snpmapinfo_tables_without_indexes(species_info):
    query = """
    select distinct table_name from information_schema.tables where table_schema = 'dbsnp_{0}' and table_name like '{1}'
    except
    select distinct tablename from pg_catalog.pg_indexes where schemaname = 'dbsnp_{0}' 
        and tablename like '{1}' 
        and (indexname like '%_snpmapinfo_weight_idx' or indexname like '%_snpmapinfo_asm_acc_idx' 
                or indexname like '%_snpmapinfo_asm_version_idx' or indexname like '%_snpmapinfo_assembly_idx')
    """.format(species_info["database_name"], "b%snpmapinfo%")
    with get_db_conn_for_species(species_info) as species_connection_handle:
        results = get_all_results_for_query(species_connection_handle, query)
        return [result[0] for result in results]


def create_snpmapinfo_indexes(metadata_db_name, metadata_db_user, metadata_db_host):
    with get_connection_handle(metadata_db_name, metadata_db_user, metadata_db_host) as metadata_connection_handle:
        for species_info in get_species_info(metadata_connection_handle):
            for snpmapinfo_table_name in get_snpmapinfo_tables_without_indexes(species_info):
                if "asm_acc" in get_snpmapinfo_asm_columns(species_info, snpmapinfo_table_name):
                    queries = ["{0} dbsnp_{1}.{2} (asm_acc)",
                               "{0} dbsnp_{1}.{2} (asm_version)"]
                else:
                    queries = ["{0} dbsnp_{1}.{2} (assembly)"]

                queries += ["{0} dbsnp_{1}.{2} (weight)"]
                with get_db_conn_for_species(species_info) as species_connection_handle:
                    for query in queries:
                        index_creation_query_to_execute = query.format("create index on",
                                                                       species_info["database_name"],
                                                                       snpmapinfo_table_name)
                        logger.info("Building index with query: " + index_creation_query_to_execute)
                        execute_query(species_connection_handle, index_creation_query_to_execute)
                        species_connection_handle.commit()
                    for query in queries:
                        # This is needed for vacuum analyze to work since it can't work inside transactions!
                        species_connection_handle.set_isolation_level(0)
                        analyze_query_to_execute = query.format("vacuum analyze",
                                                                species_info["database_name"],
                                                                snpmapinfo_table_name)
                        logger.info("Vacuum analyze with query: " + analyze_query_to_execute)
                        execute_query(species_connection_handle, analyze_query_to_execute)


@click.option("--metadata-db-name", required=True)
@click.option("--metadata-db-user", required=True)
@click.option("--metadata-db-host", required=True)
@click.command()
def main(metadata_db_name, metadata_db_user, metadata_db_host):
    create_snpmapinfo_indexes(metadata_db_name, metadata_db_user, metadata_db_host)


if __name__ == '__main__':
    main()

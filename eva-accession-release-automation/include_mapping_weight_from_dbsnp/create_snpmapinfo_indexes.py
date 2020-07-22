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

from ebi_eva_common_pyutils.pg_utils import get_pg_connection_handle, create_index_on_table, vacuum_analyze_table
from include_mapping_weight_from_dbsnp.snpmapinfo_metadata import get_snpmapinfo_asm_columns, \
    get_snpmapinfo_table_names_for_species
from include_mapping_weight_from_dbsnp.dbsnp_mirror_metadata import get_species_info, get_db_conn_for_species


def create_snpmapinfo_indexes(metadata_db_name, metadata_db_user, metadata_db_host):
    with get_pg_connection_handle(metadata_db_name, metadata_db_user, metadata_db_host) as metadata_connection_handle:
        for species_info in get_species_info(metadata_connection_handle):
            for snpmapinfo_table_name in get_snpmapinfo_table_names_for_species(species_info):
                if "asm_acc" in get_snpmapinfo_asm_columns(species_info, snpmapinfo_table_name):
                    index_columns = [("asm_acc", "asm_version")]
                else:
                    index_columns = [("assembly",)]

                index_columns += [("weight",)]
                with get_db_conn_for_species(species_info) as species_connection_handle:
                    for columns in index_columns:
                        create_index_on_table(species_connection_handle, "dbsnp_" + species_info["database_name"],
                                              snpmapinfo_table_name, columns)
                        vacuum_analyze_table(species_connection_handle, "dbsnp_" + species_info["database_name"],
                                             snpmapinfo_table_name, columns)


@click.option("--metadata-db-name", required=True)
@click.option("--metadata-db-user", required=True)
@click.option("--metadata-db-host", required=True)
@click.command()
def main(metadata_db_name, metadata_db_user, metadata_db_host):
    create_snpmapinfo_indexes(metadata_db_name, metadata_db_user, metadata_db_host)


if __name__ == '__main__':
    main()

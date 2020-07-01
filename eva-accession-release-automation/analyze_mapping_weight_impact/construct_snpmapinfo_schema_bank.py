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


# This script constructs a "schema bank" for various SNPMapInfo schemas
# because NCBI dumps have different schemas across different build versions.
# This data can then be used to match a SNPMapInfo file (for which we don't have an explicit schema from NCBI)
# with a schema based on the number of columns.
# The schema bank dbsnp_ensembl_species.snpmapinfo_schemas_all_species has the following format:
# +---------------+-----------------------+------------------------------+-------------------+
# | database_name | snpmapinfo_table_name | snpmapinfo_schema_definition | number_of_columns |
# +---------------+-----------------------+------------------------------+-------------------+
# | apple_3750    | b149_snpmapinfo       | snp_type\nsnp_id\nweight     |                 3 |
# +---------------+-----------------------+------------------------------+-------------------+

from snpmapinfo_metadata import *
import click


# For a given build (ex: dbsnp_129), add the SNPMapInfo schemas in that build to the schema bank
def add_snpmapinfo_schemas_in_build_to_schema_bank(dbsnp_mirror_db_info, metadata_connection_handle):
    with get_db_conn_for_species(dbsnp_mirror_db_info) as dbsnp_build_info_conn:
        query_to_get_snpmapinfo_tables = "select table_schema, table_name as full_table_name " \
                                         "from information_schema.tables " \
                                         "where lower(table_name) like 'b%snpmapinfo%' " \
                                            "and lower(table_schema) not like '%donotuse' " \
                                            "and lower(table_name) not like '%donotuse'"
        for result in get_all_results_for_query(dbsnp_build_info_conn, query_to_get_snpmapinfo_tables):
            snpmapinfo_table_schema, snpmapinfo_table_name = result[0], result[1]
            command_to_get_snpmap_table_definition = \
                ("psql -AF ' ' -t -U dbsnp -h {0} -p {1} -d dbsnp_{2} -v ON_ERROR_STOP=1 -c " + '"\\d {3}.{4}"')\
                .format(dbsnp_mirror_db_info["pg_host"], dbsnp_mirror_db_info["pg_port"],
                        dbsnp_mirror_db_info["dbsnp_build"], snpmapinfo_table_schema, snpmapinfo_table_name)
            snpmapinfo_table_definition = run_command_with_output("Get SNPMapInfo table definition for "
                                                                  + snpmapinfo_table_name,
                                                                  command_to_get_snpmap_table_definition,
                                                                  return_process_output=True)
            insert_into_snpmapinfo_schema_bank(snpmapinfo_table_schema, snpmapinfo_table_name,
                                               snpmapinfo_table_definition.strip(), metadata_connection_handle)


def insert_into_snpmapinfo_schema_bank(snpmapinfo_table_schema, snpmapinfo_table_name, snpmapinfo_table_definition,
                                       metadata_connection_handle):
    # Ignore load_order column since this was an extra column added while debugging the dbSNP import process
    snpmapinfo_table_definition_lines = list(map(str.strip,
                                                 filter(lambda column_name:
                                                        not column_name.strip().lower().startswith("load_order")
                                                        and not column_name == "\n",
                                                        snpmapinfo_table_definition.split("\n"))
                                                 ))
    with metadata_connection_handle.cursor() as metadata_cursor:
        insert_statement = "insert into dbsnp_ensembl_species.snpmapinfo_schemas_all_species " \
                           "select * from " \
                           "(select cast('{0}' as text) as added_schema, cast('{1}' as text) as added_table, " \
                            "cast('{2}' as text), {3}) temp " \
                           "where (temp.added_schema, temp.added_table) not in " \
                           "(select dbsnp_database_name, snpmapinfo_table_name " \
                            "from dbsnp_ensembl_species.snpmapinfo_schemas_all_species)"\
                            .format(snpmapinfo_table_schema, snpmapinfo_table_name,
                                    ",".join(snpmapinfo_table_definition_lines),
                                    len(snpmapinfo_table_definition_lines))
        metadata_cursor.execute(insert_statement)
        metadata_cursor.connection.commit()


def construct_snpmapinfo_schema_bank(metadata_db_name, metadata_db_user, metadata_db_host):
    with get_connection_handle(metadata_db_name, metadata_db_user, metadata_db_host) as metadata_connection_handle:
        for dbsnp_mirror_info in get_dbsnp_mirror_db_info(metadata_db_name, metadata_db_user, metadata_db_host):
            add_snpmapinfo_schemas_in_build_to_schema_bank(dbsnp_mirror_info, metadata_connection_handle)


@click.option("--metadata-db-name", required=True)
@click.option("--metadata-db-user", required=True)
@click.option("--metadata-db-host", required=True)
@click.command()
def main(metadata_db_name, metadata_db_user, metadata_db_host):
    construct_snpmapinfo_schema_bank(metadata_db_name, metadata_db_user, metadata_db_host)


if __name__ == '__main__':
    main()

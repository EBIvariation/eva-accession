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


# This script attempts to match a DDL from the "schema bank" (see construct_snpmapinfo_schema_bank.py)
# against the NCBI SNPMapInfo dumps for each species and if a match is found, attempt to import it

import click
import glob
import gzip
from snpmapinfo_metadata import *


# dbsnp_data_source_base: NFS path to the root directory where dbSNP SQL dumps are stored
def get_unimported_snpmapinfo_files_for_species(species_info, dbsnp_data_source_base):
    unimported_snpmapinfo_tables_for_species = []
    snpmapinfo_imported_builds_for_species = [table_name.split("_")[0].lower() for table_name in
                                              get_snpmapinfo_table_names_for_species(species_info)]
    # NOTE: Assumption using the glob pattern: all the SNPMapInfo tables start are in the format b{build}_SNPMapInfo.gz.
    # Verified with the command below:
    # find $DBSNP_DATA_SOURCE_BASE/build* -iname '*snpmapinfo*.gz' 2> /dev/null | cut -d/ -f10 | cut -d_ -f1 | sort | uniq
    glob_pattern = os.path.join(dbsnp_data_source_base, "build_" + species_info["dbsnp_build"],
                                     species_info["database_name"], "data",
                                     "b*{0}*.gz".format(
                                         "".join(["[{0}{1}]".format(char, char.upper()) for char in "snpmapinfo"]))
                                )
    # Get all the SNPMapInfo files for all the builds in a given species from the dbSNP SQL dump directory
    snpmapinfo_all_files_for_species = glob.glob(glob_pattern)
    for snpmapinfo_file_name in snpmapinfo_all_files_for_species:
        if get_build_version_from_file_name(snpmapinfo_file_name) not in snpmapinfo_imported_builds_for_species:
            unimported_snpmapinfo_tables_for_species.append(snpmapinfo_file_name)

    return unimported_snpmapinfo_tables_for_species


def attempt_snpmap_import_with_ddl(snpmapinfo_file_name, snpmapinfo_table_ddl, species_info):
    with get_db_conn_for_species(species_info) as species_data_connection_handle:
        snpmapinfo_table_name = "dbsnp_{0}.{1}_snpmapinfo".format(species_info["database_name"],
                                                                  get_build_version_from_file_name(snpmapinfo_file_name)
                                                                  )
        snpmapinfo_table_creation_query = "create table {0} ({1})".format(snpmapinfo_table_name,
                                                                          snpmapinfo_table_ddl)
        snpmapinfo_table_drop_query = "drop table {0}".format(snpmapinfo_table_name)
        with species_data_connection_handle.cursor() as cursor, gzip.open(snpmapinfo_file_name) \
                as snpmapinfo_file_handle:
            try:
                cursor.execute(snpmapinfo_table_creation_query)
                species_data_connection_handle.commit()
                cursor.copy_from(snpmapinfo_file_handle, snpmapinfo_table_name, sep="\t", null="")
            except (psycopg2.DataError, psycopg2.ProgrammingError) as error:
                logger.error(error)
                species_data_connection_handle.rollback()
                cursor.execute(snpmapinfo_table_drop_query)
                species_data_connection_handle.commit()
                return False
            return True


def attempt_matching_ddl_for_snpmapinfo_file(snpmapinfo_file_name, species_info, metadata_connection_handle):
    with gzip.open(snpmapinfo_file_name) as snpmapinfo_file_handle:
        number_of_columns_in_file = len(snpmapinfo_file_handle.readline().decode("utf-8").split("\t"))
        query_to_get_matching_ddl = "select distinct snpmapinfo_schema_definition from " \
                                       "dbsnp_ensembl_species.snpmapinfo_schemas_all_species " \
                                       "where number_of_columns = {0} order by snpmapinfo_schema_definition"\
            .format(number_of_columns_in_file)

        snpmap_import_successful = False
        for result in get_all_results_for_query(metadata_connection_handle,
                                                query_to_get_matching_ddl):
            snpmapinfo_table_ddl = result[0]
            snpmap_import_successful = attempt_snpmap_import_with_ddl(snpmapinfo_file_name, snpmapinfo_table_ddl,
                                                                      species_info)
            if snpmap_import_successful:
                return

        if not snpmap_import_successful:
            logger.error("No matching SNPMapInfo schema found for the file: " + snpmapinfo_file_name)


# dbsnp_data_source_base: NFS path to the root directory where dbSNP SQL dumps are stored
def import_unimported_snpmapinfo_tables_for_species(species_info, metadata_connection_handle, dbsnp_data_source_base):
    for file_name in get_unimported_snpmapinfo_files_for_species(species_info, dbsnp_data_source_base):
        logger.info("Attempting import of: " + file_name)
        attempt_matching_ddl_for_snpmapinfo_file(file_name, species_info,  metadata_connection_handle)


def import_missing_snpmapinfo_tables(metadata_db_name, metadata_db_user, metadata_db_host, dbsnp_data_source_base):
    metadata_connection_handle = get_connection_handle(metadata_db_name, metadata_db_user, metadata_db_host)
    for species_info in get_species_info(metadata_connection_handle):
        logger.info("Detecting unimported tables for species: " + species_info["database_name"])
        import_unimported_snpmapinfo_tables_for_species(species_info, metadata_connection_handle,
                                                        dbsnp_data_source_base)


@click.option("--metadata-db-name", required=True)
@click.option("--metadata-db-user", required=True)
@click.option("--metadata-db-host", required=True)
@click.option("--dbsnp-data-source-base", required=True)
@click.command()
def main(metadata_db_name, metadata_db_user, metadata_db_host, dbsnp_data_source_base):
    import_missing_snpmapinfo_tables(metadata_db_name, metadata_db_user, metadata_db_host, dbsnp_data_source_base)


if __name__ == '__main__':
    main()

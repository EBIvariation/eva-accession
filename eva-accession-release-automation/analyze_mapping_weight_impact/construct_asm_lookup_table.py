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


# This script creates a look up table that maps the GCF assembly (or) assembly name in the SNPMapInfo tables
# to the correct GCA accession
import click

from pg_query_utils import execute_query, get_pg_connection_handle
from snpmapinfo_metadata import *
from __init__ import *


def create_asm_lookup_table(metadata_connection_handle, asm_lookup_table_name):
    asm_lookup_table_creation_query = "create table if not exists " \
                                      "{0} " \
                                      "(database_name text, snpmapinfo_table_name text, " \
                                      "assembly text, assembly_accession text, " \
                                      "constraint unique_constraint unique(database_name, snpmapinfo_table_name, " \
                                        "assembly))".format(asm_lookup_table_name)
    execute_query(metadata_connection_handle, asm_lookup_table_creation_query)
    metadata_connection_handle.commit()


def resolve_asm_to_GCA_accession(asm, type_of_asm):
    try:
        if type_of_asm == "assembly_name":
            resolved_GCA_accession = resolve_assembly_name_to_GCA_accession(asm)
        else:
            resolved_GCA_accession = retrieve_genbank_equivalent_for_GCF_accession(asm)
    except Exception:
        resolved_GCA_accession = "unresolved"
    return resolved_GCA_accession


def construct_asm_lookup_table(metadata_db_name, metadata_db_user, metadata_db_host):
    with get_pg_connection_handle(metadata_db_name, metadata_db_user, metadata_db_host) as metadata_connection_handle:
        asm_lookup_table_name = "dbsnp_ensembl_species.EVA2015_snpmapinfo_asm_lookup"
        create_asm_lookup_table(metadata_connection_handle, asm_lookup_table_name)
        for species_info in get_species_info(metadata_connection_handle):
            for snpmapinfo_table_name in get_snpmapinfo_table_names_for_species(species_info):
                distinct_asm, type_of_asm = get_distinct_asm_with_overweight_snps_in_snpmapinfo_table\
                    (snpmapinfo_table_name, species_info)
                for asm in distinct_asm:
                    resolved_GCA_accession = resolve_asm_to_GCA_accession(asm, type_of_asm)
                    query = "insert into {0} values ('{1}', '{2}', '{3}', '{4}') " \
                            "on conflict on constraint unique_constraint do nothing"\
                        .format(asm_lookup_table_name
                                , species_info["database_name"]
                                , snpmapinfo_table_name
                                , asm
                                , resolved_GCA_accession)
                    logger.info("Executing query: " + query)
                    execute_query(metadata_connection_handle, query)
                    metadata_connection_handle.commit()


@click.option("--metadata-db-name", required=True)
@click.option("--metadata-db-user", required=True)
@click.option("--metadata-db-host", required=True)
@click.command()
def main(metadata_db_name, metadata_db_user, metadata_db_host):
    construct_asm_lookup_table(metadata_db_name, metadata_db_user, metadata_db_host)


if __name__ == '__main__':
    main()

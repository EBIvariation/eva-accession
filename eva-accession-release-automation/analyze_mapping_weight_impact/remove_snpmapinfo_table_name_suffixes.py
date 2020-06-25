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

# This script removes unnecessary suffixes like "_0_0" in SNPMapInfo table names
import click
from snpmapinfo_metadata import *


def remove_snpmapinfo_table_name_suffixes(metadata_db_name, metadata_db_user, metadata_db_host):
    with get_connection_handle(metadata_db_name, metadata_db_user, metadata_db_host) as metadata_connection_handle:
        for species_info in get_species_info(metadata_connection_handle):
            for snpmapinfo_table_name in get_snpmapinfo_table_names_for_species(species_info):
                table_name_components = list(filter(lambda x: x.strip() != "",
                                                    snpmapinfo_table_name.lower().split("snpmapinfo"))
                                             )
                if len(table_name_components) > 1:
                    with get_db_conn_for_species(species_info) as species_db_connection_handle:
                        rename_query = "alter table dbsnp_{0}.{1} rename to {2}"\
                            .format(species_info["database_name"], snpmapinfo_table_name,
                                    "".join(table_name_components[:-1]) + "snpmapinfo")
                        logger.info("Running rename query: " + rename_query)
                        execute_query(species_db_connection_handle, rename_query)
                        species_db_connection_handle.commit()


@click.option("--metadata-db-name", required=True)
@click.option("--metadata-db-user", required=True)
@click.option("--metadata-db-host", required=True)
@click.command()
def main(metadata_db_name, metadata_db_user, metadata_db_host):
    remove_snpmapinfo_table_name_suffixes(metadata_db_name, metadata_db_user, metadata_db_host)


if __name__ == '__main__':
    main()

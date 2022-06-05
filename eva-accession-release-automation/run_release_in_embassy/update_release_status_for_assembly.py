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
import psycopg2


from run_release_in_embassy.release_metadata import update_release_progress_status
from ebi_eva_common_pyutils.config_utils import get_pg_metadata_uri_for_eva_profile


logger = logging.getLogger(__name__)


def update_release_status_for_assembly(private_config_xml_file, profile, release_species_inventory_table, taxonomy_id,
                                       assembly_accession, release_version):
    with psycopg2.connect(get_pg_metadata_uri_for_eva_profile(profile, private_config_xml_file),
                          user="evapro") as metadata_connection_handle:
        update_release_progress_status(metadata_connection_handle, release_species_inventory_table,
                                       taxonomy_id, assembly_accession, release_version,
                                       release_status='Completed')
        logger.info("Successfully marked release status as 'Completed' in {0} for taxonomy {1} and assembly {2}"
                    .format(release_species_inventory_table, taxonomy_id, assembly_accession))


@click.option("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
@click.option("--profile", help="Maven profile to use, ex: internal", required=True)
@click.option("--release-species-inventory-table", help="Name of release inventory table", required=True)
@click.option("--taxonomy-id", help="ex: 9913", required=True)
@click.option("--assembly-accession", help="ex: GCA_000003055.6", required=True)
@click.option("--release-version", help="ex: 2", type=int, required=True)
@click.command()
def main(private_config_xml_file, profile, release_species_inventory_table, taxonomy_id, assembly_accession, release_version):
    update_release_status_for_assembly(private_config_xml_file, profile, release_species_inventory_table, taxonomy_id,
                                       assembly_accession, release_version)


if __name__ == "__main__":
    main()

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


from run_release_in_embassy.release_metadata import update_release_progress_status
from ebi_eva_internal_pyutils.metadata_utils import get_metadata_connection_handle


logger = logging.getLogger(__name__)


def initiate_release_status_for_assembly(private_config_xml_file, profile, release_species_inventory_table,
                                         taxonomy_id, assembly_accession, release_version):
    with get_metadata_connection_handle(profile, private_config_xml_file) as metadata_connection_handle:
        update_release_progress_status(metadata_connection_handle, release_species_inventory_table,
                                       taxonomy_id, assembly_accession, release_version,
                                       release_status='Started')
        logger.info("Initiate release status as 'Started' in {0} for taxonomy {1} and assembly {2}"
                    .format(release_species_inventory_table, taxonomy_id, assembly_accession))


@click.option("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
@click.option("--profile", help="Maven profile to use, ex: internal", required=True)
@click.option("--release-species-inventory-table", help="Name of release inventory table", required=True)
@click.option("--taxonomy-id", help="ex: 9913", required=True)
@click.option("--assembly-accession", help="ex: GCA_000003055.6", required=True)
@click.option("--release-version", help="ex: 2", type=int, required=True)
@click.command()
def main(private_config_xml_file, profile, release_species_inventory_table, taxonomy_id, assembly_accession,
         release_version):
    initiate_release_status_for_assembly(private_config_xml_file, profile, release_species_inventory_table,
                                         taxonomy_id, assembly_accession, release_version)


if __name__ == "__main__":
    main()

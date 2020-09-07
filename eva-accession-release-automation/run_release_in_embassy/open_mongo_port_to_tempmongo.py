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
import sys
import traceback

from run_release_in_embassy.release_metadata import get_target_mongo_instance_for_taxonomy
from ebi_eva_common_pyutils.config_utils import get_pg_metadata_uri_for_eva_profile
from ebi_eva_common_pyutils.network_utils import get_available_local_port, forward_remote_port_to_local_port

logger = logging.getLogger(__name__)


def open_mongo_port_to_tempmongo(private_config_xml_file, taxonomy_id, release_species_inventory_table):
    MONGO_PORT = 27017
    local_forwarded_port = get_available_local_port(MONGO_PORT)
    exit_code = 0
    try:
        with psycopg2.connect(get_pg_metadata_uri_for_eva_profile("development", private_config_xml_file),
                              user="evadev") as \
                metadata_connection_handle:
            tempmongo_instance = get_target_mongo_instance_for_taxonomy(taxonomy_id, release_species_inventory_table,
                                                                        metadata_connection_handle)
            logger.info("Forwarding remote MongoDB port 27017 to local port {0}...".format(local_forwarded_port))
            port_forwarding_process_id = forward_remote_port_to_local_port(tempmongo_instance, MONGO_PORT,
                                                                           local_forwarded_port)
            open("tempmongo_instance_{0}.info".format(taxonomy_id), "w").write(
                "{0}\t{1}".format(port_forwarding_process_id,
                                  local_forwarded_port))
    except Exception:
        logger.error("Encountered an error while opening a port to the remote MongoDB instance: "
                     + tempmongo_instance + "\n" + traceback.format_exc())
        sys.exit(-1)
    sys.exit(exit_code)


@click.option("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
@click.option("--release-species-inventory-table", default="dbsnp_ensembl_species.release_species_inventory",
              required=False)
@click.option("--taxonomy-id", help="ex: 9913", required=True)
@click.command()
def main(private_config_xml_file, taxonomy_id, release_species_inventory_table):
    open_mongo_port_to_tempmongo(private_config_xml_file, taxonomy_id, release_species_inventory_table)


if __name__ == "__main__":
    main()

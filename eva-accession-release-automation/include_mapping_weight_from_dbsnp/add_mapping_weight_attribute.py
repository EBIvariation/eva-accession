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
import traceback

import click
import logging
import psycopg2
import sys
from pymongo.uri_parser import parse_uri
from run_release_in_embassy.release_metadata import get_assemblies_to_import_for_dbsnp_species
from ebi_eva_common_pyutils.config_utils import get_mongo_uri_for_eva_profile, get_pg_metadata_uri_for_eva_profile
from include_mapping_weight_from_dbsnp.incorporate_mapping_weight_into_accessioning import \
    incorporate_mapping_weight_into_accessioning

logger = logging.getLogger(__name__)


def import_mapping_weight_attribute_for_dbsnp_species(private_config_xml_file, metadata_connection_handle,
                                                      dbsnp_species_taxonomy):
    metadata_params = metadata_connection_handle.get_dsn_parameters()
    mongo_params = parse_uri(get_mongo_uri_for_eva_profile("production", private_config_xml_file))
    # nodelist is in format: [(host1,port1), (host2,port2)]. Just choose one.
    # Mongo is smart enough to fallback to secondaries automatically.
    mongo_host = mongo_params["nodelist"][0][0]
    for assembly in get_assemblies_to_import_for_dbsnp_species(metadata_connection_handle,
                                                               dbsnp_species_taxonomy, release_version=2):

        incorporate_mapping_weight_into_accessioning(metadata_params["dbname"], metadata_params["user"],
                                                     metadata_params["host"],
                                                     mongo_params["username"], mongo_params["password"], mongo_host,
                                                     assembly)


def add_mapping_weight_attribute_for_dbsnp_species(private_config_xml_file, dbsnp_species_name):
    try:
        with psycopg2.connect(get_pg_metadata_uri_for_eva_profile("production", private_config_xml_file),
                              user="evapro") as metadata_connection_handle:
            dbsnp_species_taxonomy = int(dbsnp_species_name.split("_")[-1])
            import_mapping_weight_attribute_for_dbsnp_species(private_config_xml_file, metadata_connection_handle,
                                                              dbsnp_species_taxonomy)
    except Exception as ex:
        logger.error("Encountered an error while adding mapping attribute for " + dbsnp_species_name + "\n" +
                     "\n".join(traceback.format_exception(type(ex), ex, ex.__traceback__)))
        sys.exit(1)

    sys.exit(0)


@click.option("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
@click.option("--dbsnp-species-name", help="ex: cow_9913", required=False)
@click.command()
def main(private_config_xml_file, dbsnp_species_name):
    add_mapping_weight_attribute_for_dbsnp_species(private_config_xml_file, dbsnp_species_name)


if __name__ == "__main__":
    main()

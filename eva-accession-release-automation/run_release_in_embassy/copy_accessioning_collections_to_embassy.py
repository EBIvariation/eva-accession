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
import shutil
import sys
import traceback

from ebi_eva_common_pyutils.config_utils import get_mongo_uri_for_eva_profile
from ebi_eva_common_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_common_pyutils.mongo_utils import copy_db
from pymongo import MongoClient
from pymongo.uri_parser import parse_uri
from run_release_in_embassy.release_common_utils import open_mongo_port_to_tempmongo, close_mongo_port_to_tempmongo, \
    get_release_db_name_in_tempmongo_instance
from run_release_in_embassy.release_metadata import get_release_inventory_info_for_assembly


logger = logging.getLogger(__name__)
collections_assembly_attribute_map = {
    "dbsnpSubmittedVariantEntity": "seq",
    "dbsnpSubmittedVariantOperationEntity": "inactiveObjects.seq",
    "submittedVariantEntity": "seq",
    "submittedVariantOperationEntity": "inactiveObjects.seq",
    "dbsnpClusteredVariantEntity": "asm",
    "dbsnpClusteredVariantOperationEntity": "inactiveObjects.asm",
    "clusteredVariantEntity": "asm",
    "clusteredVariantOperationEntity": "inactiveObjects.asm"
}


def mongo_data_copy_to_remote_host(local_forwarded_port, private_config_xml_file, profile, assembly_accession,
                                   collections_to_copy_map, dump_dir, destination_db_name):
    mongo_params = parse_uri(get_mongo_uri_for_eva_profile(profile, private_config_xml_file))
    # nodelist is in format: [(host1,port1), (host2,port2)]. Just choose one.
    # Mongo is smart enough to fallback to secondaries automatically.
    mongo_host = mongo_params["nodelist"][0][0]
    logger.info("Beginning data copy for assembly: " + assembly_accession)
    dump_output_dir = "{0}/dump_{1}".format(dump_dir, assembly_accession.replace(".", "_"))

    # To be idempotent, clear source dump files
    shutil.rmtree(dump_output_dir, ignore_errors=True)

    for collection, collection_assembly_attribute_name in sorted(collections_to_copy_map.items()):
        logger.info("Begin processing collection: " + collection)
        # Curly braces when they are not positional parameters
        query = """'{{"{0}": {{"$in":["{1}"]}}}}'""".format(collection_assembly_attribute_name, assembly_accession)
        sharded_db_name = "eva_accession_sharded"
        mongodump_args = {"db": sharded_db_name, "host": mongo_host,
                          "username": mongo_params["username"], "password": mongo_params["password"],
                          "authenticationDatabase": "admin", "collection": collection,
                          "query": query, "out": dump_output_dir
                          }
        mongorestore_args = {"db": destination_db_name,
                             "dir": "{0}/{1}/{2}.bson".format(dump_output_dir, sharded_db_name, collection),
                             "collection": collection, "port": local_forwarded_port}
        logger.info("Running export to {0} with query {1} against {2}.{3} in {4}"
                    .format(dump_output_dir, query, sharded_db_name, collection, profile))
        copy_db(mongodump_args, mongorestore_args)


def get_collections_to_copy(collections_to_copy, sources="EVA,DBSNP"):
    collections_to_copy_map = collections_assembly_attribute_map
    if collections_to_copy:
        collections_to_copy_map = {key: value for (key, value) in collections_assembly_attribute_map.items()
                                   if key in collections_to_copy}
    if "dbsnp" not in sources.lower():
        collections_to_copy_map = dict(filter(lambda key_value: not key_value[0].startswith("dbsnp"),
                                              collections_to_copy_map.items()))
    if "eva" not in sources.lower():
        collections_to_copy_map = dict(filter(lambda key_value: key_value[0].startswith("dbsnp"),
                                              collections_to_copy_map.items()))
    return collections_to_copy_map


def copy_accessioning_collections_to_embassy(private_config_xml_file, profile, taxonomy_id, assembly_accession,
                                             collections_to_copy, release_species_inventory_table, release_version,
                                             dump_dir):
    port_forwarding_process_id, mongo_port, exit_code = None, None, -1
    try:
        port_forwarding_process_id, mongo_port = open_mongo_port_to_tempmongo(private_config_xml_file, profile, taxonomy_id,
                                                                              assembly_accession, release_species_inventory_table,
                                                                              release_version)
        with get_metadata_connection_handle(profile, private_config_xml_file) as metadata_connection_handle:
            # To be idempotent, clear destination tempmongo database
            destination_db_name = get_release_db_name_in_tempmongo_instance(taxonomy_id, assembly_accession)
            MongoClient(port=mongo_port).drop_database(destination_db_name)

            release_info = get_release_inventory_info_for_assembly(taxonomy_id, assembly_accession,
                                                                   release_species_inventory_table,
                                                                   release_version, metadata_connection_handle)
            logger.info("Beginning data copy to remote MongoDB host {0} on port {1}..."
                        .format(release_info["tempmongo_instance"], mongo_port))
            collections_to_copy_map = get_collections_to_copy(collections_to_copy, sources=release_info["sources"])
            mongo_data_copy_to_remote_host(mongo_port, private_config_xml_file, profile, assembly_accession,
                                           collections_to_copy_map, dump_dir, destination_db_name)
            exit_code = 0
    except Exception as ex:
        logger.error("Encountered an error while copying species data to Embassy for assemblies in "
                     + release_info["tempmongo_instance"] + "\n" + traceback.format_exc())
        exit_code = -1
    finally:
        close_mongo_port_to_tempmongo(port_forwarding_process_id)
        logger.info("Copy process completed with exit_code: " + str(exit_code))
        sys.exit(exit_code)


@click.option("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
@click.option("--profile", help="Maven profile to use, ex: internal", required=True)
@click.option("--taxonomy-id", help="ex: 9913", required=True)
@click.option("--assembly-accession", help="ex: GCA_000003055.3", required=True)
@click.option("--collections-to-copy", "-c", default=collections_assembly_attribute_map.keys(),
              help="ex: dbsnpSubmittedVariantEntity,dbsnpSubmittedVariantOperationEntity", multiple=True,
              required=False)
@click.option("--release-species-inventory-table", default="eva_progress_tracker.clustering_release_tracker",
              required=False)
@click.option("--release-version", help="ex: 2", type=int, required=True)
@click.option("--dump-dir", help="ex: /path/to/dump", required=True)
@click.command()
def main(private_config_xml_file, profile, taxonomy_id, assembly_accession, collections_to_copy, release_species_inventory_table,
         release_version, dump_dir):
    copy_accessioning_collections_to_embassy(private_config_xml_file, profile, taxonomy_id, assembly_accession,
                                             collections_to_copy, release_species_inventory_table, release_version,
                                             dump_dir)


if __name__ == "__main__":
    main()

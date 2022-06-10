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

# The purpose of this script is to validate the mapping weight attribute addition that was performed by
# the script incorporate_mapping_weight_into_accessioning.py


import click
import logging
import os

from collections import defaultdict
from itertools import islice
from pymongo import MongoClient

from ebi_eva_common_pyutils.config_utils import get_mongo_uri_for_eva_profile
from ebi_eva_common_pyutils.command_utils import run_command_with_output
from ebi_eva_common_pyutils.file_utils import file_diff, FileDiffOption
from include_mapping_weight_from_dbsnp.incorporate_mapping_weight_into_accessioning import collections_to_update \
    as collection_attribute_paths

logger = logging.getLogger(__name__)


def get_multimap_snps_from_mongo(private_config_xml_file, collection_to_validate):
    #  Dirty hack: since mongoexport does not allow switching databases
    #  replace admin in the URI with the database name and relegate admin to authSource
    production_mongo_uri = get_mongo_uri_for_eva_profile("production_processing", private_config_xml_file) \
        .replace("/admin", "/eva_accession_sharded?authSource=admin")
    output_file = collection_to_validate + "_multimap_snp_ids.txt"
    accession_attribute = collection_attribute_paths[collection_to_validate]["rs_accession_attribute_name"].replace(
        "inactiveObjects.", "inactiveObjects.0.")
    assembly_attribute = collection_attribute_paths[collection_to_validate]["assembly_attribute_name"].replace(
        "inactiveObjects.", "inactiveObjects.0.")

    export_command = 'mongoexport --uri "{0}" --collection {1} --type=csv --fields \'{2},{3}\' ' \
                     '--query \'{{"{4}": {{$exists: true}}}}\' --noHeaderLine --out {5}' \
        .format(production_mongo_uri, collection_to_validate,
                accession_attribute, assembly_attribute,
                collection_attribute_paths[collection_to_validate]["mapping_weight_attribute_path"]
                .replace("$.", ""), output_file)
    # Mongoexport is one of those brain-damaged commands that outputs progress to stderr.
    # So, log error stream to output.
    run_command_with_output("Export multimap SNP IDs in collection: " + collection_to_validate, export_command,
                            log_error_stream_to_output=True)
    run_command_with_output("Sorting multimap SNP IDs from collection: " + collection_to_validate,
                            "sort -u {0} -o {0}".format(output_file))
    return output_file


def are_all_unprocessed_multimap_snps_absent_in_mongo(private_config_xml_file, collection_to_validate,
                                                      unprocessed_multimap_snps_in_dbsnp_file):
    chunk_size = 2000
    num_entries_looked_up = 0
    with MongoClient(get_mongo_uri_for_eva_profile("production_processing", private_config_xml_file)) as mongo_handle:
        with open(unprocessed_multimap_snps_in_dbsnp_file) as unprocessed_snps_in_dbsnp_file_handle:
            while True:
                snps_to_lookup_in_mongo = defaultdict(list)
                lines = list(islice(unprocessed_snps_in_dbsnp_file_handle, chunk_size))
                # List of assembly, RS ID
                for id in lines:
                    assembly = id.split(",")[1].rstrip()
                    snp_id = int(id.split(",")[0].rstrip())
                    snps_to_lookup_in_mongo[assembly].append(snp_id)

                accession_attribute = collection_attribute_paths[collection_to_validate]["rs_accession_attribute_name"]
                assembly_attribute = collection_attribute_paths[collection_to_validate]["assembly_attribute_name"]
                if len(snps_to_lookup_in_mongo.keys()) > 0:
                    for assembly in snps_to_lookup_in_mongo.keys():
                        unprocessed_multimap_snp_from_dbsnp_present_in_mongo = \
                            mongo_handle["eva_accession_sharded"][collection_to_validate] \
                                .find_one(
                                {assembly_attribute: assembly,
                                 accession_attribute: {"$in": snps_to_lookup_in_mongo[assembly]}})
                        if unprocessed_multimap_snp_from_dbsnp_present_in_mongo is not None:
                            raise Exception(
                                "Some unprocessed multimap SNPs from dbSNP source dumps were present in Mongo. "
                                "See rs ID " +
                                str(unprocessed_multimap_snp_from_dbsnp_present_in_mongo[accession_attribute])
                                + " for example")
                    num_entries_looked_up += chunk_size
                    logger.info("Looked up {0} entries so far...".format(num_entries_looked_up))
                else:
                    break


def validate_mapping_weight_addition_to_mongo(private_config_xml_file, all_multimap_snps_from_dbsnp_file,
                                              collection_to_validate):
    # No "extra multi-map RSs" should be in Mongo with a mapping weight attribute other than the RSs from source DB
    # MULTIMAP_RS_FROM_MONGO_COLLECTION minus MULTIMAP_RS_FROM_DB = None
    logger.info("Checking if there are multi-map RS IDs in Mongo which are not in dbSNP source dumps...")
    extra_snps_in_mongo_file = collection_to_validate + "_extra_snps_in_mongo.txt"
    multimap_snps_from_mongo_file = get_multimap_snps_from_mongo(private_config_xml_file, collection_to_validate)
    file_diff(multimap_snps_from_mongo_file, all_multimap_snps_from_dbsnp_file, FileDiffOption.NOT_IN,
              extra_snps_in_mongo_file)
    if os.path.getsize(extra_snps_in_mongo_file) != 0:
        raise Exception("Extra multi-map RSs were found in Mongo which were not in dbSNP source dumps. See file: " +
                        extra_snps_in_mongo_file)
    logger.info("SUCCESS: No extra multi-map RS IDs were found in Mongo!")

    # If some multimap RSs from DB don't appear with the mapping weight attribute in Mongo,
    # it must be because those RSs were not present
    # in MongoDB in the first place (for various reasons like unmapped, rejected by release pipeline etc.,)
    # (MULTIMAP_RS_FROM_DB minus MULTIMAP_RS_FROM_MONGO) NOT IN (ALL_RS_FROM_MONGO_COLLECTION)
    logger.info("Checking if there are multi-map RS IDs from dbSNP which failed to be included in Mongo...")
    unprocessed_multimap_snps_in_dbsnp_file = collection_to_validate + "_unprocessed_snps_in_dbsnp.txt"
    file_diff(all_multimap_snps_from_dbsnp_file, multimap_snps_from_mongo_file, FileDiffOption.NOT_IN,
              unprocessed_multimap_snps_in_dbsnp_file)
    if os.path.getsize(unprocessed_multimap_snps_in_dbsnp_file) != 0:
        are_all_unprocessed_multimap_snps_absent_in_mongo(private_config_xml_file, collection_to_validate,
                                                          unprocessed_multimap_snps_in_dbsnp_file)
    logger.info("SUCCESS: No unprocessed multi-map RS IDs from dbSNP were present in Mongo!")


@click.option("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
@click.option("--all-multimap-snps-from-dbsnp-file", required=True)
@click.option("--collection-to-validate", help="ex: dbsnpClusteredVariantEntity", required=True)
@click.command()
def main(private_config_xml_file, all_multimap_snps_from_dbsnp_file, collection_to_validate):
    validate_mapping_weight_addition_to_mongo(private_config_xml_file, all_multimap_snps_from_dbsnp_file,
                                              collection_to_validate)


if __name__ == "__main__":
    main()

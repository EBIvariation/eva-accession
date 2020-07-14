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

# This script adds "mapping weight" attribute to the dbSNP-imported SNPs in Mongo collections - see EVA-2063, EVA-2015

import click
import pymongo.errors
import traceback
from mongo_query_utils import *
from pg_query_utils import *
from snpmapinfo_metadata import *

eva_accession_database = "eva_accession_sharded"
collections_to_update = {"dbsnpClusteredVariantEntity":
                             {"assembly_attribute_name": "asm",
                              "rs_accession_attribute_name": "accession",
                              "mapping_weight_attribute_path": "mapWeight",
                              "update_statements": []},
                         "dbsnpClusteredVariantOperationEntity":
                             {"assembly_attribute_name": "inactiveObjects.asm",
                              "rs_accession_attribute_name": "accession",
                              "mapping_weight_attribute_path": "inactiveObjects.$.mapWeight",
                              "update_statements": []},
                         "dbsnpSubmittedVariantEntity":
                             {"assembly_attribute_name": "seq",
                              "rs_accession_attribute_name": "rs",
                              "mapping_weight_attribute_path": "mapWeight",
                              "update_statements": []},
                         "dbsnpSubmittedVariantOperationEntity":
                             {"assembly_attribute_name": "inactiveObjects.seq",
                              "rs_accession_attribute_name": "inactiveObjects.rs",
                              "mapping_weight_attribute_path": "inactiveObjects.$.mapWeight",
                              "update_statements": []}
                         }


def get_all_assemblies_in_accessioning(accessioning_mongo_handle: pymongo.MongoClient):
    return sorted(accessioning_mongo_handle["eva_accession_sharded"]["dbsnpSubmittedVariantEntity"].distinct("seq"))


def get_mapping_weight_query(assembly, schema_name, table_name):
    query_to_get_mapping_weight = "select snp_id, weight from dbsnp_{0}.{1} where weight > 1 and " \
        .format(schema_name, table_name)
    if assembly.lower().startswith("gcf"):
        asm_acc, asm_version = assembly.split(".")
        query_to_get_mapping_weight += "asm_acc = '{0}' and asm_version = '{1}'".format(asm_acc, asm_version)
    else:
        query_to_get_mapping_weight += "assembly = '{0}'".format(assembly)
    return query_to_get_mapping_weight


def get_collection_update_statements(collection_name, collection_handle, assembly_attribute_name, GCA_accession,
                                     rs_accession_attribute_name, snp_id,
                                     mapping_weight_attribute_path, weight) -> list:
    update_statements = []
    # We have to get _id because updates cannot be issued to a sharded collection without the shard key (_id)
    for document in collection_handle.find({assembly_attribute_name: GCA_accession,
                                            rs_accession_attribute_name: snp_id}):
        filter_criteria = {"_id": document["_id"]}
        if assembly_attribute_name == "inactiveObjects.asm":
            filter_criteria[assembly_attribute_name] = GCA_accession
        # For dbsnpSubmittedVariantOperationEntity collection,
        # we should ensure that the same element in the array has the requisite RS as well as the assembly
        # This is needed because both the SNP to search and assembly are within an array in MongoDB
        # unlike dbsnpClusteredVariantOperationEntity which has the accession outside the array and the assembly inside
        elif assembly_attribute_name == "inactiveObjects.seq":
            filter_criteria["inactiveObjects"] = {"$elemMatch": {"rs": snp_id, "seq": GCA_accession}}
        update_statement = pymongo.UpdateMany(filter=filter_criteria,
                                              update={"$set": {mapping_weight_attribute_path: weight}}, upsert=True)
        logger.info("Preparing update statement {0} for collection {1}".format(update_statement.__repr__(),
                                                                               collection_name))
        update_statements.append(update_statement)
    return update_statements


def bulk_update(collection_handle, update_statements, batch_size=1000, force_update=False):
    if force_update or len(update_statements) >= batch_size:
        try:
            collection_handle.bulk_write(requests=update_statements, ordered=False)
            return True
        except Exception as ex:
            logger.error(ex)
            logger.error(traceback.format_exc())
            sys.exit(1)
    return False


def incorporate_mapping_weight_for_assembly(accessioning_mongo_handle: pymongo.MongoClient,
                                            metadata_connection_handle: pymongo.MongoClient,
                                            GCA_accession: str):
    accessioning_database_handle = accessioning_mongo_handle[eva_accession_database]

    for schema_name, table_name, assembly in get_snpmapinfo_tables_with_GCA_assembly(metadata_connection_handle,
                                                                                     GCA_accession):
        species_info = get_species_info(metadata_connection_handle, schema_name)[0]
        query_to_get_mapping_weight = get_mapping_weight_query(assembly, schema_name, table_name)
        logger.info("Running query to get mapping weight: " + query_to_get_mapping_weight)
        with get_db_conn_for_species(species_info) as species_connection_handle, \
                get_result_cursor(species_connection_handle, query_to_get_mapping_weight) as cursor:
            for result in cursor:
                snp_id, weight = result[0], result[1]
                for collection_name, collection_attributes in collections_to_update.items():
                    collection_handle = accessioning_database_handle[collection_name]
                    collection_attributes["update_statements"].extend(
                        get_collection_update_statements(
                            collection_name,
                            collection_handle,
                            collection_attributes["assembly_attribute_name"], GCA_accession,
                            collection_attributes["rs_accession_attribute_name"], snp_id,
                            collection_attributes["mapping_weight_attribute_path"], weight)
                    )
                    if bulk_update(collection_handle, collection_attributes["update_statements"]):
                        # Reset the list of update statements for this collection after a successful bulk update
                        collections_to_update[collection_name]["update_statements"] = []

    # Perform any residual updates
    for collection_name, collection_attributes in collections_to_update.items():
        update_statements = collection_attributes["update_statements"]
        if len(update_statements) > 0:
            bulk_update(accessioning_database_handle[collection_name], update_statements, force_update=True)


def incorporate_mapping_weight_into_accessioning(metadata_db_name, metadata_db_user, metadata_db_host,
                                                 mongo_user, mongo_password, mongo_host, assembly_accession):
    with get_pg_connection_handle(metadata_db_name, metadata_db_user, metadata_db_host) as metadata_connection_handle, \
        get_mongo_connection_handle(username=mongo_user,
                                    password=mongo_password, host=mongo_host) as accessioning_mongo_handle:
        incorporate_mapping_weight_for_assembly(accessioning_mongo_handle, metadata_connection_handle,
                                                assembly_accession)


@click.option("--metadata-db-name", required=True)
@click.option("--metadata-db-user", required=True)
@click.option("--metadata-db-host", required=True)
@click.option("--mongo-user", required=True)
@click.option("--mongo-password", required=True)
@click.option("--mongo-host", required=True)
@click.option("--assembly-accession", required=True)
@click.command()
def main(metadata_db_name, metadata_db_user, metadata_db_host,
         mongo_user, mongo_password, mongo_host, assembly_accession):
    incorporate_mapping_weight_into_accessioning(metadata_db_name, metadata_db_user, metadata_db_host,
                                                 mongo_user, mongo_password, mongo_host, assembly_accession)


if __name__ == "__main__":
    main()

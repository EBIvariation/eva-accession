#!/usr/bin/env python3

# Copyright 2019 EMBL - European Bioinformatics Institute
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

import itertools
import logging
import os
import re
import shutil
import sys
import traceback

from ebi_eva_common_pyutils.command_utils import run_command_with_output
from ebi_eva_common_pyutils.file_utils import file_diff, FileDiffOption
from run_release_in_embassy.release_common_utils import open_mongo_port_to_tempmongo, close_mongo_port_to_tempmongo, \
    get_release_db_name_in_tempmongo_instance
from run_release_in_embassy.copy_accessioning_collections_to_embassy import collections_assembly_attribute_map, \
    submitted_collections_taxonomy_attribute_map
from pymongo import MongoClient

logger = logging.getLogger(__name__)

sve_collection_name = "submittedVariantEntity"
svoe_collection_name = "submittedVariantOperationEntity"
dbsnp_sve_collection_name = "dbsnpSubmittedVariantEntity"
dbsnp_svoe_collection_name = "dbsnpSubmittedVariantOperationEntity"
cve_collection_name = "clusteredVariantEntity"
dbsnp_cve_collection_name = "dbsnpClusteredVariantEntity"

get_merged_ss_query = [
        {
            "$match": {
                "eventType": "MERGED",
                "inactiveObjects.rs": {
                    "$in": [

                    ]
                }
            }
        },
        {
            "$project" : {
                "accession": "$inactiveObjects.rs",
                "_id" : 0
            }
        },
        {
            "$group" : {
                "_id" : "null",
                "distinct" : {
                    "$addToSet": "$$ROOT"
                }
            }
        },
        {
            "$unwind" : {
                "path" : "$distinct",
                "preserveNullAndEmptyArrays" : False
            }
        },
        {
            "$project" : {
                 "accession": "$distinct.accession"
            }
        }
    ]

get_declustered_ss_query = [
        {
            "$match" : {
                "eventType" : "UPDATED",
                "reason" : {"$regex": r'^Declustered.*'},
                "inactiveObjects.rs" : {
                    "$in" : [

                    ]
                }
            }
        },
        {
            "$project" : {
                "accession" : "$inactiveObjects.rs",
                "_id" : 0
            }
        },
        {
            "$group" : {
                "_id" : "null",
                "distinct" : {
                    "$addToSet" : "$$ROOT"
                }
            }
        },
        {
            "$unwind" : {
                "path" : "$distinct",
                "preserveNullAndEmptyArrays" : False
            }
        },
        {
            "$project" : {
                 "accession": "$distinct.accession"
            }
        }
    ]

get_tandem_repeat_rs_query = [
        {
            "$match" : {
                "type" : "TANDEM_REPEAT",
                "accession" : {
                    "$in" : [

                    ]
                }
            }
        },
        {
            "$project" : {
                "accession" : "$accession",
                "_id" : 0
            }
        },
        {
            "$group" : {
                "_id" : "null",
                "distinct" : {
                    "$addToSet" : "$$ROOT"
                }
            }
        },
        {
            "$unwind" : {
                "path" : "$distinct",
                "preserveNullAndEmptyArrays" : False
            }
        },
        {
            "$project" : {
                 "accession": "$distinct.accession"
            }
        }
    ]

get_rs_with_non_nucleotide_letters_query_SVE = [
        {
            "$match" : {
                "rs" : {
                    "$in" : [

                    ]
                },
                "$or" : [
                    {
                        "ref" : {"$not" : re.compile("^[acgtnACGTN]+$") }

                    },
                    {
                        "alt" : {"$not" : re.compile("^[acgtnACGTN]+$") }
                    }
                ]
            }
        },
        {
            "$project" : {
                "accession" : "$rs",
                "_id" : 0
            }
        },
        {
            "$group" : {
                "_id" : "null",
                "distinct" : {
                    "$addToSet" : "$$ROOT"
                }
            }
        },
        {
            "$unwind" : {
                "path" : "$distinct",
                "preserveNullAndEmptyArrays" : False
            }
        },
        {
            "$project" : {
                 "accession": "$distinct.accession"
            }
        }
    ]

get_rs_with_non_nucleotide_letters_query_SVOE = [
        {
            "$match" : {
                "inactiveObjects.rs" : {
                    "$in" : [

                    ]
                },
                "$or" : [
                    {
                        "inactiveObjects.ref" : {"$not" : re.compile("^[acgtnACGTN]+$") }

                    },
                    {
                        "inactiveObjects.alt" : {"$not" : re.compile("^[acgtnACGTN]+$") }
                    }
                ]
            }
        },
        {
            "$project" : {
                "accession" : "$inactiveObjects.rs",
                "_id" : 0
            }
        },
        {
            "$group" : {
                "_id" : "null",
                "distinct" : {
                    "$addToSet" : "$$ROOT"
                }
            }
        },
        {
            "$unwind" : {
                "path" : "$distinct",
                "preserveNullAndEmptyArrays" : False
            }
        },
        {
            "$project" : {
                 "accession": "$distinct.accession"
            }
        }
    ]


def read_next_batch_of_missing_ids(missing_rs_ids_file_handle):
    num_lines_to_read = 1000
    for lines_read in iter(lambda: list(itertools.islice(missing_rs_ids_file_handle, num_lines_to_read)), []):
        yield lines_read


def get_unique_release_rs_ids(species_release_folder, taxonomy_id, assembly_accession):
    folder_prefix = os.path.join(species_release_folder, assembly_accession, f'{taxonomy_id}_{assembly_accession}')
    active_rs_ids_file = folder_prefix + "_current_ids_with_genbank.vcf.gz"
    merged_rs_ids_file = folder_prefix + "_merged_ids_with_genbank.vcf.gz"
    multimap_rs_ids_file = folder_prefix + "_multimap_ids_with_genbank.vcf.gz"
    merged_deprecated_rs_ids_file = folder_prefix + "_merged_deprecated_ids.txt.gz"
    deprecated_rs_ids_file = folder_prefix + "_deprecated_ids.txt.gz"

    all_ids_file = folder_prefix + "_all_release_ids.txt"
    unique_ids_file = folder_prefix + "_unique_release_ids.txt"

    command_to_split_semicolon_separated_rs_ids = 'sed s/";"/\\\\n/g'
    run_command_with_output("Remove pre-existing all IDs and unique IDs file:", "rm -f {0} {1}"
                            .format(all_ids_file, unique_ids_file))
    run_command_with_output("Get active RS IDs", "zcat {0} | grep -v ^# | cut -f3 | sed s/rs//g | {1} >> {2}"
                            .format(active_rs_ids_file, command_to_split_semicolon_separated_rs_ids, all_ids_file))
    run_command_with_output("Get merged RS IDs", "zcat {0} | grep -v ^# | cut -f3 | sed s/rs//g | {1} >> {2}"
                            .format(merged_rs_ids_file, command_to_split_semicolon_separated_rs_ids, all_ids_file))
    run_command_with_output("Get multimap RS IDs", "zcat {0} | grep -v ^# | cut -f3 | sed s/rs//g | {1} >> {2}"
                            .format(multimap_rs_ids_file, command_to_split_semicolon_separated_rs_ids, all_ids_file))
    run_command_with_output("Get merged deprecated RS IDs column 1", "zcat {0} | cut -f1 | sed s/rs//g >> {1}"
                            .format(merged_deprecated_rs_ids_file, all_ids_file))
    run_command_with_output("Get merged deprecated RS IDs column 2", "zcat {0} | cut -f2 | sed s/rs//g >> {1}"
                            .format(merged_deprecated_rs_ids_file, all_ids_file))
    run_command_with_output("Get deprecated RS IDs", "zcat {0} | sed s/rs//g >> {1}"
                            .format(deprecated_rs_ids_file, all_ids_file))
    run_command_with_output("Create unique IDs file from all IDs file", "sort {0} | uniq >> {1}"
                            .format(all_ids_file, unique_ids_file))

    return unique_ids_file


def get_ids_from_mongo_for_category(missing_rs_ids_file, assembly_accession, mongo_database_handle, aggregate_query_to_use,
                                    rs_id_attribute_path, collections_to_query, attribution_category):
    collection_handles = [mongo_database_handle[collection] for collection in collections_to_query]
    output_file = os.path.join(os.path.dirname(missing_rs_ids_file),
                               "{0}_{1}.txt".format(assembly_accession, attribution_category))

    with open(missing_rs_ids_file) as missing_rs_ids_file_handle, open(output_file, "w") \
            as output_file_handle:
        for lines in read_next_batch_of_missing_ids(missing_rs_ids_file_handle):
            aggregate_query_to_use[0]["$match"][rs_id_attribute_path]["$in"] = [int(line.strip()) for line in
                                                                                list(lines)]
            for collection_handle in collection_handles:
                # Documents retrieved from inactiveObjects attribute returns array
                # So the result must be subscripted to get the accession
                if "inactiveObjects" in rs_id_attribute_path:
                    lines_to_write = [str(elem["accession"][0]) + "\n" for elem in list(
                        collection_handle.aggregate(pipeline=aggregate_query_to_use, allowDiskUse=True))]
                else:
                    lines_to_write = [str(elem["accession"]) + "\n" for elem in list(
                        collection_handle.aggregate(pipeline=aggregate_query_to_use, allowDiskUse=True))]
                output_file_handle.writelines(lines_to_write)

    return output_file


def get_rs_with_merged_ss(missing_rs_ids_file, assembly_accession, mongo_database_handle):
    return get_ids_from_mongo_for_category(missing_rs_ids_file, assembly_accession, mongo_database_handle,
                                           aggregate_query_to_use=get_merged_ss_query,
                                           rs_id_attribute_path="inactiveObjects.rs",
                                           collections_to_query=[dbsnp_svoe_collection_name, svoe_collection_name],
                                           attribution_category="rs_with_merged_ss_parents")


def get_rs_with_declustered_ss(missing_rs_ids_file, assembly_accession, mongo_database_handle):
    return get_ids_from_mongo_for_category(missing_rs_ids_file, assembly_accession, mongo_database_handle,
                                           aggregate_query_to_use=get_declustered_ss_query,
                                           rs_id_attribute_path="inactiveObjects.rs",
                                           collections_to_query=[dbsnp_svoe_collection_name, svoe_collection_name],
                                           attribution_category="rs_with_declustered_ss_parents")


def get_rs_with_tandem_repeat_type(missing_rs_ids_file, assembly_accession, mongo_database_handle):
    return get_ids_from_mongo_for_category(missing_rs_ids_file, assembly_accession, mongo_database_handle,
                                           aggregate_query_to_use=get_tandem_repeat_rs_query,
                                           rs_id_attribute_path="accession",
                                           collections_to_query=[dbsnp_cve_collection_name, cve_collection_name],
                                           attribution_category="rs_with_tandem_repeat_type")


def get_rs_with_non_nucleotide_letters(missing_rs_ids_file, assembly_accession, mongo_database_handle):
    results_from_sve_file = get_ids_from_mongo_for_category(missing_rs_ids_file, assembly_accession, mongo_database_handle,
                                                            aggregate_query_to_use=
                                                            get_rs_with_non_nucleotide_letters_query_SVE,
                                                            rs_id_attribute_path="rs",
                                                            collections_to_query=[dbsnp_sve_collection_name,
                                                                                  sve_collection_name],
                                                            attribution_category="rs_with_non_nucleotide_letters_SVE")
    results_from_svoe_file = get_ids_from_mongo_for_category(missing_rs_ids_file, assembly_accession,
                                                             mongo_database_handle,
                                                             aggregate_query_to_use=
                                                             get_rs_with_non_nucleotide_letters_query_SVOE,
                                                             rs_id_attribute_path="inactiveObjects.rs",
                                                             collections_to_query=[dbsnp_svoe_collection_name,
                                                                                   svoe_collection_name],
                                                             attribution_category="rs_with_non_nucleotide_letters_SVOE")

    final_result_file = results_from_sve_file.replace("_SVE", "")
    run_command_with_output("Concatenate SVE and SVOE results for RS IDs with non-nucleotide letters",
                            "(cat {0} {1} | sort | uniq > {2})".format(results_from_sve_file, results_from_svoe_file,
                                                                       final_result_file))
    return final_result_file


def get_residual_missing_rs_ids_file(rs_still_missing_file, attributed_rs_ids_file):
    import tempfile
    run_command_with_output("Sorting residual file {0}".format(rs_still_missing_file),
                            "sort -o {0} {0}".format(rs_still_missing_file))
    run_command_with_output("Sorting attributed RS ID file {0}".format(attributed_rs_ids_file),
                            "sort -o {0} {0}".format(attributed_rs_ids_file))
    _, temp_residual_file = tempfile.mkstemp(dir=os.path.dirname(rs_still_missing_file))
    file_diff(rs_still_missing_file, attributed_rs_ids_file, FileDiffOption.NOT_IN, output_file_path=temp_residual_file)
    shutil.move(temp_residual_file, rs_still_missing_file)
    return rs_still_missing_file


def get_missing_ids_attributions(assembly_accession, missing_rs_ids_file, mongo_database_handle):
    logger.info("Beginning attributions for missing IDs....")
    # Residual file that contains missing RS IDs after each attribution.
    # After all attributions are made, this will contain missing RS IDs that could not be attributed
    # to any of the above categories
    rs_still_missing_file = os.path.join(os.path.dirname(missing_rs_ids_file),
                                         "{0}_rs_still_missing.txt".format(assembly_accession))
    run_command_with_output("Initializing residual RS ID file with missing IDs",
                            "cp {0} {1}".format(missing_rs_ids_file, rs_still_missing_file))

    rs_with_merged_ss_file = get_rs_with_merged_ss(rs_still_missing_file, assembly_accession, mongo_database_handle)
    logger.info("Wrote missing RS IDs attributed to merged SS to {0}....".format(rs_with_merged_ss_file))

    # Residual RS IDs = (Residual RS IDs so far) - (RS with merged SS)
    rs_still_missing_file = get_residual_missing_rs_ids_file(rs_still_missing_file, rs_with_merged_ss_file)
    rs_with_declustered_ss_file = get_rs_with_declustered_ss(rs_still_missing_file, assembly_accession,
                                                        mongo_database_handle)
    logger.info("Wrote missing RS IDs attributed to declustered SS to {0}....".format(rs_with_declustered_ss_file))

    # Residual RS IDs = (Residual RS IDs so far) - (RS with declustered SS)
    rs_still_missing_file = get_residual_missing_rs_ids_file(rs_still_missing_file, rs_with_declustered_ss_file)
    rs_with_tandem_repeats_file = get_rs_with_tandem_repeat_type(rs_still_missing_file, assembly_accession,
                                                                 mongo_database_handle)
    logger.info("Wrote missing RS IDs which are tandem repeats to {0}....".format(rs_with_tandem_repeats_file))

    # Residual RS IDs = (Residual RS IDs so far) - (tandem repeat RS)
    rs_still_missing_file = get_residual_missing_rs_ids_file(rs_still_missing_file, rs_with_tandem_repeats_file)
    rs_with_non_nucleotide_letters_file = get_rs_with_non_nucleotide_letters(rs_still_missing_file, assembly_accession,
                                                                             mongo_database_handle)
    logger.info("Wrote missing RS IDs which have non-nucleotide letters to {0}...."
                .format(rs_with_non_nucleotide_letters_file))

    # Residual RS IDs = (Residual RS IDs so far) - (RS with non-nucleotide letters)
    rs_still_missing_file = get_residual_missing_rs_ids_file(rs_still_missing_file, rs_with_non_nucleotide_letters_file)
    num_unattributed_missing_rs_ids = int(run_command_with_output("Counting number of RS IDs that are unattributed...",
                                                                  "(wc -l < {0})".format(rs_still_missing_file),
                                                                  return_process_output=True).strip())

    if num_unattributed_missing_rs_ids > 0:
        raise Exception("There are still {0} missing IDs. See {1}.".format(num_unattributed_missing_rs_ids,
                                                                           rs_still_missing_file))
    else:
        logger.info("All missing RS IDs have been accounted for!")


def export_unique_rs_ids_from_mongo(mongo_database_handle, taxonomy_id, assembly_accession, mongo_unique_rs_ids_file):
    collection_rs_ids_files = []
    for collection, assembly_attribute_path in collections_assembly_attribute_map.items():
        if "clustered" in collection.lower():
            collection_handle = mongo_database_handle[collection]
            collection_rs_ids_file = mongo_unique_rs_ids_file.replace(".txt", "_{0}.txt".format(collection))
            agg_pipeline = []
            for sve_coll in (sve_collection_name, dbsnp_sve_collection_name):
                taxonomy_attribute_path = submitted_collections_taxonomy_attribute_map[sve_coll]
                agg_pipeline.append({
                    "$lookup": {
                        "from": sve_coll,
                        "let": {"rsAccession": "$accession"},
                        "pipeline": [{"$match": {"$expr": {"$and": [
                            {"$eq": ["$rs", "$$rsAccession"]},
                            {"$eq": [f"${assembly_attribute_path}", assembly_accession]},
                            {"$eq": [f"${taxonomy_attribute_path}", int(taxonomy_id)]},
                        ]}}}],
                        "as": sve_coll
                    }
                })
            agg_pipeline.extend([
                {"$addFields": {
                    "ssInfo": {"$concatArrays": ["$submittedVariantEntity", "$dbsnpSubmittedVariantEntity"]}}},
                {"$match": {"ssInfo": {"$ne": []}}},
                {"$project": {"accession": 1, "_id": 0}}
            ])
            logger.info(f'Exporting RS IDs from collection {collection}')
            logger.debug(f"Using aggregation pipeline: {agg_pipeline}")
            results = collection_handle.aggregate(agg_pipeline)
            with open(collection_rs_ids_file, 'w+') as f:
                f.write('\n'.join(r['accession'] for r in results))
            collection_rs_ids_files.append(collection_rs_ids_file)
    run_command_with_output("Removing duplicates from RS IDs exported from Mongo",
                            "(cat {0} | sort -u > {1})".format(" ".join(collection_rs_ids_files),
                                                               mongo_unique_rs_ids_file))


def validate_rs_release_files(private_config_xml_file, profile, taxonomy_id, assembly_accession, release_species_inventory_table,
                              release_version, species_release_folder):
    port_forwarding_process_id, mongo_port, exit_code = None, None, -1
    try:
        port_forwarding_process_id, mongo_port = open_mongo_port_to_tempmongo(private_config_xml_file, profile, taxonomy_id,
                                                                              assembly_accession, release_species_inventory_table,
                                                                              release_version)
        db_name_in_tempmongo_instance = get_release_db_name_in_tempmongo_instance(taxonomy_id, assembly_accession)
        with MongoClient(port=mongo_port) as client:
            db_handle = client[db_name_in_tempmongo_instance]
            mongo_unique_rs_ids_file = os.path.join(species_release_folder, assembly_accession,
                                                    "{0}_mongo_unique_rs_ids.txt".format(assembly_accession))
            export_unique_rs_ids_from_mongo(db_handle, taxonomy_id, assembly_accession, mongo_unique_rs_ids_file)
            unique_release_rs_ids_file = get_unique_release_rs_ids(species_release_folder, taxonomy_id,
                                                                   assembly_accession)
            missing_rs_ids_file = os.path.join(os.path.dirname(unique_release_rs_ids_file),
                                               assembly_accession + "_missing_ids.txt")
            file_diff(mongo_unique_rs_ids_file, unique_release_rs_ids_file, FileDiffOption.NOT_IN, missing_rs_ids_file)
            get_missing_ids_attributions(assembly_accession, missing_rs_ids_file, db_handle)
            exit_code = 0
    except Exception as ex:
        logger.error("Encountered an error while running release for assembly: " + assembly_accession + "\n"
                     + traceback.format_exc())
        exit_code = -1
    finally:
        close_mongo_port_to_tempmongo(port_forwarding_process_id)
        logger.info("Validate RS release file process completed with exit_code: " + str(exit_code))
        sys.exit(exit_code)


@click.option("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
@click.option("--profile", help="Maven profile to use, ex: internal", required=True)
@click.option("--taxonomy-id", help="ex: 9913", required=True)
@click.option("--assembly-accession", help="ex: GCA_000003055.6", required=True)
@click.option("--release-species-inventory-table", default="eva_progress_tracker.clustering_release_tracker",
              required=False)
@click.option("--release-version", help="ex: 2", type=int, required=True)
@click.option("--species-release-folder", required=True)
@click.command()
def main(private_config_xml_file, profile, taxonomy_id, assembly_accession, release_species_inventory_table,
         release_version, species_release_folder):
    validate_rs_release_files(private_config_xml_file, profile, taxonomy_id, assembly_accession,
                              release_species_inventory_table, release_version, species_release_folder)


if __name__ == '__main__':
    main()

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
import configparser
import os
import re
import subprocess
import urllib.parse

from pymongo import MongoClient
from __init__ import *

release_dir, assembly_accession, dbsnpCVE, dbsnpSVE, dbsnpSVOE = None, None, None, None, None

get_merged_ss_parents_query = [
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

get_declustered_ss_parents_query = [
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


def run_command(command_description, command):
    logger.info("Starting process: " + command_description)
    logger.info("Running command: " + command)
    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1, universal_newlines=True,
                          shell=True) as process:
        for line in process.stdout:
            print(line, end='')
        errors = os.linesep.join(process.stderr.readlines())
    if process.returncode != 0:
        logger.error(command_description + " failed!" + os.linesep + errors)
        raise subprocess.CalledProcessError(process.returncode, process.args)
    else:
        logger.info(command_description + " completed successfully")
    return process.returncode


def generate_unique_rs_ids_file():
    active_rs_ids_file = os.path.sep.join([release_dir, assembly_accession]) + "_current_ids.vcf.gz"
    merged_rs_ids_file = os.path.sep.join([release_dir, assembly_accession]) + "_merged_ids.vcf.gz"
    merged_deprecated_rs_ids_file = os.path.sep.join([release_dir, assembly_accession]) + "_merged_deprecated_ids.txt.gz"
    deprecated_rs_ids_file = os.path.sep.join([release_dir, assembly_accession]) + "_deprecated_ids.txt.gz"

    curr_dir = os.getcwd()
    all_ids_file = os.path.sep.join([curr_dir, assembly_accession]) + "_all_ids.txt"
    unique_ids_file = os.path.sep.join([curr_dir, assembly_accession]) + "_unique_ids.txt"

    run_command("Remove pre-existing all IDs and unique IDs file:", "rm -f {0} {1}"
                .format(all_ids_file, unique_ids_file))
    run_command("Get active RS IDs", "zcat {0} | grep -v ^# | cut -f3 | sed s/rs//g >> {1}"
                .format(active_rs_ids_file, all_ids_file))
    run_command("Get merged RS IDs", "zcat {0} | grep -v ^# | cut -f3 | sed s/rs//g >> {1}"
                .format(merged_rs_ids_file, all_ids_file))
    run_command("Get merged deprecated RS IDs column 1", "zcat {0} | cut -f1 | sed s/rs//g >> {1}"
                .format(merged_deprecated_rs_ids_file, all_ids_file))
    run_command("Get merged deprecated RS IDs column 2", "zcat {0} | cut -f2 | sed s/rs//g >> {1}"
                .format(merged_deprecated_rs_ids_file, all_ids_file))
    run_command("Get deprecated RS IDs", "zcat {0} | sed s/rs//g >> {1}"
                .format(deprecated_rs_ids_file, all_ids_file))
    run_command("Create unique IDs file from all IDs file", "sort {0} | uniq >> {1}"
                .format(all_ids_file, unique_ids_file))

    return unique_ids_file


def diff_release_ids_against_mongo_rs_ids(unique_release_ids_file, unique_mongo_ids_file):
    curr_dir = os.getcwd()
    missing_ids_file = os.path.sep.join([curr_dir, assembly_accession]) + "_missing_ids.txt"
    run_command("Comparing RS IDs from the release files against those in Mongo",
                "comm -31 {0} {1} | grep -E \"^[0-9]+$\" | cat > {2}"
                .format(unique_release_ids_file, unique_mongo_ids_file, missing_ids_file))
    return missing_ids_file


def get_missing_ids_array(missing_ids_file):
    return [int(line) for line in open(missing_ids_file).readlines()]


def get_rs_with_merged_ss_parents(missing_ids):
    get_merged_ss_parents_query[0]["$match"]["inactiveObjects.rs"]["$in"] = list(missing_ids)
    return [elem["accession"][0] for elem in list(dbsnpSVOE.aggregate(pipeline=get_merged_ss_parents_query,
                                                                      allowDiskUse=True))]


def get_rs_with_declustered_ss_parents(missing_ids):
    get_declustered_ss_parents_query[0]["$match"]["inactiveObjects.rs"]["$in"] = list(missing_ids)
    return [elem["accession"][0] for elem in list(dbsnpSVOE.aggregate(pipeline=get_declustered_ss_parents_query,
                                                                      allowDiskUse=True))]


def get_rs_with_tandem_repeat_type(missing_ids):
    get_tandem_repeat_rs_query[0]["$match"]["accession"]["$in"] = list(missing_ids)
    return [elem["accession"] for elem in list(dbsnpCVE.aggregate(pipeline=get_tandem_repeat_rs_query,
                                                                  allowDiskUse=True))]


def get_rs_with_non_nucleotide_letters(missing_ids):
    get_rs_with_non_nucleotide_letters_query_SVE[0]["$match"]["rs"]["$in"] = list(missing_ids)
    list_from_SVE = [elem["accession"] for elem in list(dbsnpSVE.aggregate(
        pipeline=get_rs_with_non_nucleotide_letters_query_SVE, allowDiskUse=True))]
    get_rs_with_non_nucleotide_letters_query_SVOE[0]["$match"]["inactiveObjects.rs"]["$in"] = list(missing_ids)
    list_from_SVOE = [elem["accession"][0] for elem in list(dbsnpSVOE.aggregate(
        pipeline=get_rs_with_non_nucleotide_letters_query_SVOE, allowDiskUse=True))]

    return list(set(list_from_SVE + list_from_SVOE))


def get_missing_ids_attributions(missing_ids_file):
    logger.info("Beginning attributions for missing IDs....")
    file_prefix = os.path.sep.join([os.path.dirname(missing_ids_file), assembly_accession])
    rs_with_merged_ss_parents_file = file_prefix + "_rs_with_merged_ss_parents.txt"
    rs_with_declustered_ss_parents_file = file_prefix + "_rs_with_declustered_ss_parents.txt"
    rs_with_tandem_repeats_file = file_prefix + "_rs_with_tandem_repeat_type.txt"
    rs_with_non_nucleotide_letters_file = file_prefix + "_rs_with_non_nucleotide_letters.txt"
    still_missing_rs_file = file_prefix + "_rs_still_missing.txt"

    missing_ids = get_missing_ids_array(missing_ids_file)
    rs_with_merged_ss_parents = get_rs_with_merged_ss_parents(missing_ids)
    logger.info("Writing missing IDs with merged SS parents....")
    open(rs_with_merged_ss_parents_file, "w").writelines([str(elem) + "\n" for elem in rs_with_merged_ss_parents])

    missing_ids = list(set(missing_ids) - set(rs_with_merged_ss_parents))
    rs_with_declustered_ss_parents = get_rs_with_declustered_ss_parents(missing_ids)
    logger.info("Writing missing IDs with declustered SS parents....")
    open(rs_with_declustered_ss_parents_file, "w").writelines([str(elem) + "\n" for elem in
                                                               rs_with_declustered_ss_parents])

    missing_ids = list(set(missing_ids) - set(rs_with_declustered_ss_parents))
    rs_with_tandem_repeats = get_rs_with_tandem_repeat_type(missing_ids)
    logger.info("Writing missing IDs which are STRs....")
    open(rs_with_tandem_repeats_file, "w").writelines([str(elem) + "\n" for elem in rs_with_tandem_repeats])

    missing_ids = list(set(missing_ids) - set(rs_with_tandem_repeats))
    rs_with_non_nucleotide_letters = get_rs_with_non_nucleotide_letters(missing_ids)
    logger.info("Writing missing IDs which have non-nucleotide letters....")
    open(rs_with_non_nucleotide_letters_file, "w").writelines([str(elem) + "\n" for elem in
                                                               rs_with_non_nucleotide_letters])

    missing_ids = list(set(missing_ids) - set(rs_with_non_nucleotide_letters))
    open(still_missing_rs_file, "w").writelines([str(elem) + "\n" for elem in missing_ids])
    if len(missing_ids) > 0:
        logger.info("There are still {0} missing IDs. See {1}.".format(len(missing_ids), still_missing_rs_file))
    else:
        logger.info("All missing RS IDs have been accounted for!")


def get_args_from_release_properties_file(assembly_properties_file):
    parser = configparser.ConfigParser()
    parser.optionxform = str

    with open(assembly_properties_file, "r") as properties_file_handle:
        # Dummy section is needed because
        # ConfigParser is not clever enough to read config files without section headers
        properties_section_name = "release_properties"
        properties_string = '[{0}]\n{1}'.format(properties_section_name, properties_file_handle.read())
        parser.read_string(properties_string)
        config = dict(parser.items(section=properties_section_name))
        return config


def validate_rs_release_files(release_properties_file, mongo_unique_ids_file):
    global release_dir, assembly_accession, dbsnpCVE, dbsnpSVE, dbsnpSVOE

    properties_file_args = get_args_from_release_properties_file(release_properties_file)
    assembly_accession = properties_file_args["parameters.assemblyAccession"]
    release_dir = properties_file_args["parameters.outputFolder"]
    mongo_host = properties_file_args["spring.data.mongodb.host"]
    mongo_port = properties_file_args["spring.data.mongodb.port"]
    mongo_db = properties_file_args["spring.data.mongodb.database"]
    mongo_user = properties_file_args["spring.data.mongodb.username"]
    mongo_pass = properties_file_args["spring.data.mongodb.password"]
    mongo_auth = properties_file_args["spring.data.mongodb.authentication-database"]

    with MongoClient("mongodb://{0}:{1}@{2}:{3}/{4}".format(mongo_user, urllib.parse.quote_plus(mongo_pass),
                                                            mongo_host, mongo_port, mongo_auth)) as client:
        db = client[mongo_db]
        dbsnpSVOE = db["dbsnpSubmittedVariantOperationEntity"]
        dbsnpCVE = db["dbsnpClusteredVariantEntity"]
        dbsnpSVE = db["dbsnpSubmittedVariantEntity"]

        unique_release_rs_ids_file = generate_unique_rs_ids_file()
        missing_rs_ids_file = diff_release_ids_against_mongo_rs_ids(unique_release_rs_ids_file, mongo_unique_ids_file)

        get_missing_ids_attributions(missing_rs_ids_file)


@click.option("-p", "--release-properties-file", required=True)
@click.option("-m", "--mongo-unique-ids-file", required=True,
              help="Unique RS ID from Mongo (unique_mongo.csv from accessioning count validation output)")
@click.command()
def main(release_properties_file, mongo_unique_ids_file):
    validate_rs_release_files(release_properties_file, mongo_unique_ids_file)


if __name__ == '__main__':
    main()

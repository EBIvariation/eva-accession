#!/usr/bin/env python3

# Copyright 2021 EMBL - European Bioinformatics Institute
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

# This script replaces incorrect contigs in the accessioning warehouse
# (usually RefSeq contig accessions or chromosome names like MT) with the correct Genbank contig names
# Adapted from https://github.com/EBIvariation/eva-tasks/blob/master/tasks/eva_2464/correct_contig_error_in_study.py

import argparse
import hashlib
import pymongo
import traceback

from ebi_eva_common_pyutils.logger import logging_config
from ebi_eva_common_pyutils.mongodb import MongoDatabase
from pymongo import WriteConcern
from pymongo.read_concern import ReadConcern


logger = logging_config.get_logger(__name__)
logging_config.add_stdout_handler()


def get_SHA1(variant_rec):
    """Calculate the SHA1 digest from the seq, study, contig, start, ref, and alt attributes of the variant"""
    h = hashlib.sha1()
    keys = ['seq', 'study', 'contig', 'start', 'ref', 'alt']
    h.update('_'.join([str(variant_rec[key]) for key in keys]).encode())
    return h.hexdigest().upper()


def replace_with_correct_contig(mongo_source, assembly_accession, study_accession, incorrect_contig, correct_contig,
                                num_variants_to_replace):
    sve_collection = mongo_source.mongo_handle[mongo_source.db_name]["submittedVariantEntity"]
    filter_criteria = {'seq': assembly_accession, 'study': study_accession, 'contig': incorrect_contig}
    cursor = sve_collection.with_options(read_concern=ReadConcern("majority")) \
        .find(filter_criteria, no_cursor_timeout=True).limit(num_variants_to_replace)
    insert_statements = []
    drop_statements = []
    total_inserted, total_dropped = 0, 0
    try:
        for variant in cursor:
            original_id = get_SHA1(variant)
            assert variant['_id'] == original_id, "Original id is different from the one calculated %s != %s" % (
                variant['_id'], original_id)
            variant['contig'] = correct_contig
            variant['_id'] = get_SHA1(variant)
            insert_statements.append(pymongo.InsertOne(variant))
            drop_statements.append(pymongo.DeleteOne({'_id': original_id}))
        result_insert = sve_collection.with_options(write_concern=WriteConcern(w="majority", wtimeout=1200000)) \
            .bulk_write(requests=insert_statements, ordered=False)
        total_inserted += result_insert.inserted_count
        result_drop = sve_collection.with_options(write_concern=WriteConcern(w="majority", wtimeout=1200000)) \
            .bulk_write(requests=drop_statements, ordered=False)
        total_dropped += result_drop.deleted_count
        logger.info('%s / %s new documents inserted' % (total_inserted, num_variants_to_replace))
        logger.info('%s / %s old documents dropped' % (total_dropped, num_variants_to_replace))
    except Exception as e:
        print(traceback.format_exc())
        raise e
    finally:
        cursor.close()
    return total_inserted


def main():
    parser = argparse.ArgumentParser(
        description='Replace incorrect contig in a given assembly and study with the correct contig', add_help=False)
    parser.add_argument("--mongo-source-uri",
                        help="Mongo Source URI (ex: mongodb://user:@mongos-source-host:27017/admin)", required=True)
    parser.add_argument("--mongo-source-secrets-file",
                        help="Full path to the Mongo Source secrets file (ex: /path/to/mongo/source/secret)",
                        required=True)
    parser.add_argument("--assembly-accession", help="Genbank assembly accession (ex: GCA_000003055.5)", required=True)
    parser.add_argument("--study-accession", help="Study accession (ex: PRJEB29734)", required=True)
    parser.add_argument("--incorrect-contig", help="Study accession (ex: MT)", required=True)
    parser.add_argument("--correct-contig", help="Study accession (ex: AY526085.1)", required=True)
    parser.add_argument("--num-variants-to-replace", help="Number of variants to replace (ex: 10)", type=int,
                        required=True)

    args = parser.parse_args()
    mongo_source = MongoDatabase(uri=args.mongo_source_uri, secrets_file=args.mongo_source_secrets_file,
                                 db_name="eva_accession_sharded")
    replace_with_correct_contig(mongo_source, args.assembly_accession, args.study_accession,
                                args.incorrect_contig, args.correct_contig, args.num_variants_to_replace)


if __name__ == "__main__":
    main()

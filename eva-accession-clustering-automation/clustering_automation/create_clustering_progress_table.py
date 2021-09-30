#!/usr/bin/env python
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
import argparse
from itertools import cycle

from ebi_eva_common_pyutils.assembly import NCBIAssembly
from ebi_eva_common_pyutils.logger import logging_config

from ebi_eva_common_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_common_pyutils.mongodb import MongoDatabase
from ebi_eva_common_pyutils.pg_utils import get_all_results_for_query, execute_query
from ebi_eva_common_pyutils.taxonomy.taxonomy import normalise_taxon_scientific_name

logger = logging_config.get_logger(__name__)

all_tasks = ['create_and_fill_table', 'fill_rs_count']

def create_table(private_config_xml_file):
    query_create_table = (
        'create table if not exists eva_progress_tracker.clustering_release_tracker('
        'taxonomy int4 not null, '
        'scientific_name text not null, '
        'assembly_accession text not null, '
        'release_version int8 not null, '
        'sources text not null,'
        'clustering_status text null, '
        'clustering_start timestamp null, '
        'clustering_end timestamp null, '
        'should_be_clustered boolean null, '
        'fasta_path text null, '
        'report_path text null, '
        'tempmongo_instance text null, '
        'should_be_released boolean null, '
        'num_rs_to_release int8 null, '
        'total_num_variants int8 null, '
        'release_folder_name text null, '
        'release_status text null, '
        'primary key (taxonomy, assembly_accession, release_version))'
    )
    with get_metadata_connection_handle("development", private_config_xml_file) as pg_conn:
        execute_query(pg_conn, query_create_table)


def fill_in_table_from_remapping(private_config_xml_file, release_version, reference_directory):
    # round robin through the instances from 1 to 10
    tempmongo_instances = cycle([f'tempmongo-{instance}' for instance in range(1, 11)])
    query_retrieve_info = (
        "select taxonomy, scientific_name, assembly_accession, string_agg(distinct source, ', '), sum(num_ss_ids)"
        "from eva_progress_tracker.remapping_tracker "
        f"where release_version={release_version} "
        "group by taxonomy, scientific_name, assembly_accession")
    with get_metadata_connection_handle("development", private_config_xml_file) as pg_conn:
        for taxonomy, scientific_name, assembly_accession, sources, num_ss_id in get_all_results_for_query(pg_conn,
                                                                                                           query_retrieve_info):
            if num_ss_id == 0:
                # Do not release species with no data
                continue

            should_be_clustered = True
            should_be_released = True
            ncbi_assembly = NCBIAssembly(assembly_accession, scientific_name, reference_directory)
            fasta_path = ncbi_assembly.assembly_fasta_path
            report_path = ncbi_assembly.assembly_report_path
            tempmongo_instance = next(tempmongo_instances)
            release_folder_name = normalise_taxon_scientific_name(scientific_name)
            query_insert = (
                'INSERT INTO eva_progress_tracker.clustering_release_tracker '
                '(sources, taxonomy, scientific_name, assembly_accession, release_version, should_be_clustered, '
                'fasta_path, report_path, tempmongo_instance, should_be_released, release_folder_name) '
                f"VALUES ('{sources}', {taxonomy}, '{scientific_name}', '{assembly_accession}', {release_version}, "
                f"{should_be_clustered}, '{fasta_path}', '{report_path}', '{tempmongo_instance}', {should_be_released}, "
                f"'{release_folder_name}') ON CONFLICT DO NOTHING")
            execute_query(pg_conn, query_insert)


def fill_in_from_previous_inventory(private_config_xml_file, release_version):
    query = ("select taxonomy_id, scientific_name, assembly, sources, total_num_variants, release_folder_name "
            "from dbsnp_ensembl_species.release_species_inventory where sources='DBSNP - filesystem' and release_version=2")
    with get_metadata_connection_handle("development", private_config_xml_file) as pg_conn:
        for taxonomy, scientific_name, assembly, sources, total_num_variants, release_folder_name in get_all_results_for_query(pg_conn, query):
            should_be_clustered = False
            should_be_released = False
            query_insert = (
                'INSERT INTO eva_progress_tracker.clustering_release_tracker '
                '(sources, taxonomy, scientific_name, assembly_accession, release_version, should_be_clustered, '
                'should_be_released, release_folder_name) '
                f"VALUES ('{sources}', {taxonomy}, '{scientific_name}', '{assembly}', {release_version}, "
                f"{should_be_clustered}, {should_be_released}, '{release_folder_name}') ON CONFLICT DO NOTHING"
            )
            execute_query(pg_conn, query_insert)


def fill_num_rs_id_for_taxonomy_and_assembly(mongo_source, private_config_xml_file):
    group_rs_by_taxonomy_and_assembly = {
        "$group":
            {
                "_id": {"taxonomy": "$tax", "assembly": "$asm"},
                "rs_count": {"$sum": 1},
            }
    }
    cve_collection = mongo_source.mongo_handle[mongo_source.db_name]["clusteredVariantEntity"]
    aggregated_rs_info = cve_collection.aggregate([group_rs_by_taxonomy_and_assembly], batchSize=0)
    for agg_info in aggregated_rs_info:
        with get_metadata_connection_handle("development", private_config_xml_file) as pg_conn:
            taxonomy = agg_info["_id"]["taxonomy"]
            assembly = agg_info["_id"]["assembly"]
            rs_count = agg_info["rs_count"]
            logger.info(f'updating rs count({rs_count}) for taxonomy({taxonomy}) and assembly({assembly})')
            query_update = (
                f'UPDATE eva_progress_tracker.clustering_release_tracker SET num_rs_to_release={rs_count} '
                f'WHERE taxonomy={taxonomy} and assembly_accession=\'{assembly}\'')
            execute_query(pg_conn, query_update)


def main():
    parser = argparse.ArgumentParser(description='Create and load the clustering and release tracking table', add_help=False)
    parser.add_argument("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
    parser.add_argument("--release-version", help="version of the release", type=int, required=False)
    parser.add_argument("--reference-directory", help="Directory where the reference genomes exists or should be downloaded", required=False)
    parser.add_argument("--mongo-source-uri",
                            help="Mongo Source URI (ex: mongodb://user:@mongos-source-host:27017/admin)", required=False)
    parser.add_argument("--mongo-source-secrets-file",
                            help="Full path to the Mongo Source secrets file (ex: /path/to/mongo/source/secret)",
                            required=False)
    parser.add_argument('--tasks', required=False, type=str, nargs='+', default=all_tasks, choices=all_tasks,
                            help='Task or set of tasks to perform.')
    parser.add_argument('--help', action='help', help='Show this help message and exit')
    args = parser.parse_args()

    logging_config.add_stdout_handler()

    if not args.tasks:
        args.tasks = all_tasks

    if 'create_and_fill_table' in args.tasks:
        if not args.release_version:
            raise Exception("For running task 'create_and_fill_table', it is mandatory to provide --release-version' argument")
        create_table(args.private_config_xml_file)
        fill_in_from_previous_inventory(args.private_config_xml_file, args.release_version)
        fill_in_table_from_remapping(args.private_config_xml_file, args.release_version, args.reference_directory)

    if 'fill_rs_count' in args.tasks:
        if not args.mongo_source_uri or not args.mongo_source_secrets_file:
            raise Exception("""For running task 'fill_rs_count', it is mandatory to provide
                            '--mongo-source-uri' and '--mongo-source-secrets-file' arguments""")
        mongo_source = MongoDatabase(uri=args.mongo_source_uri, secrets_file=args.mongo_source_secrets_file,
                                     db_name="eva_accession_sharded")
        fill_num_rs_id_for_taxonomy_and_assembly(mongo_source, args.private_config_xml_file)


if __name__ == '__main__':
    main()

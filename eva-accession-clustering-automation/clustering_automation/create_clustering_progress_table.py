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
from ebi_eva_common_pyutils.pg_utils import get_all_results_for_query, execute_query
from ebi_eva_common_pyutils.taxonomy.taxonomy import normalise_taxon_scientific_name

logger = logging_config.get_logger(__name__)


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


def main():
    parser = argparse.ArgumentParser(description='Create and load the clustering and release tracking table', add_help=False)
    parser.add_argument("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
    parser.add_argument("--release-version", help="version of the release", type=int, required=False)
    parser.add_argument("--reference-directory", help="Directory where the reference genomes exists or should be downloaded", required=False)
    parser.add_argument('--help', action='help', help='Show this help message and exit')
    args = parser.parse_args()

    logging_config.add_stdout_handler()

    create_table(args.private_config_xml_file)
    fill_in_from_previous_inventory(args.private_config_xml_file, args.release_version)
    fill_in_table_from_remapping(args.private_config_xml_file, args.release_version, args.reference_directory)


if __name__ == '__main__':
    main()

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
from collections import defaultdict
from itertools import cycle

from ebi_eva_common_pyutils.assembly import NCBIAssembly
from ebi_eva_common_pyutils.config_utils import get_mongo_uri_for_eva_profile
from ebi_eva_common_pyutils.logger import logging_config
from ebi_eva_common_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_common_pyutils.mongodb import MongoDatabase
from ebi_eva_common_pyutils.pg_utils import get_all_results_for_query, execute_query
from ebi_eva_common_pyutils.taxonomy.taxonomy import normalise_taxon_scientific_name

logger = logging_config.get_logger(__name__)

# round-robin through the instances from 1 to 10
tempmongo_instances = cycle([f'tempmongo-{instance}' for instance in range(1, 11)])

all_tasks = ['create_table', 'fill_release_entries', 'fill_should_be_released']

taxonomy_tempmongo_instance = {}

def get_tempmongo_instance(pg_conn, taxonomy_id):
    if not taxonomy_tempmongo_instance:
        query = "select taxonomy, tempmongo_instance from eva_progress_tracker.clustering_release_tracker where tempmongo_instance is not null"
        for taxonomy, tempmongo_instance in get_all_results_for_query(pg_conn, query):
            taxonomy_tempmongo_instance[taxonomy] = tempmongo_instance

    if taxonomy_id not in taxonomy_tempmongo_instance:
        taxonomy_tempmongo_instance[taxonomy_id] = next(tempmongo_instances)
    return taxonomy_tempmongo_instance[taxonomy_id]

def create_table_if_not_exists(private_config_xml_file):
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
    with get_metadata_connection_handle("production_processing", private_config_xml_file) as pg_conn:
        execute_query(pg_conn, query_create_table)


def get_scientific_name(pg_conn, taxonomy):
    query = f'SELECT scientific_name from evapro.taxonomy where taxonomy_id={taxonomy}'
    results = get_all_results_for_query(pg_conn, query)
    if not results:
        raise Exception(f'No entry found in taxonomy table for taxonomy: {taxonomy}')
    sc_name = results[0][0]
    if "'" in sc_name:
        # some scientific names in db has single quote (') in name causing issues while inserting
        sc_name = sc_name.replace("'", "\''")
        return sc_name
    else:
        return sc_name


def fill_in_from_previous_release(private_config_xml_file, profile, curr_release_version):
    query = f"""select taxonomy, scientific_name, assembly_accession, sources, fasta_path, report_path, 
                release_folder_name from eva_progress_tracker.clustering_release_tracker 
                where release_version = {curr_release_version - 1} and assembly_accession != 'Unmapped'"""
    with get_metadata_connection_handle("production_processing", private_config_xml_file) as pg_conn:
        with get_metadata_connection_handle(profile, private_config_xml_file) as pg_conn_insert:
            for tax, sc_name, asm_acc, src, fs_path, rpt_path, rls_folder_name in get_all_results_for_query(pg_conn,
                                                                                                            query):
                tmp_instance = get_tempmongo_instance(pg_conn, tax)
                query_insert = f"""INSERT INTO eva_progress_tracker.clustering_release_tracker(
                                    taxonomy, scientific_name, assembly_accession, release_version, sources,
                                    fasta_path, report_path, tempmongo_instance, release_folder_name) 
                                    VALUES ({tax}, '{sc_name}', '{asm_acc}', {curr_release_version}, '{src}', 
                                    '{fs_path}', '{rpt_path}', '{tmp_instance}', '{rls_folder_name}') 
                                    ON CONFLICT DO NOTHING"""

                execute_query(pg_conn_insert, query_insert)


def fill_in_from_eva(private_config_xml_file, profile, release_version, ref_dir):
    query = f"""select distinct  pt.taxonomy_id as taxonomy, asm.assembly_accession as assembly_accession
                from evapro.project_taxonomy pt
                join evapro.project_analysis pa on pt.project_accession = pa.project_accession 
                join evapro.analysis a on a.analysis_accession = pa.analysis_accession 
                join evapro.assembly asm on asm.assembly_set_id = a.assembly_set_id 
                and asm.assembly_accession is not null and assembly_accession like 'GCA%'"""

    with get_metadata_connection_handle("production_processing", private_config_xml_file) as pg_conn:
        for tax, asm_acc in get_all_results_for_query(pg_conn, query):
            insert_entry_for_taxonomy_and_assembly(private_config_xml_file, profile, tax, asm_acc, release_version,
                                                   ref_dir)


def fill_in_from_supported_assembly_tracker(private_config_xml_file, profile, release_version, ref_dir):
    query = f"""select distinct taxonomy_id as taxonomy, assembly_id as assembly_accession
                from evapro.supported_assembly_tracker sat"""

    with get_metadata_connection_handle("production_processing", private_config_xml_file) as pg_conn:
        for tax, asm_acc in get_all_results_for_query(pg_conn, query):
            insert_entry_for_taxonomy_and_assembly(private_config_xml_file, profile, tax, asm_acc, release_version,
                                                   ref_dir)


def insert_entry_for_taxonomy_and_assembly(private_config_xml_file, profile, tax, asm_acc, release_version, ref_dir):
    with get_metadata_connection_handle(profile, private_config_xml_file) as pg_conn:
        sc_name = get_scientific_name(pg_conn, tax)
        src = 'EVA'
        ncbi_assembly = NCBIAssembly(asm_acc, sc_name, ref_dir)
        fasta_path = ncbi_assembly.assembly_fasta_path
        report_path = ncbi_assembly.assembly_report_path
        tempongo_instance = get_tempmongo_instance(pg_conn, tax)
        release_folder_name = normalise_taxon_scientific_name(sc_name)

        insert_query = f"""INSERT INTO eva_progress_tracker.clustering_release_tracker(
                            taxonomy, scientific_name, assembly_accession, release_version, sources,
                            fasta_path, report_path, tempmongo_instance, release_folder_name) 
                            VALUES ({tax}, '{sc_name}', '{asm_acc}', {release_version}, '{src}', 
                            '{fasta_path}', '{report_path}', '{tempongo_instance}', '{release_folder_name}') 
                            ON CONFLICT DO NOTHING"""

        execute_query(pg_conn, insert_query)


def get_assembly_list_for_taxonomy_for_release(private_config_xml_file, profile, release_version, taxonomy):
    assembly_source = {}
    with get_metadata_connection_handle(profile, private_config_xml_file) as pg_conn:
        query = f"""SELECT distinct assembly_accession, sources from eva_progress_tracker.clustering_release_tracker 
                    where taxonomy = {taxonomy} and release_version = {release_version}"""
        for assembly, sources in get_all_results_for_query(pg_conn, query):
            assembly_source[assembly] = sources

        return assembly_source


def get_taxonomy_list_for_release(private_config_xml_file, profile, release_version):
    tax_asm = defaultdict(defaultdict)
    query = f"""select distinct taxonomy, assembly_accession, sources 
                from eva_progress_tracker.clustering_release_tracker
                where release_version={release_version}"""
    with get_metadata_connection_handle(profile, private_config_xml_file) as pg_conn:
        for tax, asm_acc, sources in get_all_results_for_query(pg_conn, query):
            tax_asm[tax][asm_acc] = sources
        return tax_asm


def get_sources_for_taxonomy_assembly(private_config_xml_file, profile, release_version, taxonomy, assembly):
    with get_metadata_connection_handle(profile, private_config_xml_file) as pg_conn:
        query = f"""SELECT sources from eva_progress_tracker.clustering_release_tracker 
                    where taxonomy = {taxonomy} and assembly_accession='{assembly}' 
                    and release_version = {release_version}"""

        result = get_all_results_for_query(pg_conn, query)
        return result[0][0]


def find_if_any_ss_has_rs_for_tax_and_asm(mongo_source, coll, tax, asm):
    search_query = {'tax': tax, 'seq': asm, 'rs': {'$exists': True}}
    collection = mongo_source.mongo_handle[mongo_source.db_name][coll]
    result = collection.find_one(search_query)
    if result:
        logger.info(f'Found SS with RS for Taxonomy {tax} and Assembly {asm} : SS {result}')
        return True
    else:
        logger.warning(f'No SS with RS found for Taxonomy {tax} and Assembly {asm}')
        return False


def fill_should_be_released_for_taxonomy_and_assembly(private_config_xml_file, tax, asm, src, profile, release_version,
                                                      mongo_source):
    should_be_released_eva = should_be_released_dbsnp = False
    if 'EVA' in src:
        eva_coll = 'submittedVariantEntity'
        should_be_released_eva = find_if_any_ss_has_rs_for_tax_and_asm(mongo_source, eva_coll, tax, asm)
    if 'DBSNP' in src:
        dbsnp_coll = 'dbsnpSubmittedVariantentity'
        should_be_released_dbsnp = find_if_any_ss_has_rs_for_tax_and_asm(mongo_source, dbsnp_coll, tax, asm)

    should_be_released = should_be_released_eva or should_be_released_dbsnp
    num_rs_to_release = 1 if should_be_released else 0

    with get_metadata_connection_handle(profile, private_config_xml_file) as pg_conn:
        update_query = f"""update eva_progress_tracker.clustering_release_tracker 
                        set should_be_released={should_be_released}, num_rs_to_release={num_rs_to_release}
                        where taxonomy={tax} and assembly_accession='{asm}' and release_version={release_version}"""

        execute_query(pg_conn, update_query)


def fill_should_be_released_for_taxonomy(private_config_xml_file, tax, asm_sources, profile, release_version,
                                         mongo_source):
    for asm_acc in asm_sources:
        fill_should_be_released_for_taxonomy_and_assembly(private_config_xml_file, tax, asm_acc, asm_sources[asm_acc],
                                                          profile, release_version, mongo_source)


def fill_should_be_released_for_all_in_release(private_config_xml_file, profile, release_version, mongo_source):
    tax_asm = get_taxonomy_list_for_release(private_config_xml_file, profile, release_version)
    for tax in tax_asm:
        fill_should_be_released_for_taxonomy(private_config_xml_file, tax, tax_asm[tax], profile, release_version,
                                             mongo_source)


def main():
    parser = argparse.ArgumentParser(description='Create and load the clustering and release tracking table',
                                     add_help=False)
    parser.add_argument("--private-config-xml-file", required=True, help="ex: /path/to/eva-maven-settings.xml")
    parser.add_argument("--release-version", required=True, type=int, help="version of the release")
    parser.add_argument("--reference-directory", required=True,
                        help="Directory where the reference genomes exists or should be downloaded")
    parser.add_argument("--profile", required=True, help="profile where entries should be made e.g. development")
    parser.add_argument('--tasks', required=False, type=str, nargs='+', choices=all_tasks,
                        help='Task or set of tasks to perform')
    parser.add_argument("--taxonomy", required=False, type=int,
                        help="taxonomy id for which should be released needs to be updated")
    parser.add_argument("--assembly", required=False,
                        help="assembly accession for which should be released needs to be updated")
    parser.add_argument('--help', action='help', help='Show this help message and exit')
    args = parser.parse_args()

    logging_config.add_stdout_handler()

    if not args.tasks:
        args.tasks = ['fill_release_entries']

    if 'create_table' in args.tasks:
        create_table_if_not_exists(args.private_config_xml_file)

    mongo_source_uri = get_mongo_uri_for_eva_profile('production', args.private_config_xml_file)
    mongo_source = MongoDatabase(uri=mongo_source_uri, db_name="eva_accession_sharded")

    if 'fill_release_entries' in args.tasks:
        fill_in_from_previous_release(args.private_config_xml_file, args.profile, args.release_version)
        fill_in_from_eva(args.private_config_xml_file, args.profile, args.release_version, args.reference_directory)
        fill_in_from_supported_assembly_tracker(args.private_config_xml_file, args.profile, args.release_version,
                                                args.reference_directory)

        fill_should_be_released_for_all_in_release(args.private_config_xml_file, args.profile, args.release_version,
                                                   mongo_source)

    if 'fill_should_be_released' in args.tasks:
        if not args.taxonomy:
            raise Exception("For running task 'fill_should_be_released', it is mandatory to provide --taxonomy")
        if not args.assembly:
            asm_list = get_assembly_list_for_taxonomy_for_release(args.private_config_xml_file, args.profile,
                                                                  args.release_version, args.taxonomy)
            fill_should_be_released_for_taxonomy(args.private_config_xml_file, args.taxonomy, asm_list, args.profile,
                                                 args.release_version, mongo_source)
        else:
            sources = get_sources_for_taxonomy_assembly(args.private_config_xml_file, args.profile,
                                                        args.release_version, args.taxonomy, args.assembly)
            fill_should_be_released_for_taxonomy_and_assembly(args.private_config_xml_file, args.taxonomy,
                                                              args.assembly, sources, args.profile,
                                                              args.release_version, mongo_source)


if __name__ == '__main__':
    main()

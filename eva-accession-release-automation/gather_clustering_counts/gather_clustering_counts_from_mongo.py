import argparse
import os
import pickle

import psycopg2
import psycopg2.extras
from collections import defaultdict
from datetime import datetime

from ebi_eva_common_pyutils.logger import logging_config
from ebi_eva_common_pyutils.mongodb import MongoDatabase
from ebi_eva_common_pyutils.config_utils import get_accession_pg_creds_for_profile
from ebi_eva_common_pyutils.pg_utils import execute_query, get_all_results_for_query
from ebi_eva_common_pyutils.metadata_utils import get_metadata_connection_handle
from gather_clustering_counts.gather_per_species_clustering_counts import assembly_table_name, tracker_table_name
from urllib.parse import urlsplit

logger = logging_config.get_logger(__name__)
logging_config.add_stdout_handler()
taxonomy_scientific_name_map = {}
assembly_taxonomy_map = defaultdict(set)


def get_release_end_timestamps_from_last_release(private_config_xml_file, current_release_version) -> dict:
    """
    Get release end timestamps from last release for all assemblies
    """
    assembly_timestamp_map = {}
    with get_metadata_connection_handle("production_processing", private_config_xml_file) as metadata_connection_handle:
        prev_release_end_timestamp_query = "select assembly_accession, " \
                                           "max(release_end)::timestamp AT TIME ZONE 'Europe/London' " \
                                           "as last_release_end_timestamp from " \
                                           f"{tracker_table_name} " \
                                           f"where release_version < {current_release_version} " \
                                           f"and release_end is not null " \
                                           "group by assembly_accession"
        for result in get_all_results_for_query(metadata_connection_handle, prev_release_end_timestamp_query):
            assembly_timestamp_map[result[0]] = result[1]
    return assembly_timestamp_map


def get_all_assemblies_for_current_release(private_config_xml_file, current_release_version):
    with get_metadata_connection_handle("production_processing", private_config_xml_file) as metadata_connection_handle:
        query_to_get_assemblies_for_release = "select distinct assembly_accession from " \
                                              f"{tracker_table_name} " \
                                              f"where release_version = {current_release_version} "
    return [x[0] for x in get_all_results_for_query(metadata_connection_handle, query_to_get_assemblies_for_release)]


def gather_count_from_mongo(mongo_source, private_config_xml_file, release_version):
    ranges_per_assembly = defaultdict(dict)
    current_release_assemblies = get_all_assemblies_for_current_release(private_config_xml_file, release_version)
    prev_release_end_timestamp_per_assembly = get_release_end_timestamps_from_last_release(private_config_xml_file,
                                                                                           release_version)
    # Only run if dump files don't already exist
    ranges_fp = f'ranges_release_{release_version}.pkl'
    metrics_fp = f'metrics_release_{release_version}.pkl'
    if not os.path.exists(ranges_fp):
        for assembly_accession in current_release_assemblies:
            prev_release_end_timestamp_for_assembly = prev_release_end_timestamp_per_assembly.get(assembly_accession,
                                                                                                  None)
            time_ranges_by_job_id_from_job_tracker = get_time_ranges_by_job_id_from_job_tracker(
                assembly_accession, private_config_xml_file, prev_release_end_timestamp_for_assembly)
            ranges_per_assembly.update(get_assembly_info_and_date_ranges(assembly_accession,
                                                                         time_ranges_by_job_id_from_job_tracker))
        pickle.dump(ranges_per_assembly, open(ranges_fp, 'wb+'))
    else:
        ranges_per_assembly = pickle.load(open(ranges_fp, 'rb'))
    if not os.path.exists(metrics_fp):
        metrics_per_assembly = get_metrics_per_assembly(mongo_source, ranges_per_assembly)
        pickle.dump(metrics_per_assembly, open(metrics_fp, 'wb+'))
    else:
        metrics_per_assembly = pickle.load(open(metrics_fp, 'rb'))

    insert_counts_in_db(private_config_xml_file, metrics_per_assembly, ranges_per_assembly, release_version)


def get_assembly_info_and_date_ranges(assembly_accession, time_ranges_by_job_id: dict) -> dict:
    """
    Parse all the log files and retrieve assembly basic information (taxonomy, scientific name) and the date ranges
    where all jobs and steps were run during the clustering process
    Results returned as follows:
    {
        asm_1: {
            'species_info': [ (tax_1, name_1), ... ]
            'metrics': {
                metric_1: {
                    log_1: {
                        from: ...,
                        to: ...
                    },
                    log_2: { ... }
                },
                metric_2: { ... }
            }
        },
        asm_2: { ... }
    }
    """
    ranges_per_assembly = defaultdict(dict)
    for job_id, job_id_time_ranges in time_ranges_by_job_id.items():
        logger.info('Process Job ID: ' + str(job_id))
        if assembly_accession not in ranges_per_assembly:
            ranges_per_assembly[assembly_accession] = defaultdict(dict)
            ranges_per_assembly[assembly_accession]['metrics'] = defaultdict(dict)

        # species info - a list of (taxonomy, scientific name) associated with this assembly
        if 'species_info' not in ranges_per_assembly[assembly_accession]:
            ranges_per_assembly[assembly_accession]['species_info'] = set()
            for taxid in assembly_taxonomy_map[assembly_accession]:
                ranges_per_assembly[assembly_accession]['species_info'].add((taxid,
                                                                             taxonomy_scientific_name_map[taxid]))

        # new_remapped_current_rs
        if 'CLUSTERING_CLUSTERED_VARIANTS_FROM_MONGO_STEP' in job_id_time_ranges:
            ranges_per_assembly[assembly_accession]['metrics']['new_remapped_current_rs'][job_id] = {
                'from': job_id_time_ranges['CLUSTERING_CLUSTERED_VARIANTS_FROM_MONGO_STEP'],
                'to': job_id_time_ranges['CLEAR_RS_MERGE_AND_SPLIT_CANDIDATES_STEP']
                if "CLEAR_RS_MERGE_AND_SPLIT_CANDIDATES_STEP" in job_id_time_ranges
                else job_id_time_ranges["last_timestamp"]
            }
        # new_clustered_current_rs
        if 'CLUSTERING_NON_CLUSTERED_VARIANTS_FROM_MONGO_STEP' in job_id_time_ranges:
            ranges_per_assembly[assembly_accession]['metrics']['new_clustered_current_rs'][job_id] = {
                'from': job_id_time_ranges['CLUSTERING_NON_CLUSTERED_VARIANTS_FROM_MONGO_STEP'],
                'to': job_id_time_ranges['CLUSTER_UNCLUSTERED_VARIANTS_JOB']["completed"]
                if "completed" in job_id_time_ranges['CLUSTER_UNCLUSTERED_VARIANTS_JOB']
                else job_id_time_ranges["last_timestamp"]
            }
        study_clustering_step_name = 'STUDY_CLUSTERING_STEP'
        study_clustering_job_name = 'STUDY_CLUSTERING_JOB'
        # new_clustered_current_rs
        if study_clustering_step_name in job_id_time_ranges:
            ranges_per_assembly[assembly_accession]['metrics']['new_clustered_current_rs'][job_id] = {
                'from': job_id_time_ranges[study_clustering_step_name],
                'to': job_id_time_ranges[study_clustering_job_name]["completed"]
                if "completed" in job_id_time_ranges[study_clustering_job_name]
                else job_id_time_ranges["last_timestamp"]
            }
        # merged_rs
        if 'PROCESS_RS_MERGE_CANDIDATES_STEP' in job_id_time_ranges:
            ranges_per_assembly[assembly_accession]['metrics']['merged_rs'][job_id] = {
                'from': job_id_time_ranges['PROCESS_RS_MERGE_CANDIDATES_STEP'],
                'to': job_id_time_ranges['PROCESS_RS_SPLIT_CANDIDATES_STEP']
                if "PROCESS_RS_SPLIT_CANDIDATES_STEP" in job_id_time_ranges
                else job_id_time_ranges["last_timestamp"]
            }
        # split_rs
        if 'PROCESS_RS_SPLIT_CANDIDATES_STEP' in job_id_time_ranges:
            ranges_per_assembly[assembly_accession]['metrics']['split_rs'][job_id] = {
                'from': job_id_time_ranges['PROCESS_RS_SPLIT_CANDIDATES_STEP'],
                'to': job_id_time_ranges['CLEAR_RS_MERGE_AND_SPLIT_CANDIDATES_STEP']
                if "CLEAR_RS_MERGE_AND_SPLIT_CANDIDATES_STEP" in job_id_time_ranges
                else job_id_time_ranges["last_timestamp"]
            }
        # new_ss_clustered
        if 'CLUSTERING_NON_CLUSTERED_VARIANTS_FROM_MONGO_STEP' in job_id_time_ranges:
            ranges_per_assembly[assembly_accession]['metrics']['new_ss_clustered'][job_id] = {
                'from': job_id_time_ranges['CLUSTERING_NON_CLUSTERED_VARIANTS_FROM_MONGO_STEP'],
                'to': job_id_time_ranges['CLUSTER_UNCLUSTERED_VARIANTS_JOB']["completed"]
                if "completed" in job_id_time_ranges['CLUSTER_UNCLUSTERED_VARIANTS_JOB']
                else job_id_time_ranges["last_timestamp"]
            }
        # new_ss_clustered
        if study_clustering_step_name in job_id_time_ranges:
            ranges_per_assembly[assembly_accession]['metrics']['new_ss_clustered'][job_id] = {
                'from': job_id_time_ranges[study_clustering_step_name],
                'to': job_id_time_ranges[study_clustering_job_name]["completed"]
                if "completed" in job_id_time_ranges[study_clustering_job_name]
                else job_id_time_ranges["last_timestamp"]
            }
    return ranges_per_assembly


def get_time_ranges_by_job_id_from_job_tracker(assembly_accession, private_config_xml_file,
                                               prev_release_end_timestamp_for_assembly: datetime = None):
    # Job and step start/end times by job ID
    time_ranges_by_job_id = defaultdict(dict)
    url, username, password = get_accession_pg_creds_for_profile("production_processing", private_config_xml_file)
    with psycopg2.connect(urlsplit(url).path, user=username, password=password,
                          cursor_factory=psycopg2.extras.RealDictCursor) as accessioning_job_tracker_handle:
        # See clustering jobs here: https://github.com/EBIvariation/eva-accession/blob/5f827ae8f062ae923a83c16070f6ebf08c544e31/eva-accession-clustering/src/main/java/uk/ac/ebi/eva/accession/clustering/configuration/BeanNames.java#L76
        jobs = ["CLUSTERING_FROM_MONGO_JOB", "STUDY_CLUSTERING_JOB", "PROCESS_REMAPPED_VARIANTS_WITH_RS_JOB",
                "CLUSTER_UNCLUSTERED_VARIANTS_JOB"]
        jobs_comma_separated = ",".join([f"'{job}'" for job in jobs])
        query_to_get_step_start_end_times = f"""
        SELECT DISTINCT
          i.JOB_NAME as job_name,
          j.JOB_EXECUTION_ID as job_id,
          s.STEP_EXECUTION_ID as step_id,
          s.STEP_NAME as step_name,
          j.START_TIME::timestamp AT TIME ZONE 'Europe/London' as job_start_time,
          j.END_TIME::timestamp AT TIME ZONE 'Europe/London' as job_end_time,
          s.START_TIME::timestamp AT TIME ZONE 'Europe/London' as step_start_time,
          s.END_TIME::timestamp AT TIME ZONE 'Europe/London' as step_end_time,
          p.STRING_VAL as assembly
        FROM 
          BATCH_JOB_EXECUTION j
          JOIN BATCH_STEP_EXECUTION s ON j.JOB_EXECUTION_ID = s.JOB_EXECUTION_ID
          JOIN BATCH_JOB_INSTANCE i ON j.JOB_INSTANCE_ID = i.JOB_INSTANCE_ID
          JOIN BATCH_JOB_EXECUTION_PARAMS p ON j.JOB_EXECUTION_ID = p.JOB_EXECUTION_ID
        WHERE 
          i.JOB_NAME in ({jobs_comma_separated})
          AND p.KEY_NAME = 'assemblyAccession'
          AND p.STRING_VAL = '{assembly_accession}'
        """
        if prev_release_end_timestamp_for_assembly:
            release_end_timestamp_for_assembly_str = \
                prev_release_end_timestamp_for_assembly.strftime('%Y-%m-%d %H:%M:%S')
            query_to_get_step_start_end_times += f" AND j.start_time > " \
                                                 f"to_timestamp('{release_end_timestamp_for_assembly_str}', " \
                                                 f"'YYYY-MM-DD HH24:MI:SS')"
        all_results = get_all_results_for_query(accessioning_job_tracker_handle, query_to_get_step_start_end_times)
        if all_results:
            for job_id in set([result["job_id"] for result in all_results]):
                time_ranges_by_job_id[job_id] = defaultdict(dict)
                time_ranges_by_job_id[job_id]["last_timestamp"] = \
                    max([result["job_end_time"] for result in all_results
                         if result["job_id"] == job_id and result["job_end_time"]] +
                        [result["job_start_time"] for result in all_results
                         if result["job_id"] == job_id and result["job_start_time"]] +
                        [result["step_end_time"] for result in all_results
                         if result["job_id"] == job_id and result["step_end_time"]] +
                        [result["step_start_time"] for result in all_results
                         if result["job_id"] == job_id and result["step_start_time"]])
            # Fill in start/end times for clustering jobs
            for job_info in set([(result["job_id"], result["job_name"], result["job_start_time"],
                                  result["job_end_time"]) for result in all_results]):
                time_ranges_by_job_id[job_info[0]][job_info[1]] = {"launched": job_info[2]}
                # Fill out completed time only if it is not null
                if job_info[3] is not None:
                    time_ranges_by_job_id[job_info[0]][job_info[1]]["completed"] = job_info[3]
            # Fill in start times for clustering steps
            for step_info in set([(result["job_id"], result["step_name"], result["step_start_time"])
                                  for result in all_results]):
                time_ranges_by_job_id[step_info[0]][step_info[1]] = step_info[2]
            return time_ranges_by_job_id
        else:
            return {}


def get_metrics_per_assembly(mongo_source, ranges_per_assembly):
    """
    Perform queries to mongodb to get counts based on the date ranges for the different metrics
    """
    metrics_per_assembly = defaultdict(dict)
    for asm, asm_dict in ranges_per_assembly.items():
        new_remapped_current_rs, new_clustered_current_rs, merged_rs, split_rs, new_ss_clustered = 0, 0, 0, 0, 0
        for metric, log_dict in asm_dict['metrics'].items():
            expressions = []
            for log_name, query_range in log_dict.items():
                expressions.append({"createdDate": {"$gt": query_range["from"], "$lt": query_range["to"]}})

            date_range_filter = expressions
            if metric == 'new_remapped_current_rs':
                filter_criteria = {'asm': asm, '$or': date_range_filter}
                new_remapped_current_rs = query_mongo(mongo_source, filter_criteria, metric)
                logger.info(f'{metric} = {new_remapped_current_rs}')
            elif metric == 'new_clustered_current_rs':
                filter_criteria = {'asm': asm, '$or': date_range_filter}
                new_clustered_current_rs = query_mongo(mongo_source, filter_criteria, metric)
                logger.info(f'{metric} = {new_clustered_current_rs}')
            elif metric == 'merged_rs':
                filter_criteria = {'inactiveObjects.asm': asm, 'eventType': 'MERGED',
                                   '$or': date_range_filter}
                merged_rs = query_mongo(mongo_source, filter_criteria, metric)
                logger.info(f'{metric} = {merged_rs}')
            elif metric == 'split_rs':
                filter_criteria = {'inactiveObjects.asm': asm, 'eventType': 'RS_SPLIT',
                                   '$or': date_range_filter}
                split_rs = query_mongo(mongo_source, filter_criteria, metric)
                logger.info(f'{metric} = {split_rs}')
            elif metric == 'new_ss_clustered':
                filter_criteria = {'inactiveObjects.seq': asm, 'eventType': 'UPDATED',
                                   '$or': date_range_filter}
                new_ss_clustered = query_mongo(mongo_source, filter_criteria, metric)
                logger.info(f'{metric} = {new_ss_clustered}')

        metrics_per_assembly[asm]["assembly_accession"] = asm
        metrics_per_assembly[asm]["new_remapped_current_rs"] = new_remapped_current_rs
        metrics_per_assembly[asm]["new_clustered_current_rs"] = new_clustered_current_rs
        metrics_per_assembly[asm]["merged_rs"] = merged_rs
        metrics_per_assembly[asm]["split_rs"] = split_rs
        metrics_per_assembly[asm]["new_ss_clustered"] = new_ss_clustered
    return metrics_per_assembly


def query_mongo(mongo_source, filter_criteria, metric):
    total_count = 0
    for collection_name in collections[metric]:
        logger.info(f'Querying mongo: db.{collection_name}.countDocuments({filter_criteria})')
        collection = mongo_source.mongo_handle[mongo_source.db_name][collection_name]
        count = collection.count_documents(filter_criteria)
        total_count += count
        logger.info(f'{count}')
    return total_count


def insert_counts_in_db(private_config_xml_file, metrics_for_assembly, ranges_per_assembly, release_version):
    with get_metadata_connection_handle("production_processing", private_config_xml_file) as metadata_connection_handle:
        fill_data_for_current_release(metadata_connection_handle, metrics_for_assembly, ranges_per_assembly,
                                      release_version)
        fill_data_from_previous_release(metadata_connection_handle, ranges_per_assembly, release_version)


def fill_data_for_current_release(metadata_connection_handle, metrics_per_assembly, ranges_per_assembly,
                                  release_version):
    """Insert metrics for taxonomies and assemblies processed during the current release."""
    for asm in metrics_per_assembly:
        # get last release data for assembly
        query_last_release = f"select * from {assembly_table_name} " \
                             f"where assembly_accession = '{asm}' and release_version = {release_version - 1}"
        logger.info(query_last_release)
        asm_last_release_data = get_all_results_for_query(metadata_connection_handle, query_last_release)
        # asm_last_release_data can have multiple rows (if multiple taxids are associated with the same assembly),
        # but we assume though metrics are same as that's how we're currently releasing.
        if asm_last_release_data:
            assert len(set(row[4:] for row in asm_last_release_data)) == 1

        # insert data for current release - common to all taxonomies that share this assembly
        new_remapped_current_rs = metrics_per_assembly[asm]['new_remapped_current_rs']
        new_clustered_current_rs = metrics_per_assembly[asm]['new_clustered_current_rs']
        new_current_rs = new_clustered_current_rs + new_remapped_current_rs

        new_merged_rs = metrics_per_assembly[asm]['merged_rs']
        new_split_rs = metrics_per_assembly[asm]['split_rs']
        new_ss_clustered = metrics_per_assembly[asm]['new_ss_clustered']

        if asm_last_release_data:
            prev_release_current_rs = asm_last_release_data[0][5]
            prev_release_merged_rs = asm_last_release_data[0][7]
            prev_release_multi_mapped_rs = asm_last_release_data[0][6]
            prev_release_deprecated_rs = asm_last_release_data[0][8]
            prev_release_merged_deprecated_rs = asm_last_release_data[0][9]

            # get ss clustered
            query_ss_clustered = f"select sum(new_ss_clustered) " \
                                 f"from {assembly_table_name} " \
                                 f"where assembly_accession = '{asm}'"
            logger.info(query_ss_clustered)
            ss_clustered_previous_releases = get_all_results_for_query(metadata_connection_handle,
                                                                       query_ss_clustered)
            total_ss_clustered_in_release = ss_clustered_previous_releases[0][0] + new_ss_clustered

            # if assembly already existed -> add counts
            total_current_rs_in_release = prev_release_current_rs + new_current_rs
            total_merged_rs_in_release = prev_release_merged_rs + new_merged_rs
            # current_rs in previous releases + newly clustered
            total_clustered_current_rs_in_release = prev_release_current_rs + new_clustered_current_rs

            insert_query_values = f"{total_current_rs_in_release}, " \
                                  f"{prev_release_multi_mapped_rs}, " \
                                  f"{total_merged_rs_in_release}, " \
                                  f"{prev_release_deprecated_rs}, " \
                                  f"{prev_release_merged_deprecated_rs}, " \
                                  f"{new_current_rs}, " \
                                  f"0, " \
                                  f"{new_merged_rs}, " \
                                  f"0, " \
                                  f"0, " \
                                  f"{new_ss_clustered}, " \
                                  f"{new_remapped_current_rs}, " \
                                  f"{new_remapped_current_rs}, " \
                                  f"{new_split_rs}, " \
                                  f"{new_split_rs}, " \
                                  f"{total_ss_clustered_in_release}," \
                                  f"{total_clustered_current_rs_in_release}," \
                                  f"{new_clustered_current_rs})"
        else:
            # if new assembly
            insert_query_values = f"{new_current_rs}, " \
                                  f"0, " \
                                  f"{new_merged_rs}, " \
                                  f"0, " \
                                  f"0, " \
                                  f"{new_current_rs}, " \
                                  f"0, " \
                                  f"{new_merged_rs}, " \
                                  f"0, " \
                                  f"0, " \
                                  f"{new_ss_clustered}, " \
                                  f"{new_remapped_current_rs}, " \
                                  f"{new_remapped_current_rs}, " \
                                  f"{new_split_rs}, " \
                                  f"{new_split_rs}, " \
                                  f"{new_ss_clustered}," \
                                  f"{new_clustered_current_rs}," \
                                  f"{new_clustered_current_rs})"

        # Finally perform inserts, once for each taxonomy but otherwise identical
        for taxid, scientific_name in ranges_per_assembly[asm]['species_info']:
            folder = f"{scientific_name}/{asm}".replace("'", "''")
            formatted_name = scientific_name.capitalize().replace('_', ' ').replace("'", "''")
            insert_query = f"insert into {assembly_table_name} " \
                           f"(taxonomy_id, scientific_name, assembly_accession, release_folder, release_version, " \
                           f"current_rs, multi_mapped_rs, merged_rs, deprecated_rs, merged_deprecated_rs, " \
                           f"new_current_rs, new_multi_mapped_rs, new_merged_rs, new_deprecated_rs, " \
                           f"new_merged_deprecated_rs, new_ss_clustered, remapped_current_rs, " \
                           f"new_remapped_current_rs, split_rs, new_split_rs, ss_clustered, clustered_current_rs," \
                           f"new_clustered_current_rs) " \
                           f"values ({taxid}, '{formatted_name}', '{asm}', '{folder}', {release_version}, "
            insert_query = f"{insert_query} {insert_query_values}"
            logger.info(insert_query)
            execute_query(metadata_connection_handle, insert_query)


def fill_data_from_previous_release(metadata_connection_handle, ranges_per_assembly, release_version):
    """Insert metrics for taxonomies and assemblies *not* processed during the current release, but present in a
    previous release."""
    assemblies_in_logs = {a for a in ranges_per_assembly.keys()}
    asm_tax_in_logs = {(asm, int(tax)) for asm in assemblies_in_logs for tax, _ in
                       ranges_per_assembly[asm]['species_info']}
    previous_release_stats = get_all_results_for_query(metadata_connection_handle,
                                                       f"select * from {assembly_table_name} "
                                                       f"where release_version = {release_version - 1}")
    for previous_assembly_stats in previous_release_stats:
        taxonomy_id = previous_assembly_stats[0]
        scientific_name = previous_assembly_stats[1]
        formatted_name = scientific_name.capitalize().replace('_', ' ')
        assembly_accession = previous_assembly_stats[2]
        release_folder = previous_assembly_stats[3]

        # If the complete taxonomy/assembly pair is already present in the current release, do nothing
        if (assembly_accession, taxonomy_id) in asm_tax_in_logs:
            continue

        # If the assembly is present in the current release but not with this taxonomy,
        # copy this release's metrics for this assembly.
        if assembly_accession in assemblies_in_logs:
            query_current_release = f"select * from {assembly_table_name} " \
                                    f"where assembly_accession = '{assembly_accession}' " \
                                    f"and release_version = {release_version}"
            current_release_stats = get_all_results_for_query(metadata_connection_handle, query_current_release)
            # again this can have multiple rows, but we assume they contain the same metrics
            # copy these metrics exactly but using this taxonomy, scientific name, and release folder
            insert_query_values = f"values ({taxonomy_id}, '{formatted_name}', '{assembly_accession}', " \
                                  f"'{release_folder}', " \
                                  f"{', '.join((str(x) for x in current_release_stats[0][4:]))});"

        # Otherwise we copy previous assembly stats exactly
        else:
            current_rs = previous_assembly_stats[5]
            multi_mapped_rs = previous_assembly_stats[6]
            merged_rs = previous_assembly_stats[7]
            deprecated_rs = previous_assembly_stats[8]
            merged_deprecated_rs = previous_assembly_stats[9]

            # get ss clustered
            query_ss_clustered = f"select sum(new_ss_clustered) " \
                                 f"from {assembly_table_name} " \
                                 f"where assembly_accession = '{assembly_accession}'"
            logger.info(query_ss_clustered)
            ss_clustered_previous_releases = get_all_results_for_query(metadata_connection_handle,
                                                                       query_ss_clustered)
            ss_clustered = ss_clustered_previous_releases[0][0]

            insert_query_values = f"values ({taxonomy_id}, '{formatted_name}', '{assembly_accession}', '{release_folder}', " \
                                  f"{release_version}, {current_rs}, {multi_mapped_rs}, {merged_rs}, {deprecated_rs}, " \
                                  f"{merged_deprecated_rs}, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, {ss_clustered}, 0, 0);"

        insert_query = f"insert into {assembly_table_name} " \
                       f"(taxonomy_id, scientific_name, assembly_accession, release_folder, release_version, " \
                       f"current_rs, multi_mapped_rs, merged_rs, deprecated_rs, merged_deprecated_rs, " \
                       f"new_current_rs, new_multi_mapped_rs, new_merged_rs, new_deprecated_rs, " \
                       f"new_merged_deprecated_rs, new_ss_clustered, remapped_current_rs, " \
                       f"new_remapped_current_rs, split_rs, new_split_rs, ss_clustered, clustered_current_rs, " \
                       f"new_clustered_current_rs) " + insert_query_values
        logger.info(insert_query)
        execute_query(metadata_connection_handle, insert_query)


collections = {
    "new_remapped_current_rs": [
        "clusteredVariantEntity",
        "dbsnpClusteredVariantEntity"
    ],
    "new_clustered_current_rs": [
        "clusteredVariantEntity"
    ],
    "merged_rs": [
        "clusteredVariantOperationEntity",
        "dbsnpClusteredVariantOperationEntity"
    ],
    "split_rs": [
        "clusteredVariantOperationEntity",
        "dbsnpClusteredVariantOperationEntity"
    ],
    "new_ss_clustered": [
        "submittedVariantOperationEntity",
        "dbsnpSubmittedVariantOperationEntity"
    ]
}


def populate_assembly_taxonomy_map(private_config_xml_file, release_version):
    with get_metadata_connection_handle("production_processing", private_config_xml_file) as metadata_connection_handle:
        query_taxonomy_assembly_assoc = f"select distinct cast(taxonomy as varchar(10)) as taxid, assembly_accession " \
                                        f"from {tracker_table_name} " \
                                        f"where release_version = {release_version}"
        for taxonomy, assembly_accession in get_all_results_for_query(metadata_connection_handle,
                                                                      query_taxonomy_assembly_assoc):
            assembly_taxonomy_map[assembly_accession].add(taxonomy)


def populate_taxonomy_scientific_name_association(private_config_xml_file, release_version):
    with get_metadata_connection_handle("production_processing", private_config_xml_file) as metadata_connection_handle:
        query_taxonomy_scientific_name = f"select distinct cast(taxonomy as varchar(10)) as taxid, scientific_name " \
                                         f"from {tracker_table_name} " \
                                         f"where release_version = {release_version}"
        for taxonomy, scientific_name in get_all_results_for_query(metadata_connection_handle,
                                                                   query_taxonomy_scientific_name):
            # Use similar approach to what is used in clustering automation: https://github.com/EBIvariation/eva-accession/blob/85371091fe5bcc56545ec2d7ccb73baa8a793c92/eva-accession-clustering-automation/clustering_automation/cluster_from_mongo.py#L59
            taxonomy_scientific_name_map[taxonomy] = scientific_name.lower().replace(' ', '_')


def main():
    parser = argparse.ArgumentParser(
        description='Parse all the clustering logs to get date ranges and query mongo to get metrics counts')
    parser.add_argument("--mongo-source-uri",
                        help="Mongo Source URI (ex: mongodb://user:@mongos-source-host:27017/admin)", required=True)
    parser.add_argument("--mongo-source-secrets-file",
                        help="Full path to the Mongo Source secrets file (ex: /path/to/mongo/source/secret)",
                        required=True)
    parser.add_argument('--private-config-xml-file', help='Path to the file containing the ', required=True)
    parser.add_argument("--release-version", type=int, help="current release version", required=True)

    args = parser.parse_args()
    mongo_source = MongoDatabase(uri=args.mongo_source_uri, secrets_file=args.mongo_source_secrets_file,
                                 db_name="eva_accession_sharded")
    populate_assembly_taxonomy_map(args.private_config_xml_file, args.release_version)
    populate_taxonomy_scientific_name_association(args.private_config_xml_file, args.release_version)
    gather_count_from_mongo(mongo_source, args.private_config_xml_file, args.release_version)


if __name__ == '__main__':
    main()

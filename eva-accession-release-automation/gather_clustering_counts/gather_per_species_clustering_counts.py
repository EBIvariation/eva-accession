import argparse
import os

from ebi_eva_common_pyutils.logger import logging_config
from ebi_eva_common_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_common_pyutils.command_utils import run_command_with_output
from ebi_eva_common_pyutils.pg_utils import get_all_results_for_query, execute_query

logger = logging_config.get_logger(__name__)
logging_config.add_stdout_handler()

shell_script_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bash')

species_table_name = 'eva_stats.release_rs_statistics_per_species'
assembly_table_name = 'eva_stats.release_rs_statistics_per_assembly'
tracker_table_name = 'eva_progress_tracker.clustering_release_tracker'

id_to_column = {
    'current': 'current_rs',
    'multimap': 'multi_mapped_rs',
    'merged': 'merged_rs',
    'deprecated': 'deprecated_rs',
    'merged_deprecated': 'merged_deprecated_rs',
    'unmapped': 'unmapped_rs'
}


def write_counts_to_table(private_config_xml_file, counts):
    all_columns = counts[0].keys()
    all_values = [f"({','.join(str(species_counts[c]) for c in all_columns)})" for species_counts in counts]
    insert_query = f"insert into {species_table_name} " \
                   f"({','.join(all_columns)}) " \
                   f"values {','.join(all_values)}"
    with get_metadata_connection_handle('production_processing', private_config_xml_file) as db_conn:
        execute_query(db_conn, insert_query)


def get_new_ss_clustered(private_config_xml_file, release_version, taxonomy_id):
    query = f"select new_ss_clustered from {assembly_table_name} " \
            f"where release_version={release_version} " \
            f"and taxonomy_id={taxonomy_id} " \
            f"and new_ss_clustered > 0"
    with get_metadata_connection_handle('production_processing', private_config_xml_file) as db_conn:
        results = get_all_results_for_query(db_conn, query)
    if len(results) > 1:
        logger.warning(f'Found {len(results)} assemblies for taxonomy {taxonomy_id} with new clustered ss')
    elif len(results) == 0:
        logger.warning(f'No assemblies found with new clustered ss for taxonomy {taxonomy_id}')
        return 0
    return sum(x[0] for x in results)


def get_last_release_metric(private_config_xml_file, release_version, taxonomy_id, column_name):
    query = f"select {column_name} from {species_table_name} " \
            f"where release_version={release_version-1} " \
            f"and taxonomy_id={taxonomy_id}"
    with get_metadata_connection_handle('production_processing', private_config_xml_file) as db_conn:
        results = get_all_results_for_query(db_conn, query)
    # If this is a new species for this release, won't find anything, so just return 0
    if len(results) < 1:
        return 0
    return results[0][0]


def get_taxonomy_and_scientific_name(private_config_xml_file, release_version, species_folder):
    query = f"select taxonomy, scientific_name from {tracker_table_name} " \
            f"where release_version={release_version} " \
            f"and release_folder_name='{species_folder}' "
    with get_metadata_connection_handle('production_processing', private_config_xml_file) as db_conn:
        results = get_all_results_for_query(db_conn, query)
    if len(results) < 1:
        logger.warning(f'Failed to get scientific name and taxonomy for {species_folder}')
        return None, None
    return results[0][0], results[0][1]


def run_count_script(script_name, species_dir, metric_id):
    log_file = f'{os.path.basename(species_dir)}_count_{metric_id}_rsid.log'
    if not os.path.exists(log_file):
        run_command_with_output(
            f'Run {script_name}',
            f'{os.path.join(shell_script_dir, script_name)} {species_dir} {metric_id}'
        )
    return log_file


def gather_counts(private_config_xml_file, release_version, release_dir):
    results = []
    for species_dir in os.listdir(release_dir):
        full_species_dir = os.path.join(release_dir, species_dir)

        # Get data from other tables
        taxid, sci_name = get_taxonomy_and_scientific_name(private_config_xml_file, release_version, species_dir)
        if not taxid or not sci_name:
            continue
        new_ss_clustered = get_new_ss_clustered(private_config_xml_file, release_version, taxid)
        per_species_results = {
            'taxonomy_id': taxid,
            'scientific_name': f"'{sci_name}'",
            'release_folder': f"'{species_dir}'",
            'release_version': release_version,
            'new_ss_clustered': new_ss_clustered
        }

        # Get metrics from release files
        for metric_id in id_to_column.keys():
            if metric_id in {'current', 'merged', 'multimap'}:
                output_log = run_count_script('count_rs_for_release.sh', full_species_dir, metric_id)
            elif metric_id in {'deprecated', 'merged_deprecated'}:
                output_log = run_count_script('count_rs_for_release_for_txt.sh', full_species_dir, metric_id)
            else:
                output_log = run_count_script('count_rs_for_release_unmapped.sh', full_species_dir, metric_id)

            with open(output_log) as f:
                total = sum(int(line.strip().split(' ')[0]) for line in f)
            per_species_results[id_to_column[metric_id]] = total

            # Include diff with previous release
            last_release_total = get_last_release_metric(private_config_xml_file, release_version, taxid,
                                                         id_to_column[metric_id])
            per_species_results[f'new_{id_to_column[metric_id]}'] = max(0, total-last_release_total)

        results.append(per_species_results)
    return results


def main():
    parser = argparse.ArgumentParser(
        description='Parse all the release output to get RS statistics per species')
    parser.add_argument("--release-root-path", type=str,
                        help="base directory where all the release was run.", required=True)
    parser.add_argument("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
    parser.add_argument("--release-version", type=int, help="current release version", required=True)

    args = parser.parse_args()
    counts = gather_counts(args.private_config_xml_file, args.release_version, args.release_root_path)
    write_counts_to_table(args.private_config_xml_file, counts)


if __name__ == '__main__':
    main()

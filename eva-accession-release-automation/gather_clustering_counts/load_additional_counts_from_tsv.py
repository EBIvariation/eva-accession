import argparse
import csv

from ebi_eva_internal_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_common_pyutils.logger import logging_config
from ebi_eva_internal_pyutils.pg_utils import execute_query

from gather_clustering_counts.gather_per_species_clustering_counts import assembly_table_name


logger = logging_config.get_logger(__name__)
logging_config.add_stdout_handler()


metrics_map = {
    'MERGED': ('new_merged_rs', 'merged_rs'),
    'DEPRECATED': ('new_deprecated_rs', 'deprecated_rs'),
}


def load_from_tsv(tsv_file, release_version, private_config_xml_file):
    all_queries = []
    with open(tsv_file) as open_file:
        reader = csv.DictReader(open_file, delimiter='\t', quoting=csv.QUOTE_NONE)

        for record in reader:
            new_metric, metric = metrics_map.get(record.get('metric'))
            count = record.get('count')
            if metric in ('merged_rs', 'deprecated_rs'):
                query = (
                    f"UPDATE {assembly_table_name} SET "
                    f"{new_metric} = {new_metric} + {count}, "
                    f"{metric} = {metric} + {count}, "
                    f"current_rs = current_rs - {count} " 
                    f"WHERE assembly_accession='{record.get('assembly')}' and release_version={release_version};"
                )
            else:
                query = (
                    f"UPDATE {assembly_table_name} SET "
                    f"{new_metric} = {new_metric} + {count}, "
                    f"{metric} = {metric} + {count} "
                    f"WHERE assembly_accession='{record.get('assembly')}' and release_version={release_version};"
                )

            logger.info(query)
            all_queries.append(query)

    with get_metadata_connection_handle('production_processing', private_config_xml_file) as db_conn:
        for query in all_queries:
            execute_query(db_conn, query)


def main():
    parser = argparse.ArgumentParser(
        description='Parse a tsv file and update the release metrics counts per assembly')
    parser.add_argument("--tsv_file", type=str,
                        help="path to the tab separated file containing 3 columns with assembly, metric and count header.",
                        required=True)
    parser.add_argument('--private-config-xml-file', help='Path to the file containing the maven configuration', required=True)
    parser.add_argument("--release-version", type=int, help="current release version", required=True)

    args = parser.parse_args()

    load_from_tsv(args.tsv_file, args.release_version, args.private_config_xml_file)


if __name__ == '__main__':
    main()

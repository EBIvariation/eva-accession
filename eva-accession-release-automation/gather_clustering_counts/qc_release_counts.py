import argparse
import csv
import glob
import os
import re
from collections import defaultdict, Counter

from ebi_eva_common_pyutils.common_utils import pretty_print
from ebi_eva_common_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_common_pyutils.pg_utils import get_all_results_for_query

from gather_clustering_counts.gather_per_species_clustering_counts import get_taxonomy_and_scientific_name, \
    assembly_table_name, id_to_column


def get_counts_from_release_files(private_config_xml_file, release_version, release_dir):
    """
    Returns counts for each assembly for each taxonomy, in the form:
    {
        tax_1: {
            asm_1: { count_1: 123, count_2: 456, ... },
            asm_2: { ... },
            ...
        },
        tax_2: { ... },
        ...
    }
    """
    results = defaultdict(dict)
    for species_dir in os.listdir(release_dir):
        full_species_dir = os.path.join(release_dir, species_dir)
        taxid, sci_name = get_taxonomy_and_scientific_name(private_config_xml_file, release_version, species_dir)
        if not taxid or not sci_name:
            continue

        assembly_dirs = glob.glob(os.path.join(full_species_dir, 'GCA_*'))
        for assembly_dir in assembly_dirs:
            assembly_accession = os.path.basename(assembly_dir)
            tax_asm_counts = {}

            with open(os.path.join(assembly_dir, 'README_rs_ids_counts.txt')) as counts_file:
                counts = [l.strip().split('\t') for l in counts_file.readlines()]
            for count_line in counts:
                if len(count_line) < 2:
                    continue
                m = re.match(r'^GCA_[0-9.]+_(.*?)_ids.*?$', count_line[0])
                if m and m.group(1):
                    tax_asm_counts[id_to_column[m.group(1)]] = int(count_line[1])

            results[taxid][assembly_accession] = tax_asm_counts
    return results


def get_counts_from_database(private_config_xml_file, release_version):
    """
    These are loaded to the per-assembly counts table by gather_clustering_counts_from_mongo,
    so just read them directly. Returns results in the same format as above.
    """
    results = defaultdict(dict)
    query = (
        f"SELECT taxonomy_id, assembly_accession, "
        f"current_rs, multi_mapped_rs, merged_rs, deprecated_rs, merged_deprecated_rs "
        f"FROM {assembly_table_name} "
        f"WHERE release_version={release_version}"
    )
    with get_metadata_connection_handle('production_processing', private_config_xml_file) as db_conn:
        for row in get_all_results_for_query(db_conn, query):
            taxid, assembly, current_rs, multi_mapped_rs, merged_rs, deprecated_rs, merged_deprecated_rs = row
            results[taxid][assembly] = {
                'current_rs': current_rs,
                'multi_mapped_rs': multi_mapped_rs,
                'merged_rs': merged_rs,
                'deprecated_rs': deprecated_rs,
                'merged_deprecated_rs': merged_deprecated_rs
            }
    return results


def compare_counts(counts_from_files, counts_from_db, threshold, output_csv):
    all_metrics = ('current_rs', 'multi_mapped_rs', 'merged_rs', 'deprecated_rs', 'merged_deprecated_rs')
    header = ('Taxonomy', 'Assembly', 'Metric', 'File', 'DB', 'Diff (file-db)')
    rows = []

    all_taxids = set(counts_from_files.keys()).union(counts_from_db.keys())
    for taxid in all_taxids:
        tax_counts_from_files = counts_from_files.get(taxid, defaultdict(lambda: Counter()))
        tax_counts_from_db = counts_from_db.get(taxid, defaultdict(lambda: Counter()))
        all_asms = set(tax_counts_from_files.keys()).union(tax_counts_from_db.keys())
        for asm in all_asms:
            asm_counts_from_files = tax_counts_from_files.get(asm, Counter())
            asm_counts_from_db = tax_counts_from_db.get(asm, Counter())
            for metric in all_metrics:
                file_count = asm_counts_from_files.get(metric, 0)
                db_count = asm_counts_from_db.get(metric, 0)
                new_row = (taxid, asm, metric, file_count, db_count, file_count - db_count)
                if abs(file_count - db_count) > threshold:
                    rows.append(new_row)

    if output_csv:
        with open(output_csv, 'w+') as output_file:
            writer = csv.writer(output_file, delimiter=',')
            writer.writerow(header)
            writer.writerows(rows)
    else:
        pretty_print(header, rows)
    return rows


def main():
    parser = argparse.ArgumentParser(description='QC release counts')
    parser.add_argument("--release-root-path", help="base directory where the release was run", required=True)
    parser.add_argument("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
    parser.add_argument("--release-version", type=int, help="current release version", required=True)
    parser.add_argument("--threshold", type=int, help="only show diffs greater than this (default 0)",
                        required=False, default=0)
    parser.add_argument("--output-csv", help="optional file to output results, otherwise will print to stdout",
                        required=False, default=None)

    args = parser.parse_args()
    private_config_xml_file = args.private_config_xml_file
    release_version = args.release_version
    release_root_path = args.release_root_path
    threshold = args.threshold
    output_csv = args.output_csv

    counts_from_files = get_counts_from_release_files(private_config_xml_file, release_version, release_root_path)
    counts_from_db = get_counts_from_database(private_config_xml_file, release_version)

    compare_counts(counts_from_files, counts_from_db, threshold, output_csv)


if __name__ == '__main__':
    main()

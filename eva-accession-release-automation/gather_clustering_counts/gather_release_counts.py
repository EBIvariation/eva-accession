import argparse
import csv
import glob
import os
from collections import defaultdict, Counter
from functools import lru_cache

from ebi_eva_common_pyutils.command_utils import run_command_with_output
from ebi_eva_common_pyutils.common_utils import pretty_print
from ebi_eva_common_pyutils.logger import logging_config, AppLogger
from ebi_eva_common_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_common_pyutils.pg_utils import get_all_results_for_query


logger = logging_config.get_logger(__name__)


shell_script_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bash')

assembly_table_name = 'eva_stats.release_rs_statistics_per_assembly'

def find_link(key_set, dict1, dict2, source_linked_set1=None, source_linked_set2=None):
    """
    Assuming 2 dictionaries providing respectively the list of values linked to a key, and a list of keys linked to a
    value, (in our case this is a list of assemblies linked to a taxonomy and the list of taxonomy linked to a assembly)
    , this recursive function starts from one of the value and find all the related keys and values and provided them
    in 2 frozen sets. For any key that belong to a relationship, this should provide the same pair of frozensets
    regardless of the starting key.
    """
    if source_linked_set1 is None:
        source_linked_set1 = set()
    if source_linked_set2 is None:
        source_linked_set2 = set()
    linked_set1 = source_linked_set1.copy()
    linked_set2 = source_linked_set2.copy()
    for key1 in key_set:
        if key1 in dict1:
            for value1 in dict1.get(key1):
                linked_set2.add(value1)
                if value1 in dict2:
                    linked_set1.update(dict2.get(value1))
    # if one of the set is still growing we check again
    if linked_set1 != source_linked_set1 or linked_set2 != source_linked_set2:
        tmp_linked_set1, tmp_linked_set2 = find_link(linked_set1, dict1, dict2, linked_set1, linked_set2)
        linked_set1.update(tmp_linked_set1)
        linked_set2.update(tmp_linked_set2)
    return frozenset(linked_set1), frozenset(linked_set2)


def gather_assemblies_and_species_from_directory(release_directory):
    """
    This function creates two dictionaries by walking through the release directory:
     - all_assemblies_2_species: keys are the assemblies found in the release directory and the values are the list of
       species directory names associated with them.
     - all_species_2_assemblies: keys are species directory names and values are the list of assembly they contains.
       It also contains the list of species with unmapped data and in the case the assembly list is empty.
    """
    all_assemblies_2_species = defaultdict(list)
    all_species_2_assemblies = defaultdict(list)
    for assembly_dir in glob.glob(os.path.join(release_directory, '*', "GCA_*")):
        assembly_accession = os.path.basename(assembly_dir)
        species_name = os.path.basename(os.path.dirname(assembly_dir))
        all_assemblies_2_species[assembly_accession].append(species_name)
        all_species_2_assemblies[species_name].append(assembly_accession)
    for unmapped_file in glob.glob(os.path.join(release_directory, '*', "*unmapped_ids.txt.gz")):
        species_name = os.path.basename(os.path.dirname(unmapped_file))
        if species_name not in all_species_2_assemblies:
            all_species_2_assemblies[species_name] = []
    return all_assemblies_2_species, all_species_2_assemblies


def gather_count_for_set_species(release_directory, set_of_species, output_dir):
    """This function find al the release files for a set of species and pass them to the counting bash script."""
    all_files = collect_files_to_count(release_directory, set_of_species)
    script_name = 'count_rs_for_all_files.sh'
    input_file_list = os.path.join(output_dir, f'input_for_{"_".join(sorted(set_of_species))}.list')
    with open(input_file_list, 'w') as open_file:
        open_file.write("\n".join(all_files))
    output_file = os.path.join(output_dir, f'count_for_{"_".join(sorted(set_of_species))}.log')
    if not os.path.exists(output_file):
        run_command_with_output(
            f'Run {script_name} for {", ".join(set_of_species)}',
            f'{os.path.join(shell_script_dir, script_name)} {output_file} {input_file_list} '
        )
    else:
        logger.warning(f'output {output_file} already exists. Remove it to perform the count again')
    return output_file


def collect_files_to_count(release_directory, set_of_species):
    all_files = []
    for species in set_of_species:
        species_dir = os.path.join(release_directory, species)
        assembly_directories = glob.glob(os.path.join(species_dir, "GCA_*"))
        for assembly_dir in assembly_directories:
            vcf_pattern = f'*_GCA_*_ids.vcf.gz'
            vcf_files = glob.glob(os.path.join(assembly_dir, vcf_pattern))
            txt_pattern = f'*_GCA_*_ids.txt.gz'
            txt_files = glob.glob(os.path.join(assembly_dir, txt_pattern))
            # I don't have the taxonomy to add to the bash pattern so remove the file that start with eva_ or dbsnp_
            vcf_files = [f for f in vcf_files if 'dbsnp_' not in f and 'eva_' not in f]
            txt_files = [f for f in txt_files if 'dbsnp_' not in f and 'eva_' not in f]
            all_files.extend(vcf_files)
            all_files.extend(txt_files)
        all_files.extend(glob.glob(os.path.join(species_dir, '*_unmapped_ids.txt.gz')))
    return all_files


def calculate_all_logs(release_dir, output_dir, species_directories=None):
    all_assemblies_2_species, all_species_2_assemblies = gather_assemblies_and_species_from_directory(release_dir)
    all_sets_of_species = set()
    # Determine the species that needs to be counted together because they share assemblies
    species_to_search = species_directories
    if not species_to_search:
        species_to_search = all_species_2_assemblies.keys()
    logger.info(f'Process {len(species_to_search)} species')
    for species in species_to_search:
        set_of_species, set_of_assemblies = find_link({species}, all_species_2_assemblies, all_assemblies_2_species)
        all_sets_of_species.add(set_of_species)
    logger.info(f'Aggregate species in {len(all_sets_of_species)} groups')
    all_logs = []
    for set_of_species in all_sets_of_species:
        logger.info(f'Process files for {",".join(set_of_species)} groups')
        all_logs.append(gather_count_for_set_species(release_dir, set_of_species, output_dir))
    return all_logs


def generate_output_tsv(dict_of_counter, output_file, header):
    with open(output_file, 'w') as open_file:
        open_file.write("\t".join([header, 'Metric', 'Count']) + '\n')
        for annotation1 in dict_of_counter:
            for annotation2 in dict_of_counter[annotation1]:
                open_file.write("\t".join([
                    str(annotation1), str(annotation2), str(dict_of_counter[annotation1][annotation2])
                ]) + '\n')


class ReleaseCounter(AppLogger):

    def __init__(self, private_config_xml_file, release_version, logs):
        self.private_config_xml_file = private_config_xml_file
        self.release_version = release_version
        self.all_counts_grouped = []
        self.parse_logs(logs)
        self.add_annotations()

    @lru_cache
    def get_taxonomy_and_scientific_name(self, species_folder):
        query = (
            f"select distinct c.taxonomy, t.scientific_name "
            f"from eva_progress_tracker.clustering_release_tracker c "
            f"join evapro.taxonomy t on c.taxonomy=t.taxonomy_id "
            f"where release_version={self.release_version} AND release_folder_name='{species_folder}'"
        )
        with get_metadata_connection_handle('production_processing', self.private_config_xml_file) as db_conn:
            results = get_all_results_for_query(db_conn, query)
        if len(results) < 1:
            logger.warning(f'Failed to get scientific name and taxonomy for {species_folder}')
            return None, None
        return results[0][0], results[0][1]

    def add_annotations(self):
        for count_groups in self.all_counts_grouped:
            for count_dict in count_groups:
                taxonomy, scientific_name = self.get_taxonomy_and_scientific_name(count_dict['release_folder'])
                count_dict['taxonomy'] = taxonomy
                count_dict['scientific_name'] = scientific_name

    def get_counts_assembly_from_database(self):
        """
        DB counts are loaded to the per-assembly counts table by gather_clustering_counts_from_mongo,
        so just read them directly.
        """
        results = defaultdict(dict)
        all_metrics = ('current_rs', 'multi_mapped_rs', 'merged_rs', 'deprecated_rs', 'merged_deprecated_rs',
                       'new_merged_rs', 'new_deprecated_rs', 'new_merged_deprecated_rs')
        query = (
                f"SELECT distinct assembly_accession, "
                + ", ".join(all_metrics) +
                f" FROM {assembly_table_name} "
                f"WHERE release_version={self.release_version}"
        )
        with get_metadata_connection_handle('production_processing', self.private_config_xml_file) as db_conn:
            for row in get_all_results_for_query(db_conn, query):
                assembly = row[0]
                for index, metric in enumerate(all_metrics):
                    results[assembly][metric] = row[index + 1]
        return results

    def parse_logs(self, all_logs):
        for log_file in all_logs:
            with open(log_file) as open_file:
                for line in open_file:
                    sp_line = line.strip().split()
                    count = int(sp_line[0])
                    set_of_annotations = sp_line[1].split(',')[:-1]
                    all_groups = []
                    for annotation in set_of_annotations:
                        assembly, release_folder, idtype = annotation.split('-')
                        all_groups.append(
                            {'count': count, 'release_folder': release_folder, 'assembly': assembly, 'idtype': idtype}
                        )
                    self.all_counts_grouped.append(all_groups)

    def generate_per_species_counts(self):
        species_counts = defaultdict(Counter)
        for count_groups in self.all_counts_grouped:
            for count_dict in count_groups:
                species_counts[count_dict['taxonomy']][count_dict['idtype'] + '_rs'] += count_dict['count']
        return species_counts

    def generate_per_assembly_counts(self):
        assembly_counts = defaultdict(Counter)
        for count_groups in self.all_counts_grouped:
            for count_dict in count_groups:
                assembly_counts[count_dict['assembly']][count_dict['idtype'] + '_rs'] += count_dict['count']
        return assembly_counts

    def generate_per_species_assembly_counts(self):
        species_assembly_counts = defaultdict(Counter)
        for count_groups in self.all_counts_grouped:
            for count_dict in count_groups:
                species_assembly_counts[count_dict['assembly'] + '\t' + count_dict['assembly']][count_dict['idtype'] + '_rs'] += count_dict['count']
        return species_assembly_counts

    def detect_inconsistent_types(self):
        inconsistent_types = []
        for count_groups in self.all_counts_grouped:
            types_present = defaultdict(list)
            for count_dict in count_groups:
                types_present[count_dict['idtype']].append(count_dict)
            if len(types_present) > 1:
                count = next(iter(types_present.values()))[0]['count']
                inconsistent_assembly_descriptions = [
                    ','.join([
                        count_dict['scientific_name'] + '-' + count_dict['assembly'] + '-' + count_dict['idtype']
                        for count_dict in count_dict_list
                    ])
                    for count_dict_list in types_present.values()
                ]
                logger.warning(
                    f'Found {count} rs types across multiple types '
                    f'{", ".join(sorted(types_present.keys()))} in {" and ".join(inconsistent_assembly_descriptions)}'
                )
                inconsistent_types.append(count_groups)
        return inconsistent_types

    def compare_assembly_counts_with_db(self, threshold=0, output_csv=None):
        all_metrics = ('current_rs', 'multi_mapped_rs', 'merged_rs', 'deprecated_rs', 'merged_deprecated_rs')
        header = ('Assembly', 'Metric', 'File', 'DB', 'Diff (file-db)')
        rows = []
        count_per_assembly_from_files = self.generate_per_assembly_counts()
        counts_per_assembly_from_db = self.get_counts_assembly_from_database()
        all_asms = set(count_per_assembly_from_files.keys()).union(counts_per_assembly_from_db.keys())
        for asm in all_asms:
            asm_counts_from_files = count_per_assembly_from_files.get(asm, {})
            asm_counts_from_db = counts_per_assembly_from_db.get(asm, {})
            for metric in all_metrics:
                file_count = asm_counts_from_files.get(metric, 0)
                db_count = asm_counts_from_db.get(metric, 0)
                new_row = (asm, metric, file_count, db_count, file_count - db_count)
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
    parser = argparse.ArgumentParser(
        description='Parse all the release output to get RS statistics per species, assemblies and types    ')
    parser.add_argument("--release-root-path", type=str,
                        help="base directory where all the release was run.", required=True)
    parser.add_argument("--output-dir", type=str,
                        help="Output directory where all count logs will be created.", required=True)
    parser.add_argument("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
    parser.add_argument("--release-version", type=int, help="current release version", required=True)
    parser.add_argument("--species-directories", type=str, nargs='+',
                        help="set of directory names to process. It will process all the related one as well. "
                             "Process all if missing")

    args = parser.parse_args()
    logging_config.add_stdout_handler()
    logger.info(f'Analyse {args.release_root_path}')
    logs = calculate_all_logs(args.release_root_path, args.output_dir, args.species_directories)
    counter = ReleaseCounter(args.private_config_xml_file, args.release_version, logs)
    counter.detect_inconsistent_types()
    generate_output_tsv(counter.generate_per_species_counts(), os.path.join(args.output_dir, 'species_counts.tsv'), 'Taxonomy')
    generate_output_tsv(counter.generate_per_assembly_counts(), os.path.join(args.output_dir, 'assembly_counts.tsv'), 'Assembly')
    generate_output_tsv(counter.generate_per_species_assembly_counts(), os.path.join(args.output_dir, 'species_assembly_counts.tsv'), 'Taxonomy\tAssembly')
    counter.compare_assembly_counts_with_db(output_csv=os.path.join(args.output_dir, 'comparison_assembly_counts.csv'))


if __name__ == "__main__":
    main()

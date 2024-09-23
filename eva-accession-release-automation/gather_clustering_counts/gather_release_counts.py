import argparse
import csv
import glob
import os
from collections import defaultdict, Counter
from functools import lru_cache, cached_property
from urllib.parse import urlsplit

from ebi_eva_common_pyutils.command_utils import run_command_with_output
from ebi_eva_common_pyutils.common_utils import pretty_print
from ebi_eva_common_pyutils.logger import logging_config, AppLogger
from ebi_eva_internal_pyutils.config_utils import get_metadata_creds_for_profile
from ebi_eva_internal_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_internal_pyutils.pg_utils import get_all_results_for_query

from sqlalchemy import select
from sqlalchemy.orm import Session

from gather_clustering_counts.release_count_models import RSCountCategory, RSCount, get_sql_alchemy_engine, \
    RSCountPerTaxonomy, RSCountPerAssembly, RSCountPerTaxonomyAssembly

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
            # first set should at least contain the query
            linked_set1.add(key1)
            for value1 in dict1.get(key1):
                linked_set2.add(value1)
                if value1 in dict2:
                    linked_set1.update(dict2.get(value1))
    # if one of the set is still growing we check again
    if linked_set1 != source_linked_set1 or linked_set2 != source_linked_set2:
        tmp_linked_set1, tmp_linked_set2 = find_link(linked_set1-key_set, dict1, dict2, linked_set1, linked_set2)
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
    """This function finds all the release files for a set of species and passes them to the counting bash script."""
    all_files = collect_files_to_count(release_directory, set_of_species)
    script_name = 'count_rs_for_all_files.sh'
    input_file_list = os.path.join(output_dir, f'input_for_{"_".join(sorted(set_of_species))}.list')
    with open(input_file_list, 'w') as open_file:
        open_file.write("\n".join(all_files))
    output_file = os.path.join(output_dir, f'count_for_{"_".join(sorted(set_of_species))}.log')
    if not os.path.exists(output_file):
        run_command_with_output(
            f'Run {script_name} for {", ".join(set_of_species)}',
            f'{os.path.join(shell_script_dir, script_name)} {input_file_list} {output_file}'
        )
    else:
        logger.warning(f'output {output_file} already exists. Remove it to perform the count again')
    return output_file


def collect_files_to_count(release_directory, set_of_species):
    """Collect all the (final) release files for a set of species"""
    all_files = []
    for species in set_of_species:
        species_dir = os.path.join(release_directory, species)
        assembly_directories = glob.glob(os.path.join(species_dir, "GCA_*"))
        for assembly_dir in assembly_directories:
            vcf_pattern = f'*GCA_*_ids.vcf.gz'
            vcf_files = glob.glob(os.path.join(assembly_dir, vcf_pattern))
            txt_pattern = f'*GCA_*_ids.txt.gz'
            txt_files = glob.glob(os.path.join(assembly_dir, txt_pattern))
            # I don't have the taxonomy to add to the bash pattern so remove the file that start with eva_ or dbsnp_
            vcf_files = [f for f in vcf_files if 'dbsnp_' not in f and 'eva_' not in f]
            txt_files = [f for f in txt_files if 'dbsnp_' not in f and 'eva_' not in f]
            all_files.extend(vcf_files)
            all_files.extend(txt_files)
        all_files.extend(glob.glob(os.path.join(species_dir, '*_unmapped_ids.txt.gz')))
    return all_files


def run_calculation_script_for_species(release_dir, output_dir, species_directories=None):
    """
    Run the bash script that count the number of RS for all the specified species directories (all if not set)
    and return the logs
    """
    all_assemblies_2_species, all_species_2_assemblies = gather_assemblies_and_species_from_directory(release_dir)
    all_sets_of_species = set()
    # Determine the species that needs to be counted together because they share assemblies
    species_to_search = species_directories
    if not species_to_search:
        species_to_search = all_species_2_assemblies.keys()
    logger.info(f'Process {len(species_to_search)} species')

    # To keep track of the species already added
    all_species_added = set()
    for species in species_to_search:
        if species not in all_species_added:
            set_of_species, set_of_assemblies = find_link({species}, all_species_2_assemblies, all_assemblies_2_species)
            all_sets_of_species.add(set_of_species)
            all_species_added.update(set_of_species)
    logger.info(f'Aggregate species in {len(all_sets_of_species)} groups')
    all_logs = []
    for set_of_species in all_sets_of_species:
        logger.info(f'Process files for {",".join(set_of_species)} groups')
        all_logs.append(gather_count_for_set_species(release_dir, set_of_species, output_dir))
    return all_logs


def generate_output_tsv(dict_of_counter, output_file, header):
    with open(output_file, 'w') as open_file:
        open_file.write("\t".join([header, 'Metric', 'Count']) + '\n')
        for assembly_or_species in dict_of_counter:
            for metric in dict_of_counter[assembly_or_species]:
                open_file.write("\t".join([
                    str(assembly_or_species), str(metric), str(dict_of_counter[assembly_or_species][metric])
                ]) + '\n')

class ReleaseCounter(AppLogger):

    def __init__(self, private_config_xml_file, config_profile, release_version, logs):
        self.private_config_xml_file = private_config_xml_file
        self.config_profile = config_profile
        self.release_version = release_version
        self.all_counts_grouped = []
        self.parse_count_script_logs(logs)
        self.add_annotations()

    @lru_cache
    def get_taxonomy(self, species_folder):
        query = (
            f"select distinct c.taxonomy "
            f"from eva_progress_tracker.clustering_release_tracker c "
            f"where release_folder_name='{species_folder}'"
        )
        with get_metadata_connection_handle(self.config_profile, self.private_config_xml_file) as db_conn:
            results = get_all_results_for_query(db_conn, query)

        if len(results) < 1:
            # Support for directory from release 1
            if species_folder.split('_')[-1].isdigit():
                taxonomy = int(species_folder.split('_')[-1])
        else:
            taxonomy = results[0][0]
        return taxonomy

    @cached_property
    def sqlalchemy_engine(self):
        pg_url, pg_user, pg_pass = get_metadata_creds_for_profile(self.config_profile, self.private_config_xml_file)
        dbtype, host_url, port_and_db = urlsplit(pg_url).path.split(':')
        port, db = port_and_db.split('/')
        return get_sql_alchemy_engine(dbtype, pg_user, pg_pass, host_url.split('/')[-1], db, port)

    def add_annotations(self):
        for count_groups in self.all_counts_grouped:
            for count_dict in count_groups:
                taxonomy = self.get_taxonomy(count_dict['release_folder'])
                if taxonomy:
                    count_dict['taxonomy'] = taxonomy
                else:
                    self.error(f"Taxonomy cannot be resolved for release_folder {count_dict['release_folder']}")

    def count_descriptor(self, count_dict):
        """Description for associated with specific taxonomy, assembly and type of RS"""
        if 'taxonomy' in count_dict:
            return f"{count_dict['taxonomy']},{count_dict['assembly']},{count_dict['idtype']}"

    def group_descriptor(self, count_groups):
        """Description for associated with group of taxonomy, assembly and type of RS that have the same RS count"""
        group_descriptor_list = [
            self.count_descriptor(count_dict)
            for count_dict in count_groups]
        if None not in group_descriptor_list:
            return '|'.join(sorted(group_descriptor_list)) + f'_release_{self.release_version}'

    def _type_to_column(self, rstype, is_new=False):
        if is_new:
            return 'new_' + rstype + '_rs'
        else:
            return rstype + '_rs'

    def _write_exploded_counts(self, session):
        for count_groups in self.all_counts_grouped:
            # All the part of the group should have the same count
            count_set = set((count_dict['count'] for count_dict in count_groups))
            assert len(count_set) == 1
            count = count_set.pop()
            # This is used to uniquely identify the count for this group so that loading the same value twice does
            # not result in duplicates
            group_description = self.group_descriptor(count_groups)
            if not group_description:
                # One of the taxonomy annotation is missing, we should not load that count
                continue
            query = select(RSCount).where(RSCount.group_description == group_description)
            result = session.execute(query).fetchone()
            if result:
                rs_count = result.RSCount
            else:
                rs_count = RSCount(count=count, group_description=group_description)
                session.add(rs_count)
            for count_dict in count_groups:
                query = select(RSCountCategory).where(
                    RSCountCategory.assembly_accession == count_dict['assembly'],
                    RSCountCategory.taxonomy_id == count_dict['taxonomy'],
                    RSCountCategory.rs_type == count_dict['idtype'],
                    RSCountCategory.release_version == self.release_version,
                    RSCountCategory.rs_count_id == rs_count.rs_count_id,
                )
                result = session.execute(query).fetchone()
                if not result:
                    self.info(
                        f"Create persistence for {count_dict['assembly']}, {count_dict['taxonomy']}, {count_dict['idtype']}, {count_dict['count']}")
                    rs_category = RSCountCategory(
                        assembly_accession=count_dict['assembly'],
                        taxonomy_id=count_dict['taxonomy'],
                        rs_type=count_dict['idtype'],
                        release_version=self.release_version,
                        rs_count=rs_count
                    )
                    session.add(rs_category)
                else:
                    rs_category = result.RSCountCategory
                    # Check if we were just loading the same value as a previous run
                    if rs_category.rs_count.count != count_dict['count']:
                        self.error(f"{self.count_descriptor(count_dict)} already has a count entry in the table "
                                   f"({rs_category.rs_count.count}) different from the one being loaded "
                                   f"{count_dict['count']}")

    def _write_per_taxonomy_counts(self, session):
        """Load the aggregated count per taxonomy (assume previous version of the release was loaded already)"""
        taxonomy_counts, species_annotation = self.generate_per_taxonomy_counts()
        for taxonomy_id in taxonomy_counts:
            query = select(RSCountPerTaxonomy).where(
                RSCountPerTaxonomy.taxonomy_id == taxonomy_id,
                RSCountPerTaxonomy.release_version == self.release_version
            )
            result = session.execute(query).fetchone()
            if result:
                taxonomy_row = result.RSCountPerTaxonomy
                self.info(f"Update counts for aggregate per taxonomy {taxonomy_id}")
            else:
                self.info(f"Create persistence for aggregate per taxonomy {taxonomy_id}")
                taxonomy_row = RSCountPerTaxonomy(
                    taxonomy_id=taxonomy_id,
                    assembly_accessions=list(species_annotation.get(taxonomy_id).get('assemblies')),
                    release_folder=species_annotation.get(taxonomy_id).get('release_folder'),
                    release_version=self.release_version,
                )
            # Get the entry from previous release
            query = select(RSCountPerTaxonomy).where(
                RSCountPerTaxonomy.taxonomy_id == taxonomy_id,
                RSCountPerTaxonomy.release_version == self.release_version - 1
            )
            result = session.execute(query).fetchone()
            if result:
                prev_count_for_taxonomy = result.RSCountPerTaxonomy
            else:
                prev_count_for_taxonomy = None
            for rs_type in taxonomy_counts.get(taxonomy_id):
                count_prev = 0
                if prev_count_for_taxonomy:
                    count_prev = getattr(prev_count_for_taxonomy, self._type_to_column(rs_type))

                count_new = taxonomy_counts.get(taxonomy_id).get(rs_type) - count_prev
                setattr(taxonomy_row, self._type_to_column(rs_type), taxonomy_counts.get(taxonomy_id).get(rs_type))
                setattr(taxonomy_row, self._type_to_column(rs_type, is_new=True), count_new)
            session.add(taxonomy_row)

    def _write_per_assembly_counts(self, session):
        """Load the aggregated count per assembly (assume previous version of the release was loaded already)"""
        assembly_counts, assembly_annotations = self.generate_per_assembly_counts()
        for assembly in assembly_counts:
            query = select(RSCountPerAssembly).where(
                RSCountPerAssembly.assembly_accession == assembly,
                RSCountPerAssembly.release_version == self.release_version
            )
            result = session.execute(query).fetchone()
            if result:
                assembly_row = result.RSCountPerAssembly
                self.info(f"Update counts for aggregate per assembly {assembly}")
            else:
                self.info(f"Create persistence for aggregate per assembly {assembly}")
                assembly_row = RSCountPerAssembly(
                    assembly_accession=assembly,
                    taxonomy_ids=assembly_annotations.get(assembly).get('taxonomies'),
                    release_folder=assembly_annotations.get(assembly).get('release_folder'),
                    release_version=self.release_version,
                )
            # Retrieve the count for the previous release
            query = select(RSCountPerAssembly).where(
                RSCountPerAssembly.assembly_accession == assembly,
                RSCountPerAssembly.release_version == self.release_version - 1
            )
            result = session.execute(query).fetchone()
            if result:
                prev_count_for_assembly = result.RSCountPerAssembly
            else:
                prev_count_for_assembly = None
            for rs_type in assembly_counts.get(assembly):
                count_prev = 0
                if prev_count_for_assembly:
                    count_prev = getattr(prev_count_for_assembly, self._type_to_column(rs_type))
                count_new = assembly_counts.get(assembly).get(rs_type) -count_prev
                setattr(assembly_row, self._type_to_column(rs_type), assembly_counts.get(assembly).get(rs_type))
                setattr(assembly_row, self._type_to_column(rs_type, is_new=True), count_new)
            session.add(assembly_row)

    def _write_per_taxonomy_and_assembly_counts(self, session):
        """Load the aggregated count per assembly (assume previous version of the release was loaded already)"""
        species_assembly_counts, species_assembly_annotations = self.generate_per_taxonomy_and_assembly_counts()
        for taxonomy, assembly in species_assembly_counts:
            query = select(RSCountPerTaxonomyAssembly).where(
                RSCountPerTaxonomyAssembly.taxonomy_id == taxonomy,
                RSCountPerTaxonomyAssembly.assembly_accession == assembly,
                RSCountPerTaxonomyAssembly.release_version == self.release_version
            )
            result = session.execute(query).fetchone()
            if result:
                taxonomy_assembly_row = result.RSCountPerTaxonomyAssembly
                self.info(f"Update counts for aggregate per taxonomy {taxonomy} and assembly {assembly}")
            else:
                self.info(f"Create persistence for aggregate per taxonomy {taxonomy} and assembly {assembly}")
                taxonomy_assembly_row = RSCountPerTaxonomyAssembly(
                    taxonomy_id=taxonomy,
                    assembly_accession=assembly,
                    release_folder=species_assembly_annotations.get((taxonomy, assembly)).get('release_folder'),
                    release_version=self.release_version,
                )
            # Retrieve the count for the previous release
            query = select(RSCountPerTaxonomyAssembly).where(
                RSCountPerTaxonomyAssembly.taxonomy_id == taxonomy,
                RSCountPerTaxonomyAssembly.assembly_accession == assembly,
                RSCountPerTaxonomyAssembly.release_version == self.release_version - 1
            )
            result = session.execute(query).fetchone()
            if result:
                prev_count_for_taxonomy_assembly = result.RSCountPerTaxonomyAssembly
            else:
                prev_count_for_taxonomy_assembly = None
            for rs_type in species_assembly_counts.get((taxonomy, assembly)):
                count_prev = 0
                if prev_count_for_taxonomy_assembly:
                    count_prev = getattr(prev_count_for_taxonomy_assembly, self._type_to_column(rs_type))

                count_new = species_assembly_counts.get((taxonomy, assembly)).get(rs_type) - count_prev
                setattr(taxonomy_assembly_row, self._type_to_column(rs_type),
                        species_assembly_counts.get((taxonomy, assembly)).get(rs_type))
                setattr(taxonomy_assembly_row, self._type_to_column(rs_type, is_new=True), count_new)
            session.add(taxonomy_assembly_row)

    def write_counts_to_db(self):
        """
        For all the counts gathered in this self.all_counts_grouped, write them to the db if they do not exist already.
        Warn if the count already exists and are different.
        """
        session = Session(self.sqlalchemy_engine)
        with session.begin():
            self._write_exploded_counts(session)
            self._write_per_taxonomy_counts(session)
            self._write_per_assembly_counts(session)
            self._write_per_taxonomy_and_assembly_counts(session)

    def get_assembly_counts_from_database(self):
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
        with get_metadata_connection_handle(self.config_profile, self.private_config_xml_file) as db_conn:
            for row in get_all_results_for_query(db_conn, query):
                assembly = row[0]
                for index, metric in enumerate(all_metrics):
                    results[assembly][metric] = row[index + 1]
        return results

    def parse_count_script_logs(self, all_logs):
        '''
        Create a list  of grouped count
        :param all_logs:
        :return:
        '''
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
                            {'count': count, 'release_folder': release_folder, 'assembly': assembly, 'idtype': idtype,
                             'annotation': annotation}
                        )
                    self.all_counts_grouped.append(all_groups)

    def generate_per_taxonomy_counts(self):
        species_counts = defaultdict(Counter)
        species_annotations = defaultdict(dict)
        for count_groups in self.all_counts_grouped:
            taxonomy_and_types = set([(count_dict['taxonomy'], count_dict['idtype']) for count_dict in count_groups])
            release_folder_map = dict((count_dict['taxonomy'], count_dict['release_folder']) for count_dict in count_groups)
            for taxonomy, rstype in taxonomy_and_types:
                if taxonomy not in species_annotations:
                    species_annotations[taxonomy] = {'assemblies': set(), 'release_folder': None}
                # All count_dict have the same count in a group
                species_counts[taxonomy][rstype] += count_groups[0]['count']
                species_annotations[taxonomy]['assemblies'].update(
                    set([
                        count_dict['assembly']
                        for count_dict in count_groups
                        if count_dict['taxonomy'] is taxonomy and count_dict['idtype'] is rstype
                    ])
                )
                species_annotations[taxonomy]['release_folder'] = release_folder_map[taxonomy]
        return species_counts, species_annotations

    def generate_per_assembly_counts(self):
        assembly_counts = defaultdict(Counter)
        assembly_annotations = {}
        for count_groups in self.all_counts_grouped:
            assembly_and_types = set([(count_dict['assembly'], count_dict['idtype']) for count_dict in count_groups])
            for assembly_accession, rstype in assembly_and_types:
                if assembly_accession not in assembly_annotations:
                    assembly_annotations[assembly_accession] = {'taxonomies': set(), 'release_folder': None}
                # All count_dict have the same count in a group
                assembly_counts[assembly_accession][rstype] += count_groups[0]['count']
                assembly_annotations[assembly_accession]['taxonomies'].update(
                    set([
                        count_dict['taxonomy']
                        for count_dict in count_groups
                        if count_dict['assembly'] is assembly_accession and count_dict['idtype'] is rstype
                    ]))
                assembly_annotations[assembly_accession]['release_folder'] = assembly_accession
        return assembly_counts, assembly_annotations

    def generate_per_taxonomy_and_assembly_counts(self):
        species_assembly_counts = defaultdict(Counter)
        species_assembly_annotations = defaultdict(dict)
        for count_groups in self.all_counts_grouped:
            taxonomy_assembly_and_types = set([(count_dict['taxonomy'], count_dict['assembly'], count_dict['idtype']) for count_dict in count_groups])
            release_folder_map = dict((count_dict['taxonomy'], count_dict['release_folder']) for count_dict in count_groups)
            for taxonomy, assembly, rstype in taxonomy_assembly_and_types:
                if (taxonomy, assembly) not in species_assembly_annotations:
                    species_assembly_annotations[(taxonomy, assembly)] = {'release_folder': None}
                # All count_dict have the same count in a group
                species_assembly_counts[(taxonomy, assembly)][rstype] += count_groups[0]['count']
                if assembly != 'Unmapped':
                    species_assembly_annotations[(taxonomy, assembly)]['release_folder'] = \
                        release_folder_map[taxonomy] + '/' + assembly
                else:
                    species_assembly_annotations[(taxonomy, assembly)]['release_folder'] = release_folder_map[taxonomy]
        return species_assembly_counts, species_assembly_annotations

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
                        count_dict['taxonomy'] + '-' + count_dict['assembly'] + '-' + count_dict['idtype']
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
        counts_per_assembly_from_db = self.get_assembly_counts_from_database()
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
    parser.add_argument("--release-version", type=int, help="current release version", required=True)
    parser.add_argument("--species-directories", type=str, nargs='+',
                        help="set of directory names to process. It will process all the related one as well. "
                             "Process all if missing")
    parser.add_argument("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
    parser.add_argument("--config-profile", help="profile to use in the config xml", required=False,
                        default='production_processing')


    args = parser.parse_args()
    logging_config.add_stdout_handler()
    output_dir = os.path.abspath(args.output_dir)
    # Move to the output dir to make sure the bash tmp dir are written their
    os.chdir(output_dir)
    logger.info(f'Analyse {args.release_root_path}')
    log_files = run_calculation_script_for_species(args.release_root_path, output_dir, args.species_directories)
    counter = ReleaseCounter(args.private_config_xml_file,
                             config_profile=args.config_profile, release_version=args.release_version, logs=log_files)
    counter.write_counts_to_db()

    # TODO: Other analysis should be performed on the database counts
    # counter.detect_inconsistent_types()
    # generate_output_tsv(counter.generate_per_species_counts(), os.path.join(args.output_dir, 'species_counts.tsv'), 'Taxonomy')
    # generate_output_tsv(counter.generate_per_assembly_counts(), os.path.join(args.output_dir, 'assembly_counts.tsv'), 'Assembly')
    # generate_output_tsv(counter.generate_per_species_assembly_counts(), os.path.join(args.output_dir, 'species_assembly_counts.tsv'), 'Taxonomy\tAssembly')
    # counter.compare_assembly_counts_with_db(output_csv=os.path.join(args.output_dir, 'comparison_assembly_counts.csv'))


if __name__ == "__main__":
    main()

import argparse
import glob
import os
from collections import defaultdict

from ebi_eva_common_pyutils.command_utils import run_command_with_output

shell_script_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bash')


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


def gather_assemblies_and_species_from_directories(release_directory):
    all_assemblies_2_species = defaultdict(list)
    all_species_2_assemblies = defaultdict(list)
    for assembly_dir in  glob.glob(os.path.join(release_directory, '*', "GCA_*")):
        assembly_accession = os.path.basename(assembly_dir)
        species_name = os.path.basename(os.path.dirname(assembly_dir))
        all_assemblies_2_species[assembly_accession].append(species_name)
        all_species_2_assemblies[species_name].append(assembly_accession)
    return all_assemblies_2_species, all_species_2_assemblies


def gather_count_for_set_species(release_directory, set_of_species, output_dir):
    all_files = collect_files_to_count(release_directory, set_of_species)
    script_name = 'count_rs_for_all_files.sh'
    input_file_list = os.path.join(output_dir, f'input_for_{"_".join(set_of_species)}.list')
    with open(input_file_list, 'w') as open_file:
        open_file.write("\n".join(all_files))
    output_file = os.path.join(output_dir, f'count_for_{"_".join(set_of_species)}.log')
    if not os.path.exists(output_file):
        run_command_with_output(
            f'Run {script_name} for {", ".join(set_of_species)}',
            f'{os.path.join(shell_script_dir, script_name)} {output_file} {input_file_list} '
        )
    return output_file


def collect_files_to_count(release_directory, set_of_species):
    all_files = []
    for species in set_of_species:
        species_dir = os.path.join(release_directory, species)
        assembly_directories = glob.glob(os.path.join(species_dir, "GCA_*"))
        for assembly_dir in assembly_directories:
            # vcf_pattern = f'{taxonomy}_{assembly}_*_ids.vcf.gz'
            vcf_pattern = f'*_GCA_*_ids.vcf.gz'
            vcf_files = glob.glob(os.path.join(assembly_dir, vcf_pattern))
            txt_pattern = f'*_GCA_*_ids.txt.gz'
            txt_files = glob.glob(os.path.join(assembly_dir, txt_pattern))
            vcf_files = [f for f in vcf_files if 'dbsnp_' not in f and 'eva_' not in f]
            txt_files = [f for f in txt_files if 'dbsnp_' not in f and 'eva_' not in f]
            all_files.extend(vcf_files)
            all_files.extend(txt_files)
        all_files.extend(glob.glob(os.path.join(species_dir,'*_unmapped_ids.txt.gz')))
    return all_files


def main():
    parser = argparse.ArgumentParser(
        description='Parse all the release output to get RS statistics per species')
    parser.add_argument("--release-root-path", type=str,
                        help="base directory where all the release was run.", required=True)
    parser.add_argument("--output-dir", type=str,
                        help="Output directory where all count logs will be created.", required=True)
    # parser.add_argument("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
    # parser.add_argument("--release-version", type=int, help="current release version", required=True)
    parser.add_argument("--species-directories", type=str, nargs='+',
                        help="set of directory names to process. It will process all the related one as well. "
                             "Process all if missing")

    args = parser.parse_args()
    all_assemblies_2_species, all_species_2_assemblies = gather_assemblies_and_species_from_directories(args.release_root_path)
    all_sets_of_species = set()
    # Determine the species that needs to be counted together because they share assemblies
    species_to_search = args.species_directories
    if not species_to_search:
        species_to_search = all_species_2_assemblies.keys()
    for species in species_to_search:
        set_of_species, set_of_assemblies = find_link({species}, all_species_2_assemblies, all_assemblies_2_species)
        all_sets_of_species.add(set_of_species)

    for set_of_species in all_sets_of_species:
        count_log = gather_count_for_set_species(args.release_root_path, set_of_species, args.output_dir)


if __name__ == "__main__":
    main()


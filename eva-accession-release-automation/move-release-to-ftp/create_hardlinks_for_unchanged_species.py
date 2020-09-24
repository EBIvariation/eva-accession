
import csv
import glob
import logging
import subprocess
import os
from argparse import ArgumentParser
from __init__ import *
import create_assembly_name_symlinks


species_name_mapping_file = 'species_name_mapping.tsv'


def get_names(tax_id, previous_release):
    with open(os.path.join(previous_release, species_name_mapping_file)) as open_file:
        reader = csv.DictReader(open_file, delimiter='\t', )
        for row in reader:
            if row['taxonomy_id'] == tax_id:
                return row['#folder_name'], row['dbsnp_database_name']


def issue_command(command):
    logging.debug('running command: {}'.format(command))
    status = subprocess.run(command.split(' '))
    status.check_returncode()


def link_all_species(tax_ids, previous_release, current_release):
    for tax_id in tax_ids:
        current_species_name, current_unmapped_prefix = get_names(tax_id, current_release)
        previous_species_name, previous_unmapped_prefix = get_names(tax_id, previous_release)
        logging.info('linking species {}'.format(current_species_name))
        issue_command('mkdir {}/by_species/{}'.format(current_release, current_species_name))
        issue_command('ln {}/by_species/{}/{}_unmapped_ids.txt.gz {}/by_species/{}/'.format(
            previous_release, previous_species_name, previous_unmapped_prefix, current_release, current_species_name))
        issue_command('ln {}/by_species/{}/README_unmapped_rs_ids_count.txt {}/by_species/{}/'.format(
            previous_release, previous_species_name, current_release, current_species_name))
        issue_command('ln {}/by_species/{}/md5checksums.txt {}/by_species/{}/'.format(
            previous_release, previous_species_name, current_release, current_species_name))

        folders = glob.glob(os.path.join(previous_release, 'by_species', previous_species_name, '*'))

        # GCA or GCF symlinks should be present
        assemblies = [os.path.basename(asm) for asm in folders if os.path.basename(asm).startswith('GC')]
        for assembly in assemblies:
            logging.info('linking assembly {}'.format(assembly))
            issue_command('mkdir {}/by_assembly/{}'.format(current_release, assembly))
            asm_files = glob.glob(os.path.join(previous_release, 'by_assembly', assembly, '*'))
            for asm_file in asm_files:
                issue_command('ln {} {}/by_assembly/{}'.format(asm_file, current_release, assembly))

            issue_command('ln -sfT ../../by_assembly/{} {}/by_species/{}/{}'.format(
                assembly, current_release, current_species_name, assembly))

        create_assembly_name_symlinks.main(['{}/by_species/{}'.format(current_release, current_species_name)])


def main():
    argparse = ArgumentParser(description='This script copies the species and assemblies folders from a previous '
                                          'release. This is useful if a species hasn\'t changed since the last release.'
                                          ' The data won\'t be actually copied; hard links will be used.')
    argparse.add_argument('-t', '--tax_ids', nargs='+', help='Taxonomy IDs of the species to copy (e.g. 7227)',
                          required=True)
    argparse.add_argument('-p', '--previous_release', help='Full path of the previous release (e.g. /path/to/release_1)',
                          required=True)
    argparse.add_argument('-c', '--current_release', help='Full path of the previous release (e.g. /path/to/release_2)',
                          required=True)
    args = argparse.parse_args()
    link_all_species(args.tax_ids, args.previous_release, args.current_release)


if __name__ == "__main__":
    main()

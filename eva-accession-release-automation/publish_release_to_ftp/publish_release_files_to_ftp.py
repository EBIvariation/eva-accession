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

# Given a taxonomy, this script publishes data from the NFS staging folder to the public FTP release folder
# and creates the layout as shown in the link below:
# https://docs.google.com/presentation/d/1cishRa6P6beIBTP8l1SgJfz71vQcCm5XLmSA8Hmf8rw/edit#slide=id.g63fd5cd489_0_0

import glob
import os
from argparse import ArgumentParser

from ebi_eva_common_pyutils.config import cfg

from publish_release_to_ftp.create_assembly_name_symlinks import create_assembly_name_symlinks
from ebi_eva_common_pyutils.command_utils import run_command_with_output
from ebi_eva_common_pyutils.logger import logging_config
from ebi_eva_internal_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_internal_pyutils.pg_utils import get_all_results_for_query
from run_release_in_embassy.release_common_utils import get_release_folder_name
from run_release_in_embassy.run_release_for_species import load_config, get_release_folder
from run_release_in_embassy.release_metadata import release_vcf_file_categories, release_text_file_categories

by_assembly_folder_name = "by_assembly"
by_species_folder_name = "by_species"
release_file_types_to_be_checksummed = ("vcf.gz", "txt.gz", "csi")
readme_general_info_file = "README_release_general_info.txt"
readme_known_issues_file = "README_release_known_issues.txt"
readme_changelog_file = "README_release_changelog.txt"
species_name_mapping_file = "species_name_mapping.tsv"
release_top_level_files_to_copy = (readme_known_issues_file, readme_general_info_file)
script_dir = os.path.dirname(__file__)
unmapped_ids_file_regex = "*_unmapped_ids.txt.gz"
logger = logging_config.get_logger(__name__)


class ReleaseProperties:
    def __init__(self, release_version):
        """
        Get release properties from common release properties file
        """
        self.private_config_xml_file = cfg.query('maven', 'settings_file')
        self.profile = cfg.query('maven', 'environment')
        self.release_version = release_version
        self.release_species_inventory_table = cfg.query('release', 'inventory_table')
        self.staging_release_folder = get_release_folder(release_version)
        self.public_ftp_release_base_folder = cfg.query('release', 'public_ftp_release_base_folder')
        self.public_ftp_current_release_folder = os.path.join(self.public_ftp_release_base_folder,
                                                              f"release_{self.release_version}")
        self.public_ftp_previous_release_folder = os.path.join(self.public_ftp_release_base_folder,
                                                               f"release_{self.release_version - 1}")


def get_current_release_folder_for_taxonomy(taxonomy_id, release_properties, metadata_connection_handle):
    """
    Get info on current and previous release assemblies for the given taxonomy
    """

    def info_for_release_version(version):
        results = get_all_results_for_query(metadata_connection_handle,
                                            f"""select distinct release_folder_name
                                            from {release_properties.release_species_inventory_table}
                                            where taxonomy = '{taxonomy_id}'
                                            and release_version = {version}""")
        return results[0][0] if len(results) > 0 else None

    current_release_folder = info_for_release_version(release_properties.release_version)

    return current_release_folder


def get_release_assemblies_info_for_taxonomy(taxonomy_id, release_properties, metadata_connection_handle):
    """
    Get info on current and previous release assemblies for the given taxonomy
    """
    results = get_all_results_for_query(metadata_connection_handle,
                                        f"""select row_to_json(row) from
                                        (select * from {release_properties.release_species_inventory_table}
                                        where taxonomy = '{taxonomy_id}'
                                        and release_version in ({release_properties.release_version},
                                        {release_properties.release_version} - 1)) row""")

    if len(results) == 0:
        raise Exception("Could not find assemblies pertaining to taxonomy ID: " + taxonomy_id)
    return [result[0] for result in results]


def get_release_file_list_for_assembly(release_assembly_info):
    """
    Get list of release files at assembly level
    for example, see here, ftp://ftp.ebi.ac.uk/pub/databases/eva/rs_releases/release_1/by_assembly/GCA_000001515.4/)
    """
    taxonomy = release_assembly_info['taxonomy']
    assembly_accession = release_assembly_info["assembly_accession"]
    vcf_files = [f"{taxonomy}_{assembly_accession}_{category}.vcf.gz" for category in release_vcf_file_categories]
    text_files = [f"{taxonomy}_{assembly_accession}_{category}.txt.gz" for category in release_text_file_categories]
    csi_files = [f"{filename}.csi" for filename in vcf_files]
    release_file_list = vcf_files + text_files + csi_files + ["README_rs_ids_counts.txt"]
    return sorted(release_file_list)


def get_folder_path_for_assembly(release_folder_base, assembly_accession):
    return os.path.join(release_folder_base, by_assembly_folder_name, assembly_accession)


def get_folder_path_for_species(release_folder_base, species_release_folder_name):
    return os.path.join(release_folder_base, by_species_folder_name, species_release_folder_name)


def get_folder_path_for_species_assembly(release_folder_base, species_release_folder_name, assembly_accession):
    return os.path.join(release_folder_base, by_species_folder_name, species_release_folder_name, assembly_accession)


def create_symlink_to_species_folder_from_assembly_folder(current_release_assembly_info, release_properties,
                                                          public_release_assembly_folder):
    assembly_accession = current_release_assembly_info["assembly_accession"]
    species_release_folder_name = current_release_assembly_info["release_folder_name"]
    public_release_assembly_species_folder = os.path.join(public_release_assembly_folder, species_release_folder_name)
    public_release_species_assembly_folder = os.path.join(release_properties.public_ftp_current_release_folder,
                                                          by_species_folder_name, species_release_folder_name,
                                                          assembly_accession)
    if os.path.isdir(public_release_assembly_species_folder):
        run_command_with_output(f"""Creating symlink from assembly folder {public_release_assembly_species_folder} to
                                species folder {public_release_species_assembly_folder}""",
                                'bash -c "cd {0} && ln -sfT {1} {2}"'.format(
                                    public_release_assembly_folder,
                                    os.path.relpath(public_release_species_assembly_folder,
                                    public_release_assembly_folder),
                                    public_release_assembly_species_folder))
    else:
        raise Exception(f'The species folder {public_release_assembly_species_folder} we're linking to does not exist')


def recreate_public_release_species_assembly_folder(assembly_accession, public_release_species_assembly_folder):
    run_command_with_output(f"Removing species assembly folder for {assembly_accession}...",
                            f"rm -rf {public_release_species_assembly_folder}")
    run_command_with_output(f"Creating species assembly folder for {assembly_accession}...",
                            f"mkdir -p {public_release_species_assembly_folder}")


def copy_current_assembly_data_to_ftp(current_release_assembly_info, release_properties,
                                      public_release_species_assembly_folder):
    assembly_accession = current_release_assembly_info["assembly_accession"]
    species_release_source_folder_name = get_release_folder_name(current_release_assembly_info['taxonomy'])
    md5sum_output_file = os.path.join(public_release_species_assembly_folder, "md5checksums.txt")
    run_command_with_output(f"Removing md5 checksum file {md5sum_output_file} for assembly if it exists...",
                            f"rm -f {md5sum_output_file}")

    recreate_public_release_species_assembly_folder(assembly_accession, public_release_species_assembly_folder)

    for filename in get_release_file_list_for_assembly(current_release_assembly_info):
        source_file_path = os.path.join(release_properties.staging_release_folder, species_release_source_folder_name,
                                        assembly_accession, filename)
        run_command_with_output(f"Copying {filename} to {public_release_species_assembly_folder}...",
                                f"cp {source_file_path} {public_release_species_assembly_folder}")
        if filename.endswith(release_file_types_to_be_checksummed):
            md5sum_output = run_command_with_output("Checksumming file {0}...".format(filename),
                                                    f"(md5sum {source_file_path} | awk '{{ print $1 }}')",
                                                    return_process_output=True)
            open(md5sum_output_file, "a").write(md5sum_output.strip() + "\t" +
                                                os.path.basename(source_file_path) + "\n")

def create_public_release_assembly_folder_if_not_exists(assembly_accession, public_release_assembly_folder):
    if not os.path.exists(public_release_assembly_folder):
        run_command_with_output(f"Creating release folder for {assembly_accession}...",
                                f"mkdir -p {public_release_assembly_folder}")


def publish_assembly_release_files_to_ftp(current_release_assembly_info, release_properties,
                                          public_release_assembly_folder, species_current_release_folder_name):
    assembly_accession = current_release_assembly_info["assembly_accession"]
    public_release_species_assembly_folder = \
        get_folder_path_for_species_assembly(release_properties.public_ftp_current_release_folder,
                                             species_current_release_folder_name, assembly_accession)
    # If a species was processed during this release, copy current release data to FTP
    if current_release_assembly_info["should_be_released"] and \
            current_release_assembly_info["num_rs_to_release"] > 0:
        copy_current_assembly_data_to_ftp(current_release_assembly_info, release_properties,
                                          public_release_species_assembly_folder)

        # Symlink to release README_general_info file - See layout in the link below:
        # https://docs.google.com/presentation/d/1cishRa6P6beIBTP8l1SgJfz71vQcCm5XLmSA8Hmf8rw/edit#slide=id.g63fd5cd489_0_0
        run_command_with_output(
            f"""Symlinking to release level {readme_general_info_file} and {readme_known_issues_file} files for 
            assembly {assembly_accession}""",
            'bash -c "cd {1} && ln -sfT {0}/{2} {1}/{2} && ln -sfT {0}/{3} {1}/{3}"'.format(
                os.path.relpath(release_properties.public_ftp_current_release_folder, public_release_species_assembly_folder),
                public_release_species_assembly_folder,
                readme_general_info_file,
                readme_known_issues_file
            )
        )

    if current_release_assembly_info["num_rs_to_release"] > 0:
        # Create a link from assembly folder to species_folder ex: by_assembly/GCA_000005005.5/zea_mays to by_species/zea_mays/GCA_000005005.5
        create_symlink_to_species_folder_from_assembly_folder(current_release_assembly_info, release_properties,
                                                              public_release_assembly_folder)


def get_release_assemblies_for_release_version(assemblies_to_process, release_version):
    return list(filter(lambda x: x["release_version"] == release_version, assemblies_to_process))


def copy_unmapped_files(source_folder_to_copy_from, species_current_release_folder_path, copy_from_current_release):
    def copy_file(src, dest, copy_command):
        run_command_with_output(f"Copying file {src} to {dest}...", f"{copy_command} {src} {dest}")

    species_level_files_to_copy = (unmapped_ids_file_regex, "md5checksums.txt", "README_unmapped_rs_ids_count.txt")

    # Copy files from current release folder
    if copy_from_current_release:
        for filename in species_level_files_to_copy:
            absolute_file_path = os.path.join(source_folder_to_copy_from, filename)
            copy_file(absolute_file_path, species_current_release_folder_path, "cp")
    else:
        unmapped_variants_files = glob.glob(f"{source_folder_to_copy_from}/{unmapped_ids_file_regex}")
        assert len(unmapped_variants_files) <= 1, \
            f'Multiple unmapped variant files found in source folder: {" ".join(unmapped_variants_files)}'
        assert len(unmapped_variants_files) > 0, \
            f"No unmapped variant files found in source folder: {source_folder_to_copy_from}"
        # Ensure that the species name is properly replaced (ex: mouse_10090 renamed to mus_musculus)
        # in the unmapped variants file name
        unmapped_variants_file_path_current_release = \
            os.path.join(species_current_release_folder_path,
                         unmapped_ids_file_regex.replace("*", os.path.basename(species_current_release_folder_path)))
        copy_file(unmapped_variants_files[0], unmapped_variants_file_path_current_release, "ln -f")
        # Compute MD5 checksum file
        run_command_with_output("Compute MD5 checksum",
                                "(md5sum {0} | awk '{{print $1}}' | xargs -i echo -e '{{}}\t{1}') > {2}"
                                .format(unmapped_variants_file_path_current_release,
                                        os.path.basename(unmapped_variants_file_path_current_release),
                                        os.path.join(species_current_release_folder_path,
                                                     species_level_files_to_copy[1])))
        # Compute unmapped variants count and populate it in the README file
        run_command_with_output("Compute MD5 checksum",
                                "(zcat {0} | grep -v ^# | cut -f1 | sort | uniq | wc -l "
                                "| xargs -i echo -e '# Unique RS ID counts\n{1}\t{{}}') > {2}"
                                .format(unmapped_variants_file_path_current_release,
                                        os.path.basename(unmapped_variants_file_path_current_release),
                                        os.path.join(species_current_release_folder_path,
                                                     species_level_files_to_copy[2])))


def publish_species_level_files_to_ftp(release_properties, species_current_release_folder_name):
    species_staging_release_folder_path = os.path.join(release_properties.staging_release_folder,
                                                       species_current_release_folder_name)
    species_current_release_folder_path = \
        get_folder_path_for_species(release_properties.public_ftp_current_release_folder,
                                    species_current_release_folder_name)
    species_previous_release_folder_path = \
        get_folder_path_for_species(release_properties.public_ftp_previous_release_folder,
                                    species_current_release_folder_name)

    # Determine if the unmapped data should be copied from the current or a previous release
    copy_from_current_release = len(glob.glob(os.path.join(species_staging_release_folder_path,
                                                           unmapped_ids_file_regex))) > 0
    source_folder_to_copy_from = species_staging_release_folder_path if copy_from_current_release \
        else species_previous_release_folder_path

    copy_unmapped_files(source_folder_to_copy_from, species_current_release_folder_path, copy_from_current_release)


def publish_release_top_level_files_to_ftp(release_properties):
    grep_command_for_release_level_files = "grep " + " ".join(['-e "{0}"'.format(filename) for filename in
                                                               release_top_level_files_to_copy])
    run_command_with_output(f"""Copying release level files from {release_properties.staging_release_folder} to
                                {release_properties.public_ftp_current_release_folder}....""",
                            f"""(find {script_dir} -maxdepth 1 -type f | 
                            {grep_command_for_release_level_files} | 
                            xargs -i rsync -av {{}} {release_properties.public_ftp_current_release_folder})""")


def create_requisite_folders(release_properties):
    run_command_with_output("Creating by_species folder for the current release...",
                            f"""mkdir -p {os.path.join(release_properties.public_ftp_current_release_folder,
                                                       by_species_folder_name)}""")
    run_command_with_output("Creating by_assembly folder for the current release...",
                            f"""mkdir -p {os.path.join(release_properties.public_ftp_current_release_folder,
                                                       by_assembly_folder_name)}""")


def create_species_folder(release_properties, species_current_release_folder_name):
    species_current_release_folder_path = \
        get_folder_path_for_species(release_properties.public_ftp_current_release_folder,
                                    species_current_release_folder_name)

    run_command_with_output(f"Creating species release folder {species_current_release_folder_path}...",
                            f"rm -rf {species_current_release_folder_path} && mkdir {species_current_release_folder_path}")


def publish_release_files_to_ftp(release_version, taxonomy_id):
    release_properties = ReleaseProperties(release_version)
    create_requisite_folders(release_properties)
    # Release README, known issues etc.,
    publish_release_top_level_files_to_ftp(release_properties)

    with get_metadata_connection_handle(
            release_properties.profile, release_properties.private_config_xml_file) as metadata_connection_handle:
        assemblies_to_process = get_release_assemblies_info_for_taxonomy(taxonomy_id, release_properties,
                                                                         metadata_connection_handle)
        species_has_unmapped_data = "Unmapped" in set([assembly_info["assembly_accession"] for assembly_info in
                                                       assemblies_to_process])

        # Publish species level data
        species_current_release_folder_name = get_current_release_folder_for_taxonomy(taxonomy_id, release_properties,
                                                                                      metadata_connection_handle)

        create_species_folder(release_properties, species_current_release_folder_name)

        # Unmapped variant data is published at the species level
        # because they are not mapped to any assemblies (duh!)
        if species_has_unmapped_data:
            publish_species_level_files_to_ftp(release_properties, species_current_release_folder_name)

        # Publish assembly level data
        for current_release_assembly_info in \
                get_release_assemblies_for_release_version(assemblies_to_process, release_properties.release_version):
            if current_release_assembly_info["assembly_accession"] != "Unmapped":
                assembly_accession = current_release_assembly_info["assembly_accession"]
                public_release_assembly_folder = get_folder_path_for_assembly(
                    release_properties.public_ftp_current_release_folder, assembly_accession)
                create_public_release_assembly_folder_if_not_exists(assembly_accession, public_release_assembly_folder)

                publish_assembly_release_files_to_ftp(current_release_assembly_info, release_properties,
                                                      public_release_assembly_folder,
                                                      species_current_release_folder_name)

        # Symlinks with assembly names in the species folder ex: Sorbi1 -> GCA_000003195.1
        create_assembly_name_symlinks(get_folder_path_for_species(release_properties.public_ftp_current_release_folder,
                                                                  species_current_release_folder_name))


def main():
    argparse = ArgumentParser(description='Publish release files to FTP')
    argparse.add_argument("--release_version", type=int, required=True)
    argparse.add_argument('--taxonomy_id', required=True, type=int, help='ex: 9913')

    args = argparse.parse_args()
    load_config()
    logging_config.add_stdout_handler()

    publish_release_files_to_ftp(args.release_version, args.taxonomy_id)


if __name__ == "__main__":
    main()
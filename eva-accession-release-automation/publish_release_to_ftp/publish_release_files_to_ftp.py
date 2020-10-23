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

import click
import os
import psycopg2

from publish_release_to_ftp.create_assembly_name_symlinks import create_assembly_name_symlinks
from ebi_eva_common_pyutils.command_utils import run_command_with_output
from ebi_eva_common_pyutils.config_utils import get_pg_metadata_uri_for_eva_profile, get_properties_from_xml_file
from ebi_eva_common_pyutils.logger import logging_config
from ebi_eva_common_pyutils.pg_utils import get_all_results_for_query
from run_release_in_embassy.run_release_for_species import get_common_release_properties
from run_release_in_embassy.release_metadata import release_vcf_file_categories, release_text_file_categories

by_assembly_folder_name = "by_assembly"
by_species_folder_name = "by_species"
release_file_types_to_be_checksummed = ("vcf.gz", "txt.gz", "tbi")
readme_general_info_file = "README_release_general_info.txt"
readme_known_issues_file = "README_release_known_issues.txt"
release_top_level_files_to_copy = ("README_release_known_issues.txt", readme_general_info_file,
                                   "species_name_mapping.tsv")
unmapped_ids_file_regex = ".*_unmapped_ids.txt.gz"
logger = logging_config.get_logger(__name__)


class ReleaseProperties:
    def __init__(self, common_release_properties_file):
        """
        Get release properties from common release properties file
        """
        common_release_properties = get_common_release_properties(common_release_properties_file)
        self.private_config_xml_file = common_release_properties["private-config-xml-file"]
        self.release_version = common_release_properties["release-version"]
        self.release_species_inventory_table = common_release_properties["release-species-inventory-table"]
        self.staging_release_folder = common_release_properties["release-folder"]
        self.public_ftp_release_base_folder = common_release_properties["public-ftp-release-base-folder"]
        self.public_ftp_current_release_folder = os.path.join(self.public_ftp_release_base_folder,
                                                              "release_{0}".format(self.release_version))
        self.public_ftp_previous_release_folder = os.path.join(self.public_ftp_release_base_folder,
                                                               "release_{0}".format(self.release_version - 1))


def get_current_and_previous_release_folders_for_taxonomy(taxonomy_id, release_properties, metadata_connection_handle):
    """
    Get info on current and previous release assemblies for the given taxonomy
    """

    def info_for_release_version(version):
        results = get_all_results_for_query(metadata_connection_handle,
                                            "select distinct release_folder_name from {0} "
                                            "where taxonomy_id = '{1}' "
                                            "and release_version = {2}"
                                            .format(release_properties.release_species_inventory_table, taxonomy_id,
                                                    version))
        return results[0][0] if len(results) > 0 else None

    current_release_folder = info_for_release_version(release_properties.release_version)
    previous_release_folder = info_for_release_version(release_properties.release_version - 1)

    return current_release_folder, previous_release_folder


def get_release_assemblies_info_for_taxonomy(taxonomy_id, release_properties, metadata_connection_handle):
    """
    Get info on current and previous release assemblies for the given taxonomy
    """
    results = get_all_results_for_query(metadata_connection_handle, "select row_to_json(row) from "
                                                                    "(select * from {0} "
                                                                    "where taxonomy_id = '{1}' "
                                                                    "and release_version in ({2}, {2} - 1)) row"
                                        .format(release_properties.release_species_inventory_table, taxonomy_id,
                                                release_properties.release_version))
    if len(results) == 0:
        raise Exception("Could not find assemblies pertaining to taxonomy ID: " + taxonomy_id)
    return [result[0] for result in results]


def get_release_file_list_for_assembly(release_assembly_info):
    """
    Get list of release files at assembly level
    for example, see here, ftp://ftp.ebi.ac.uk/pub/databases/eva/rs_releases/release_1/by_assembly/GCA_000001515.4/)
    """
    assembly_accession = release_assembly_info["assembly"]
    vcf_files = ["{0}_{1}.vcf.gz".format(assembly_accession, category) for category in release_vcf_file_categories]
    text_files = ["{0}_{1}.txt.gz".format(assembly_accession, category) for category in release_text_file_categories]
    tbi_files = ["{0}.tbi".format(filename) for filename in vcf_files]
    release_file_list = vcf_files + text_files + tbi_files + ["README_rs_ids_counts.txt"]
    return sorted(release_file_list)


def get_folder_path_for_assembly(release_folder_base, assembly_accession):
    return os.path.join(release_folder_base, by_assembly_folder_name, assembly_accession)


def get_folder_path_for_species(release_folder_base, species_release_folder_name):
    return os.path.join(release_folder_base, by_species_folder_name, species_release_folder_name)


def create_symlink_to_assembly_folder_from_species_folder(current_release_assembly_info, release_properties,
                                                          public_release_assembly_folder):
    """
    Create linkage to assembly folders from the species folder
    See FTP layout linkage between by_assembly and by_species folders in the link below:
    https://docs.google.com/presentation/d/1cishRa6P6beIBTP8l1SgJfz71vQcCm5XLmSA8Hmf8rw/edit#slide=id.g63fd5cd489_0_0
    """
    assembly_accession = current_release_assembly_info["assembly"]
    species_release_folder_name = current_release_assembly_info["release_folder_name"]
    public_release_species_folder = os.path.join(release_properties.public_ftp_current_release_folder,
                                                 by_species_folder_name, species_release_folder_name)
    run_command_with_output("Creating symlink from species folder {0} to assembly folder {1}"
                            .format(public_release_species_folder, public_release_assembly_folder),
                            'bash -c "cd {1} && ln -sfT {0} {1}/{2}"'.format(
                                os.path.relpath(public_release_assembly_folder, public_release_species_folder),
                                public_release_species_folder, assembly_accession))


def recreate_public_release_assembly_folder(assembly_accession, public_release_assembly_folder):
    run_command_with_output("Removing release folder if it exists for {0}...".format(assembly_accession),
                            "rm -rf " + public_release_assembly_folder)
    run_command_with_output("Creating release folder for {0}...".format(assembly_accession),
                            "mkdir -p " + public_release_assembly_folder)


def copy_current_assembly_data_to_ftp(current_release_assembly_info, release_properties,
                                      public_release_assembly_folder):
    assembly_accession = current_release_assembly_info["assembly"]
    species_release_folder_name = current_release_assembly_info["release_folder_name"]
    md5sum_output_file = os.path.join(public_release_assembly_folder, "md5checksums.txt")
    run_command_with_output("Removing md5 checksum file {0} for assembly if it exists...".format(md5sum_output_file),
                            "rm -f " + md5sum_output_file)
    recreate_public_release_assembly_folder(assembly_accession, public_release_assembly_folder)

    for filename in get_release_file_list_for_assembly(current_release_assembly_info):
        source_file_path = os.path.join(release_properties.staging_release_folder, species_release_folder_name,
                                        assembly_accession, filename)
        run_command_with_output("Copying {0} to {1}...".format(filename, public_release_assembly_folder),
                                "rsync -av {0} {1}".format(source_file_path, public_release_assembly_folder))
        if filename.endswith(release_file_types_to_be_checksummed):
            md5sum_output = run_command_with_output("Checksumming file {0}...".format(filename),
                                                    "md5sum " + source_file_path, return_process_output=True)
            open(md5sum_output_file, "a").write(md5sum_output)


def hardlink_to_previous_release_assembly_files_in_ftp(current_release_assembly_info, release_properties):
    assembly_accession = current_release_assembly_info["assembly"]
    public_current_release_assembly_folder = \
        get_folder_path_for_assembly(release_properties.public_ftp_current_release_folder, assembly_accession)
    public_previous_release_assembly_folder = \
        get_folder_path_for_assembly(release_properties.public_ftp_previous_release_folder, assembly_accession)

    if os.path.exists(public_previous_release_assembly_folder):
        recreate_public_release_assembly_folder(assembly_accession, public_current_release_assembly_folder)
        for filename in get_release_file_list_for_assembly(current_release_assembly_info):
            file_to_hardlink = "{0}/{1}".format(public_previous_release_assembly_folder, filename)
            if os.path.exists(file_to_hardlink):
                run_command_with_output("Creating hardlink from previous release assembly folder {0} "
                                        "to current release assembly folder {1}"
                                        .format(public_current_release_assembly_folder,
                                                public_previous_release_assembly_folder)
                                        , 'ln -f {0} {1}'.format(file_to_hardlink,
                                                                 public_current_release_assembly_folder))
    else:
        raise Exception("Previous release folder {0} does not exist for assembly!"
                        .format(public_previous_release_assembly_folder))


def publish_assembly_release_files_to_ftp(current_release_assembly_info, release_properties):
    assembly_accession = current_release_assembly_info["assembly"]
    public_release_assembly_folder = \
        get_folder_path_for_assembly(release_properties.public_ftp_current_release_folder, assembly_accession)
    # If a species was processed during this release, copy current release data to FTP
    if current_release_assembly_info["should_be_processed"] and \
            current_release_assembly_info["number_variants_to_process"] > 0:
        copy_current_assembly_data_to_ftp(current_release_assembly_info, release_properties,
                                          public_release_assembly_folder)
    else:
        # Since the assembly data is unchanged from the last release, hard-link instead of symlink to older release data
        # so that deleting data in older releases does not impact the newer releases
        # (hard-linking preserves the underlying data for a link until all links to that data are deleted)
        hardlink_to_previous_release_assembly_files_in_ftp(current_release_assembly_info, release_properties)

    # Symlink to release README_general_info file - See layout in the link below:
    # https://docs.google.com/presentation/d/1cishRa6P6beIBTP8l1SgJfz71vQcCm5XLmSA8Hmf8rw/edit#slide=id.g63fd5cd489_0_0
    run_command_with_output("Symlinking to release level {0} and {1} files for assembly {1}"
                            .format(readme_general_info_file, readme_known_issues_file, assembly_accession),
                            'bash -c "cd {1} && ln -sfT {0}/{2} {1}/{2} && ln -sfT {0}/{3} {1}/{3}"'
                            .format(os.path.relpath(release_properties.public_ftp_current_release_folder,
                                                    public_release_assembly_folder), public_release_assembly_folder,
                                    readme_general_info_file, readme_known_issues_file))
    # Create a link from species folder ex: by_species/ovis_aries to point to this assembly folder
    create_symlink_to_assembly_folder_from_species_folder(current_release_assembly_info, release_properties,
                                                          public_release_assembly_folder)


def get_release_assemblies_for_release_version(assemblies_to_process, release_version):
    return list(filter(lambda x: x["release_version"] == release_version, assemblies_to_process))


def publish_species_level_files_to_ftp(taxonomy_id, release_properties, species_current_release_folder_name,
                                       species_previous_release_folder_name):
    species_staging_release_folder_path = os.path.join(release_properties.staging_release_folder,
                                                       species_current_release_folder_name)
    species_current_release_folder_path = \
        get_folder_path_for_species(release_properties.public_ftp_current_release_folder,
                                    species_current_release_folder_name)
    species_previous_release_folder_path = \
        get_folder_path_for_species(release_properties.public_ftp_previous_release_folder,
                                    species_previous_release_folder_name)
    source_folder_to_copy_from, copy_command = species_staging_release_folder_path, "rsync -av"
    species_level_files_to_copy = ("md5checksums.txt", "README_unmapped_rs_ids_count.txt", unmapped_ids_file_regex)
    grep_command_for_species_level_files = "grep " + " ".join(['-e "{0}"'.format(filename) for filename in
                                                               species_level_files_to_copy])
    run_command_with_output("Creating species release folder {0}...".format(species_current_release_folder_path),
                            "mkdir -p {0}".format(species_current_release_folder_path))

    def count_num_species_level_files_in_folder(folder):
        return int(run_command_with_output("Checking for species level release files in the folder {0}..."
                                           .format(folder),
                                           "(ls -1 {0} | {1} | wc -l)".format(folder,
                                                                              grep_command_for_species_level_files),
                                           return_process_output=True))

    num_species_level_files_in_staging_release_folder = \
        count_num_species_level_files_in_folder(species_staging_release_folder_path)
    num_species_level_files_in_previous_release_folder = \
        count_num_species_level_files_in_folder(species_previous_release_folder_path)

    if num_species_level_files_in_staging_release_folder == 0:
        if num_species_level_files_in_previous_release_folder == 0:
            raise Exception("Unmapped variants file could not be found in current or previous release "
                            "for taxonomy/species {0}/{1}!".format(taxonomy_id, species_current_release_folder_name))
        else:
            # Use hard-linking if copying from previous release
            source_folder_to_copy_from, copy_command = species_previous_release_folder_path, "ln -f"

    run_command_with_output("Copying species level release files from {0} to {1}..."
                            .format(source_folder_to_copy_from, species_current_release_folder_path),
                            "(find {0} -maxdepth 1 -type f | {1} | xargs -i {2} {{}} {3})"
                            .format(source_folder_to_copy_from, grep_command_for_species_level_files,
                                    copy_command, species_current_release_folder_path),
                            return_process_output=True)


def publish_release_top_level_files_to_ftp(release_properties):
    grep_command_for_release_level_files = "grep " + " ".join(['-e "{0}"'.format(filename) for filename in
                                                               release_top_level_files_to_copy])
    run_command_with_output("Copying release level files from {0} to {1}...."
                            .format(release_properties.staging_release_folder,
                                    release_properties.public_ftp_current_release_folder),
                            "(find {0} -maxdepth 1 -type f | {1} | xargs -i rsync -av {{}} {2})".format(
                                release_properties.staging_release_folder, grep_command_for_release_level_files,
                                release_properties.public_ftp_current_release_folder))


def create_requisite_folders(release_properties):
    run_command_with_output("Creating by_species folder for the current release...",
                            "mkdir -p " + os.path.join(release_properties.public_ftp_current_release_folder,
                                                       by_species_folder_name))
    run_command_with_output("Creating by_assembly folder for the current release...",
                            "mkdir -p " + os.path.join(release_properties.public_ftp_current_release_folder,
                                                       by_assembly_folder_name))


def publish_release_files_to_ftp(common_release_properties_file, taxonomy_id):
    release_properties = ReleaseProperties(common_release_properties_file)
    create_requisite_folders(release_properties)
    # Release README, known issues etc.,
    publish_release_top_level_files_to_ftp(release_properties)

    metadata_password = get_properties_from_xml_file("development",
                                                     release_properties.private_config_xml_file)["eva.evapro.password"]
    with psycopg2.connect(get_pg_metadata_uri_for_eva_profile("development",
                                                              release_properties.private_config_xml_file),
                          user="evadev", password=metadata_password) as metadata_connection_handle:
        assemblies_to_process = get_release_assemblies_info_for_taxonomy(taxonomy_id, release_properties,
                                                                         metadata_connection_handle)
        species_has_unmapped_data = "Unmapped" in set([assembly_info["assembly"] for assembly_info in
                                                       assemblies_to_process])

        # Publish species level data
        species_current_release_folder_name, species_previous_release_folder_name = \
            get_current_and_previous_release_folders_for_taxonomy(taxonomy_id, release_properties,
                                                                  metadata_connection_handle)
        # Unmapped variant data is published at the species level
        # because they are not mapped to any assemblies (duh!)
        if species_has_unmapped_data:
            publish_species_level_files_to_ftp(taxonomy_id, release_properties, species_current_release_folder_name,
                                               species_previous_release_folder_name)

        # Publish assembly level data
        for current_release_assembly_info in \
                get_release_assemblies_for_release_version(assemblies_to_process, release_properties.release_version):
            if current_release_assembly_info["assembly"] != "Unmapped":
                publish_assembly_release_files_to_ftp(current_release_assembly_info, release_properties)

        # Symlinks with assembly names in the species folder ex: Sorbi1 -> GCA_000003195.1
        create_assembly_name_symlinks(get_folder_path_for_species(release_properties.public_ftp_current_release_folder,
                                                                  species_current_release_folder_name))


@click.option("--common-release-properties-file", help="ex: /path/to/release/properties.yml", required=True)
@click.option("--taxonomy-id", help="ex: 9913", required=True)
@click.command()
def main(common_release_properties_file, taxonomy_id):
    publish_release_files_to_ftp(common_release_properties_file, taxonomy_id)


if __name__ == "__main__":
    main()

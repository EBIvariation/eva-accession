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
import os

import click
from ebi_eva_common_pyutils.logger import logging_config
from ebi_eva_internal_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_internal_pyutils.spring_properties import SpringPropertiesGenerator

from run_release_in_embassy.release_common_utils import get_release_db_name_in_tempmongo_instance
from run_release_in_embassy.release_metadata import get_release_inventory_info_for_assembly


def get_release_properties_for_assembly(private_config_xml_file, profile, taxonomy_id, assembly_accession,
                                        release_species_inventory_table, release_version):
    with get_metadata_connection_handle(profile, private_config_xml_file) as metadata_connection_handle:
        release_inventory_info_for_assembly = get_release_inventory_info_for_assembly(taxonomy_id, assembly_accession,
                                                                                      release_species_inventory_table,
                                                                                      release_version,
                                                                                      metadata_connection_handle)
    release_inventory_info_for_assembly["mongo_accessioning_db"] = \
        get_release_db_name_in_tempmongo_instance(taxonomy_id, assembly_accession)
    return release_inventory_info_for_assembly


def create_release_properties_file_for_assembly(private_config_xml_file, profile, taxonomy_id, assembly_accession,
                                                release_species_inventory_table, release_version,
                                                assembly_release_folder):
    os.makedirs(assembly_release_folder, exist_ok=True)
    output_file = "{0}/{1}_release.properties".format(assembly_release_folder, assembly_accession)
    release_properties = get_release_properties_for_assembly(
        private_config_xml_file, profile, taxonomy_id, assembly_accession, release_species_inventory_table,
        release_version
    )
    properties_string = SpringPropertiesGenerator(profile, private_config_xml_file).get_release_properties(
        temp_mongo_db=release_properties['mongo_accessioning_db'],
        job_name='ACCESSION_RELEASE_JOB',
        assembly_accession=assembly_accession,
        taxonomy_accession=taxonomy_id,
        fasta=release_properties['fasta_path'],
        assembly_report=release_properties['report_path'],
        contig_naming='SEQUENCE_NAME',
        output_folder=assembly_release_folder
    )
    open(output_file, "w").write(properties_string)
    return output_file


@click.option("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
@click.option("--profile", help="Maven profile to use, ex: internal", required=True)
@click.option("--taxonomy-id", help="ex: 9913", required=True)
@click.option("--assembly-accession", help="ex: GCA_000003055.6", required=True)
@click.option("--release-species-inventory-table", default="eva_progress_tracker.clustering_release_tracker",
              required=False)
@click.option("--release-version", help="ex: 2", type=int, required=True)
@click.option("--species-release-folder", required=True)
@click.command()
def main(private_config_xml_file, profile, taxonomy_id, assembly_accession, release_species_inventory_table,
         release_version, species_release_folder):
    logging_config.add_stdout_handler()
    create_release_properties_file_for_assembly(private_config_xml_file, profile, taxonomy_id, assembly_accession,
                                                release_species_inventory_table, release_version,
                                                species_release_folder)


if __name__ == "__main__":
    main()

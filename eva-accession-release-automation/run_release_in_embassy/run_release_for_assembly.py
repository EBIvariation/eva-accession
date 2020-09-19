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

import click
import logging
import sys
import traceback

from run_release_in_embassy.create_release_properties_file import create_release_properties_file_for_assembly
from run_release_in_embassy.release_common_utils import open_mongo_port_to_tempmongo, close_mongo_port_to_tempmongo
from ebi_eva_common_pyutils.command_utils import run_command_with_output


logger = logging.getLogger(__name__)


def run_release_for_assembly(private_config_xml_file, taxonomy_id, assembly_accession, release_species_inventory_table,
                             release_folder, release_jar_path, job_repo_url, memory):
    exit_code = 0
    try:
        port_forwarding_process_id, mongo_port = open_mongo_port_to_tempmongo(private_config_xml_file, taxonomy_id,
                                                                              release_species_inventory_table)
        release_properties_file = create_release_properties_file_for_assembly(private_config_xml_file, taxonomy_id,
                                                                              assembly_accession,
                                                                              release_species_inventory_table,
                                                                              release_folder, job_repo_url)
        release_command = 'java -Xmx{0}g -jar {1} --spring.config.location="{2}" -Dspring.data.mongodb.port={3}'\
            .format(memory, release_jar_path, release_properties_file, mongo_port)
        run_command_with_output("Running release pipeline for assembly: " + assembly_accession, release_command)
    except Exception as ex:
        logger.error("Encountered an error while running release for assembly: " + assembly_accession + "\n"
                     + traceback.format_exc())
        exit_code = -1
    finally:
        close_mongo_port_to_tempmongo(port_forwarding_process_id)
        logger.info("Release process completed with exit_code: " + str(exit_code))
        sys.exit(exit_code)


@click.option("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
@click.option("--taxonomy-id", help="ex: 9913", required=True)
@click.option("--assembly-accession", help="ex: GCA_000003055.6", required=True)
@click.option("--release-species-inventory-table", default="dbsnp_ensembl_species.release_species_inventory",
              required=False)
@click.option("--release-folder", required=True)
@click.option("--release-jar-path", required=True)
# TODO: Production Spring Job repository URL won't be used for Release 2
#  since it hasn't been upgraded to support Spring Boot 2 metadata schema. Therefore a separate job repository
#  has been created (with similar credentials)  and passed in through the job_repo_url property.
#  The following argument is not needed after the production repository upgrade to the Spring Boot 2 metadata schema
@click.option("--job-repo-url", required=True)
@click.option("--memory",  help="Memory in GB. ex: 8", default=8, type=int, required=False)
@click.command()
def main(private_config_xml_file, taxonomy_id, assembly_accession, release_species_inventory_table, release_folder,
         release_jar_path, job_repo_url, memory):
    run_release_for_assembly(private_config_xml_file, taxonomy_id, assembly_accession, release_species_inventory_table,
                             release_folder, release_jar_path, job_repo_url, memory)


if __name__ == "__main__":
    main()

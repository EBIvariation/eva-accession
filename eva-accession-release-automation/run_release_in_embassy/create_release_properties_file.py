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
import os
import psycopg2
import textwrap


from run_release_in_embassy.release_common_utils import get_release_db_name_in_tempmongo_instance
from run_release_in_embassy.release_metadata import get_release_inventory_info_for_assembly
from ebi_eva_common_pyutils.common_utils import merge_two_dicts
from ebi_eva_common_pyutils.config_utils import get_pg_metadata_uri_for_eva_profile, EVAPrivateSettingsXMLConfig


def get_release_job_repo_properties(private_config_xml_file, eva_profile_name):
    release_job_repo_properties = {}
    config = EVAPrivateSettingsXMLConfig(private_config_xml_file)
    xpath_location_template = '//settings/profiles/profile/id[text()="{0}"]/../properties/{1}/text()'
    release_job_repo_properties["job_repo_url"] = config.get_value_with_xpath(
        xpath_location_template.format(eva_profile_name, "eva.accession.jdbc.url"))[0]
    release_job_repo_properties["job_repo_username"] = config.get_value_with_xpath(
        xpath_location_template.format(eva_profile_name, "eva.accession.user"))[0]
    release_job_repo_properties["job_repo_password"] = config.get_value_with_xpath(
        xpath_location_template.format(eva_profile_name, "eva.accession.password"))[0]
    return release_job_repo_properties


def get_release_properties_for_assembly(private_config_xml_file, profile, taxonomy_id, assembly_accession,
                                        release_species_inventory_table, release_version, species_release_folder):
    with psycopg2.connect(get_pg_metadata_uri_for_eva_profile(profile, private_config_xml_file),
                          user="evadev") as \
            metadata_connection_handle:
        release_inventory_info_for_assembly = get_release_inventory_info_for_assembly(taxonomy_id, assembly_accession,
                                                                                      release_species_inventory_table,
                                                                                      release_version,
                                                                                      metadata_connection_handle)
    if not release_inventory_info_for_assembly["report_path"].startswith("file:"):
        release_inventory_info_for_assembly["report_path"] = "file:" + \
                                                             release_inventory_info_for_assembly["report_path"]
    release_inventory_info_for_assembly["output_folder"] = os.path.join(species_release_folder, assembly_accession)
    release_inventory_info_for_assembly["mongo_accessioning_db"] = \
        get_release_db_name_in_tempmongo_instance(taxonomy_id)
    return merge_two_dicts(release_inventory_info_for_assembly,
                           get_release_job_repo_properties(private_config_xml_file, profile))


def create_release_properties_file_for_assembly(private_config_xml_file, profile, taxonomy_id, assembly_accession,
                                                release_species_inventory_table, release_version,
                                                species_release_folder):
    assembly_species_release_folder = os.path.join(species_release_folder, assembly_accession)
    os.makedirs(assembly_species_release_folder, exist_ok=True)
    output_file = "{0}/{1}_release.properties".format(assembly_species_release_folder, assembly_accession)
    release_properties = get_release_properties_for_assembly(private_config_xml_file, profile, taxonomy_id, assembly_accession,
                                                             release_species_inventory_table, release_version,
                                                             species_release_folder)
    properties_string = """
        spring.batch.job.names=ACCESSION_RELEASE_JOB
        parameters.assemblyAccession={assembly}
        parameters.assemblyReportUrl={report_path}
        parameters.chunkSize=1000
        parameters.fasta={fasta_path}
        parameters.forceRestart=true
        parameters.outputFolder={output_folder}
        
        # job repository datasource
        spring.datasource.driver-class-name=org.postgresql.Driver
        
        spring.datasource.url={job_repo_url}
        spring.datasource.username={job_repo_username}
        spring.datasource.password={job_repo_password}
        spring.datasource.tomcat.max-active=3
        
        # Only to set up the database!
        # spring.jpa.generate-ddl=true
        
        # To suppress weird error message in Spring Boot 2 
        # See https://github.com/spring-projects/spring-boot/issues/12007#issuecomment-370774241  
        spring.jpa.properties.hibernate.jdbc.lob.non_contextual_creation=true
        spring.data.mongodb.database={mongo_accessioning_db}
        # spring.data.mongodb.username=
        # spring.data.mongodb.password=
        # spring.data.mongodb.authentication-database=admin
        mongodb.read-preference=primaryPreferred
        
        spring.main.web-environment=false
        
        logging.level.uk.ac.ebi.eva.accession.release=INFO
        """.format(**release_properties)
    open(output_file, "w").write(textwrap.dedent(properties_string).lstrip())
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
    create_release_properties_file_for_assembly(private_config_xml_file, profile, taxonomy_id, assembly_accession,
                                                release_species_inventory_table, release_version,
                                                species_release_folder)


if __name__ == "__main__":
    main()

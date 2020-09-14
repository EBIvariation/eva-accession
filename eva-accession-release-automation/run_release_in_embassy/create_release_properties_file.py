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


from run_release_in_embassy.release_metadata import get_release_inventory_info_for_assembly
from ebi_eva_common_pyutils.common_utils import merge_two_dicts
from ebi_eva_common_pyutils.config_utils import get_pg_metadata_uri_for_eva_profile, EVAPrivateSettingsXMLConfig


def get_release_job_repo_properties(private_config_xml_file):
    release_job_repo_properties = {}
    eva_profile_name = "production"
    config = EVAPrivateSettingsXMLConfig(private_config_xml_file)
    xpath_location_template = '//settings/profiles/profile/id[text()="{0}"]/../properties/{1}/text()'
    release_job_repo_properties["job_repo_url"] = config.get_value_with_xpath(
        xpath_location_template.format(eva_profile_name, "eva.accession.jdbc.url"))[0]
    release_job_repo_properties["job_repo_username"] = config.get_value_with_xpath(
        xpath_location_template.format(eva_profile_name, "eva.accession.user"))[0]
    release_job_repo_properties["job_repo_password"] = config.get_value_with_xpath(
        xpath_location_template.format(eva_profile_name, "eva.accession.password"))[0]
    return release_job_repo_properties


def get_release_properties_for_assembly(private_config_xml_file, taxonomy_id, assembly_accession,
                                        release_species_inventory_table, release_folder):
    with psycopg2.connect(get_pg_metadata_uri_for_eva_profile("development", private_config_xml_file),
                          user="evadev") as \
            metadata_connection_handle:
        release_inventory_info_for_assembly = get_release_inventory_info_for_assembly(taxonomy_id, assembly_accession,
                                                                                      release_species_inventory_table,
                                                                                      metadata_connection_handle)
    release_inventory_info_for_assembly["output_folder"] = os.path.join(release_folder, assembly_accession)
    release_inventory_info_for_assembly["mongo_accessioning_db"] = "acc_" + assembly_accession.replace('.', '_')
    return merge_two_dicts(release_inventory_info_for_assembly,
                           get_release_job_repo_properties(private_config_xml_file))


def create_release_properties_file_for_assembly(private_config_xml_file, taxonomy_id, assembly_accession,
                                                release_species_inventory_table, release_folder, job_repo_url):
    assembly_release_folder = os.path.join(release_folder, assembly_accession)
    os.makedirs(assembly_release_folder, exist_ok=True)
    output_file = "{0}/{1}_release.properties".format(assembly_release_folder, assembly_accession)
    release_properties = get_release_properties_for_assembly(private_config_xml_file, taxonomy_id, assembly_accession,
                                                             release_species_inventory_table, release_folder)
    # TODO: Production Spring Job repository URL won't be used for Release 2
    #  since it hasn't been upgraded to support Spring Boot 2 metadata schema. Therefore a separate job repository
    #  has been created (with similar credentials)  and passed in through the job_repo_url parameter.
    #  The following line + job_repo_url parameter should be removed after the
    #  production upgrade to the Spring Boot 2 metadata schema
    release_properties["job_repo_url"] = job_repo_url
    properties_string = """
        spring.batch.job.names=ACCESSION_RELEASE_JOB
        parameters.assemblyAccession={assembly}
        parameters.assemblyReportUrl={report_path}
        parameters.chunkSize=1000
        parameters.fasta={fasta_path}
        parameters.forceRestart=false
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
@click.option("--taxonomy-id", help="ex: 9913", required=True)
@click.option("--assembly-accession", help="ex: GCA_000003055.6", required=True)
@click.option("--release-species-inventory-table", default="dbsnp_ensembl_species.release_species_inventory",
              required=False)
@click.option("--release-folder", required=True)
@click.option("--job-repo-url", required=True)
@click.command()
def main(private_config_xml_file, taxonomy_id, assembly_accession, release_species_inventory_table, release_folder,
         job_repo_url):
    create_release_properties_file_for_assembly(private_config_xml_file, taxonomy_id, assembly_accession,
                                                release_species_inventory_table, release_folder, job_repo_url)


if __name__ == "__main__":
    main()

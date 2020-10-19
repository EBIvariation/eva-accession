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
import collections
import copy
import datetime
import logging
import os
import psycopg2
import yaml

from ebi_eva_common_pyutils.common_utils import merge_two_dicts
from ebi_eva_common_pyutils.config_utils import get_pg_metadata_uri_for_eva_profile
from run_release_in_embassy.release_metadata import get_release_assemblies_for_taxonomy
from run_release_in_embassy.release_common_utils import get_ensembl_scientific_name


logger = logging.getLogger(__name__)
timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
copy_process = "copy_accessioning_collections_to_embassy"
# Processes, in order, that make up the workflow and the arguments that they take
workflow_process_arguments_map = collections.OrderedDict(
    [(copy_process, ["private-config-xml-file", "taxonomy-id",
                     "release-species-inventory-table", "release-version", "dump-dir"]),
     ("run_release_for_assembly", ["private-config-xml-file", "taxonomy-id",
                                   "assembly-accession", "release-species-inventory-table",
                                   "release-version", "species-release-folder", "release-jar-path", "job-repo-url",
                                   "memory"]),
     ("merge_dbsnp_eva_release_files", ["private-config-xml-file", "bgzip-path", "tabix-path", "bcftools-path",
                                        "vcf-sort-script-path", "taxonomy-id", "assembly-accession",
                                        "release-species-inventory-table", "release-version",
                                        "species-release-folder"]),
     ("sort_bgzip_tabix_release_files", ["bgzip-path", "tabix-path",
                                         "vcf-sort-script-path", "assembly-accession",
                                         "species-release-folder"]),
     ("validate_release_vcf_files", ["private-config-xml-file", "taxonomy-id",
                                     "assembly-accession", "release-species-inventory-table", "release-version",
                                     "species-release-folder",
                                     "vcf-validator-path", "assembly-checker-path"]),
     ("analyze_vcf_validation_results", ["species-release-folder", "assembly-accession"]),
     ("count_rs_ids_in_release_files", ["count-ids-script-path", "assembly-accession", "species-release-folder"]),
     ("validate_rs_release_files", ["private-config-xml-file", "taxonomy-id", "assembly-accession",
                                    "release-species-inventory-table", "release-version", "species-release-folder"]),
     ("update_release_status_for_assembly", ["private-config-xml-file", "taxonomy-id", "assembly-accession",
                                             "release-version"])
     ])

workflow_process_template_for_nextflow = """
process {workflow-process-name} {{
    memory='{memory} GB'
    input:
        val flag from {previous-process-output-flag}
    output:
        val true into {current-process-output-flag}
    script:
    \"\"\"
    export PYTHONPATH={script-path} &&  ({python3-path} -m run_release_in_embassy.{process-with-args} 1>> {release-log-file} 2>&1)
    \"\"\"
}}
"""


def get_release_properties_for_current_species(common_release_properties, taxonomy_id, memory):
    release_properties = {"taxonomy-id": taxonomy_id, "memory": memory,
                          "species-release-folder": os.path.join(common_release_properties["release-folder"],
                                                                 get_ensembl_scientific_name(taxonomy_id))}
    os.makedirs(release_properties["species-release-folder"], exist_ok=True)
    release_properties["timestamp"] = timestamp
    release_properties["release-log-file"] = \
        "{species-release-folder}/release_{taxonomy-id}_{timestamp}.log".format(**release_properties)
    release_properties["dump-dir"] = os.path.join(release_properties["species-release-folder"], "dumps")
    os.makedirs(release_properties["dump-dir"], exist_ok=True)
    return release_properties


def get_release_properties_for_current_assembly(species_release_properties, assembly_accession):
    release_properties = {"assembly-accession": assembly_accession,
                          "assembly-release-folder": os.path.join(species_release_properties["species-release-folder"],
                                                                  assembly_accession)}
    os.makedirs(release_properties["assembly-release-folder"], exist_ok=True)
    return release_properties


def get_nextflow_process_definition(assembly_release_properties, workflow_process_name, workflow_process_args,
                                    process_name_suffix=None):
    if process_name_suffix is None:
        process_name_suffix = assembly_release_properties["assembly-accession"].replace('.', '_')
    release_properties = copy.deepcopy(assembly_release_properties)
    release_properties["workflow-process-name"] = workflow_process_name + "_" + process_name_suffix

    release_properties["process-with-args"] = "{0} {1}".format(workflow_process_name,
                                                               " ".join(["--{0} {1}"
                                                                        .format(arg, release_properties[arg])
                                                                         for arg in workflow_process_args]))
    return workflow_process_template_for_nextflow.format(**release_properties)


def prepare_release_workflow_file_for_species(common_release_properties, taxonomy_id, assemblies, memory):
    process_index = 1
    release_properties = merge_two_dicts(common_release_properties,
                                         get_release_properties_for_current_species(common_release_properties,
                                                                                    taxonomy_id, memory))
    # This hack is needed to kick-off the initial process in Nextflow
    release_properties["previous-process-output-flag"] = "true"
    release_properties["current-process-output-flag"] = "flag" + str(process_index)
    # Ensure that PYTHONPATH is properly set so that scripts can be run
    # as "python3 -m run_release_in_embassy.<script_name>"
    release_properties["script-path"] = os.environ["PYTHONPATH"]
    workflow_file_name = os.path.join(release_properties["species-release-folder"],
                                      "{taxonomy-id}_release_workflow_{timestamp}.nf".format(**release_properties))

    with open(workflow_file_name, "w") as workflow_file_handle:
        header = "#!/usr/bin/env nextflow"
        workflow_file_handle.write(header + "\n")
        for assembly_accession in assemblies:
            release_properties = merge_two_dicts(release_properties,
                                                 get_release_properties_for_current_assembly(release_properties,
                                                                                             assembly_accession))
            for workflow_process_name, workflow_process_args in workflow_process_arguments_map.items():
                release_properties["current-process-output-flag"] = "flag" + str(process_index)
                if workflow_process_name == copy_process:
                    # Copy process to Embassy must be carried out only once per species
                    if process_index == 1:
                        workflow_file_handle.write(
                            get_nextflow_process_definition(release_properties, workflow_process_name,
                                                            workflow_process_args,
                                                            process_name_suffix=str(taxonomy_id)))
                    else:
                        continue
                else:
                    workflow_file_handle.write(
                        get_nextflow_process_definition(release_properties, workflow_process_name,
                                                        workflow_process_args,
                                                        process_name_suffix=assembly_accession.replace(".", "_")))
                workflow_file_handle.write("\n")
                process_index += 1
                # Set the flag that will capture the output status of the current process
                # This variable will be used to decide whether the next process should be started
                # See http://nextflow-io.github.io/patterns/index.html#_mock_dependency
                release_properties["previous-process-output-flag"] = "flag" + str(process_index - 1)
    return workflow_file_name, release_properties["release-log-file"]


def get_common_release_properties(common_release_properties_file):
    return yaml.load(open(common_release_properties_file), Loader=yaml.FullLoader)


def run_release_for_species(common_release_properties_file, taxonomy_id, memory):
    common_release_properties = get_common_release_properties(common_release_properties_file)
    private_config_xml_file = common_release_properties["private-config-xml-file"]
    release_species_inventory_table = common_release_properties["release-species-inventory-table"]
    release_version = common_release_properties["release-version"]
    with psycopg2.connect(get_pg_metadata_uri_for_eva_profile("development", private_config_xml_file), user="evadev") \
        as metadata_connection_handle:
        release_assemblies = get_release_assemblies_for_taxonomy(taxonomy_id, release_species_inventory_table,
                                                                 release_version, metadata_connection_handle)
        workflow_file_name, release_log_file = prepare_release_workflow_file_for_species(common_release_properties,
                                                                                         taxonomy_id,
                                                                                         release_assemblies,
                                                                                         memory)
        workflow_report_file_name = workflow_file_name.replace(".nf", ".report.html")
        if os.path.exists(workflow_report_file_name):
            os.remove(workflow_report_file_name)
        workflow_command = "cd {0} && {1} run {2} -c {3} -with-report {4} -bg".format(
            os.path.dirname(release_log_file),
            common_release_properties["nextflow-binary-path"], workflow_file_name,
            common_release_properties["nextflow-config-path"], workflow_report_file_name)
        logger.info("Check log file in: " + release_log_file + " to monitor progress...")
        logger.info("Running workflow file {0} with the following command:\n\n {1} \n\n"
                    "Use the above command with -resume if this workflow needs to be resumed in the future"
                    .format(workflow_file_name, workflow_command))
        os.system(workflow_command)


@click.option("--common-release-properties-file", help="ex: /path/to/release/properties.yml", required=True)
@click.option("--taxonomy-id", help="ex: 9913", required=True)
@click.option("--memory",  help="Memory in GB. ex: 8", default=8, type=int, required=False)
@click.command()
def main(common_release_properties_file, taxonomy_id, memory):
    run_release_for_species(common_release_properties_file, taxonomy_id, memory)


if __name__ == "__main__":
    main()

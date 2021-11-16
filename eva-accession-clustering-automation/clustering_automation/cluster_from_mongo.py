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
import argparse
import sys
import logging
import datetime
import yaml

from ebi_eva_common_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_common_pyutils.nextflow import LinearNextFlowPipeline, NextFlowProcess
from ebi_eva_common_pyutils.pg_utils import get_all_results_for_query

from clustering_automation.create_clustering_properties import create_properties_file


logger = logging.getLogger(__name__)
timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")


def get_assemblies_and_scientific_name_from_taxonomy(taxonomy_id, metadata_connection_handle, clustering_tracking_table, release_version):
    query = (f"SELECT assembly_accession, scientific_name FROM {clustering_tracking_table} "
             f"WHERE taxonomy = '{taxonomy_id}' "
             f"and release_version = {release_version} "
             f"and assembly_accession <> 'Unmapped' "
             f"and should_be_clustered = 't'")
    results = get_all_results_for_query(metadata_connection_handle, query)
    if len(results) == 0:
        raise Exception("Could not find assemblies pertaining to taxonomy ID: " + taxonomy_id)
    return [result[0] for result in results], results[0][1]


def get_common_clustering_properties(common_clustering_properties_file):
    return yaml.load(open(common_clustering_properties_file), Loader=yaml.FullLoader)


def generate_linear_pipeline(taxonomy_id, scientific_name, assembly_list, common_properties, memory, instance):
    private_config_xml_file = common_properties["private-config-xml-file"]
    profile = common_properties["profile"]
    clustering_artifact = common_properties["clustering-jar-path"]
    python = common_properties["python3-path"]
    release_version = common_properties['release-version']
    clustering_folder = common_properties['clustering-folder']
    clustering_tracking_table = common_properties['clustering-release-tracker']

    pipeline = LinearNextFlowPipeline()
    for assembly in assembly_list:
        output_directory = os.path.join(clustering_folder, f"{scientific_name.lower().replace(' ', '_')}_{taxonomy_id}", assembly)
        os.makedirs(output_directory, exist_ok=True)
        properties_path = create_properties_file('MONGO', None, None, assembly,
                                                 private_config_xml_file, profile, output_directory, instance)
        status_update_template = (f'{python} -m clustering_automation.update_clustering_status '
                                  f'--private-config-xml-file {private_config_xml_file} '
                                  f'--clustering-tracking-table {clustering_tracking_table} '
                                  f'--release {release_version} '
                                  f'--assembly {assembly} '
                                  f'--taxonomy {taxonomy_id} '
                                  '--status {status}')  # will be filled in later
        
        suffix = assembly.replace('.', '_')
        pipeline.add_process(
            process_name=f'start_{suffix}',
            command_to_run=status_update_template.format(status='Started'),
        )

        process_directives_for_java_pipelines = {'memory': f'{memory} MB',
                                                 'clusterOptions': (f'-o {output_directory}/cluster_{timestamp}.log '
                                                                    f'-e {output_directory}/cluster_{timestamp}.err')}
        # Refer to ProcessRemappedVariantsWithRSJobConfiguration.java and ClusterUnclusteredVariantsJobConfiguration.java
        # for descriptions and rationale for 2 separate jobs
        # Access to internal method _add_new_process needed for process_directives
        pipeline._add_new_process(NextFlowProcess(
            process_name=f'process_remapped_variants_with_rs_{suffix}',
            command_to_run=f'java -Xmx{memory}m -jar {clustering_artifact} --spring.config.location=file:{properties_path} '
                           f'--spring.batch.job.names=PROCESS_REMAPPED_VARIANTS_WITH_RS_JOB',
            process_directives=process_directives_for_java_pipelines
        ))
        pipeline._add_new_process(NextFlowProcess(
            process_name=f'cluster_{suffix}',
            command_to_run=f'java -Xmx{memory}m -jar {clustering_artifact} --spring.config.location=file:{properties_path} '
                           f'--spring.batch.job.names=CLUSTER_UNCLUSTERED_VARIANTS_JOB',
            process_directives={'memory': process_directives_for_java_pipelines['memory'],
                                'clusterOptions': f"{process_directives_for_java_pipelines['clusterOptions']}"
                                                  f" -g /accession/{instance} "}  # needed to serialize accessioning
        ))
        pipeline.add_process(
            process_name=f'end_{suffix}',
            command_to_run=status_update_template.format(status='Completed')  # TODO: how to choose completed/failed?
        )
        # TODO add QA process
    return pipeline, output_directory


def cluster_multiple_from_mongo(taxonomy_id, common_clustering_properties_file, memory, instance):
    """
    Generates and runs a Nextflow pipeline to cluster all assemblies for a given taxonomy.
    """
    common_properties = get_common_clustering_properties(common_clustering_properties_file)
    clustering_tracking_table = common_properties["clustering-release-tracker"]
    release_version = common_properties["release-version"]
    clustering_folder = common_properties['clustering-folder']
    with get_metadata_connection_handle("development", common_properties["private-config-xml-file"]) as metadata_connection_handle:
        assembly_list, scientific_name = get_assemblies_and_scientific_name_from_taxonomy(taxonomy_id, metadata_connection_handle, clustering_tracking_table, release_version)
        pipeline, output_directory = generate_linear_pipeline(taxonomy_id, scientific_name, assembly_list, common_properties, memory, instance)
        pipeline.run_pipeline(
            workflow_file_path=os.path.join(clustering_folder, f'{taxonomy_id}_clustering_workflow_{timestamp}.nf'),
            nextflow_binary_path=common_properties['nextflow-binary-path'],
            nextflow_config_path=common_properties['nextflow-config-path'],
            working_dir=output_directory
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cluster multiple assemblies', add_help=False)
    parser.add_argument("--taxonomy-id", help="Taxonomy id", required=True)
    parser.add_argument("--common-clustering-properties-file", help="ex: /path/to/clustering/properties.yml", required=True)
    parser.add_argument("--memory", help="Amount of memory jobs will use", required=False, default=8192)
    parser.add_argument("--instance", help="Accessioning instance id", required=False, default=6,
                        type=int, choices=range(1, 13))
    parser.add_argument('--help', action='help', help='Show this help message and exit')

    args = {}
    try:
        args = parser.parse_args()
        cluster_multiple_from_mongo(args.taxonomy_id, args.common_clustering_properties_file, args.memory, args.instance)
    except Exception as ex:
        logger.exception(ex)
        sys.exit(1)

    sys.exit(0)

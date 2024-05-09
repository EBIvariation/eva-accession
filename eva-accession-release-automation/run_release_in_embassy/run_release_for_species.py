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
from argparse import ArgumentParser

import os

import yaml
from ebi_eva_common_pyutils.command_utils import run_command_with_output
from ebi_eva_common_pyutils.logger import logging_config
from ebi_eva_internal_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_common_pyutils.config import cfg
from run_release_in_embassy.release_metadata import get_release_assemblies_for_taxonomy
from run_release_in_embassy.release_common_utils import get_release_folder_name


logger = logging_config.getLogger(__name__)


def get_nextflow_config_param(taxonomy_id, assembly_accession, release_version):
    dump_dir = os.path.join(get_species_release_folder(taxonomy_id), 'dumps')
    release_dir = get_release_log_file_name(taxonomy_id, assembly_accession)
    config_param = os.path.join(release_dir, f'nextflow_params_{taxonomy_id}_{assembly_accession}.yaml')
    os.makedirs(dump_dir, exist_ok=True)
    yaml_data = {
        'assembly': assembly_accession,
        'dump_dir': dump_dir,
        'executable': cfg['executable'],
        # 'executable.bgzip':
        # 'executable.convert_vcf_file':
        # 'executable.count_ids_in_vcf':
        # 'executable.sort_vcf_sorted_chromosomes':
        # 'executable.vcf_assembly_checker':
        # 'executable.vcf_validator':
        'jar': cfg['jar'],
        'log_file': get_release_log_file_name(taxonomy_id, assembly_accession),
        'maven': cfg['maven'],
        'python_script': cfg.query('python', 'interpreter'),
        'python_path': os.environ['PYTHONPATH'],
        'release_version': release_version,
        'assembly_folder': release_dir,
        'taxonomy': taxonomy_id
    }
    with open(config_param, 'w') as open_file:
        yaml.safe_dump(yaml_data, open_file)
    return config_param


def get_run_release_for_assembly_nextflow():
    curr_dir = os.path.dirname(__file__)
    return os.path.join(curr_dir, 'run_release_for_assembly.nf')


def get_release_log_file_name(taxonomy_id, assembly_accession):
    return f"{cfg['species-release-folder']}/{assembly_accession}/release_{taxonomy_id}_{assembly_accession}.log"


def get_species_release_folder(taxonomy_id):
    return os.path.join(cfg["release-folder"], get_release_folder_name(taxonomy_id))


def get_assembly_release_folder(taxonomy_id, assembly_accession):
    return os.path.join(get_species_release_folder(taxonomy_id), assembly_accession)


def run_release_for_species(taxonomy_id, release_assemblies, release_version):
    private_config_xml_file = cfg.query("maven", "settings_file")
    profile = cfg.query("maven", "environment")
    release_species_inventory_table = cfg["release-species-inventory-table"]
    with get_metadata_connection_handle(profile, private_config_xml_file) as metadata_connection_handle:
        if not release_assemblies:
            release_assemblies = get_release_assemblies_for_taxonomy(
                taxonomy_id, release_species_inventory_table, release_version, metadata_connection_handle
            )

        for assembly_accession in release_assemblies:
            nextflow_params = get_nextflow_config_param(taxonomy_id, assembly_accession, release_version)
            workflow_file_path = get_run_release_for_assembly_nextflow()
            release_dir = get_assembly_release_folder(taxonomy_id, assembly_accession)
            workflow_command = (
                f"cd {release_dir} && "
                f"{cfg.query('executable', 'nextflow')} run {workflow_file_path} "
                f"-params-file {nextflow_params}"
            )
            logger.info(f"Running workflow file {workflow_file_path} with the following command: "
                        f"\n {workflow_command} \n")
            run_command_with_output(f"Running workflow file {workflow_file_path}", workflow_command)


def load_config(config_file):
    cfg.load_config_file(config_file)


def main():
    argparse = ArgumentParser()
    argparse.add_argument("--common-release-properties-file", help="ex: /path/to/release/properties.yml", required=True)
    argparse.add_argument("--taxonomy-id", help="ex: 9913", required=True)
    argparse.add_argument("--assembly-accessions", nargs='+', help="ex: GCA_000003055.3")
    argparse.add_argument("--release_version", required=True)
    args = argparse.parse_args()

    logging_config.add_stdout_handler()

    load_config(args.common_release_properties_file)

    run_release_for_species(args.taxonomy_id, args.assembly_accessions, args.release_version)


if __name__ == "__main__":
    main()

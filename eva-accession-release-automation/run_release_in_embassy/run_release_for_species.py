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
import sys
from argparse import ArgumentParser

import os
from functools import lru_cache

import yaml
from ebi_eva_common_pyutils.command_utils import run_command_with_output
from ebi_eva_common_pyutils.common_utils import pretty_print
from ebi_eva_common_pyutils.logger import logging_config
from ebi_eva_internal_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_common_pyutils.config import cfg
from run_release_in_embassy.release_metadata import get_release_assemblies_for_taxonomy, get_release_for_status_and_version
from run_release_in_embassy.release_common_utils import get_release_folder_name


logger = logging_config.get_logger(__name__)


def get_nextflow_params(taxonomy_id, assembly_accession, release_version):
    dump_dir = os.path.join(get_species_release_folder(taxonomy_id), 'dumps')
    release_dir = get_assembly_release_folder(taxonomy_id, assembly_accession)
    config_param = os.path.join(release_dir, f'nextflow_params_{taxonomy_id}_{assembly_accession}.yaml')
    os.makedirs(dump_dir, exist_ok=True)
    yaml_data = {
        'assembly': assembly_accession,
        'dump_dir': dump_dir,
        'executable': cfg['executable'],
        'jar': cfg['jar'],
        'log_file': get_release_log_file_name(taxonomy_id, assembly_accession),
        'maven': cfg['maven'],
        'python_path': os.environ['PYTHONPATH'],
        'release_version': release_version,
        'assembly_folder': release_dir,
        'taxonomy': taxonomy_id
    }
    with open(config_param, 'w') as open_file:
        yaml.safe_dump(yaml_data, open_file)
    return config_param


def get_nextflow_config():
    if 'RELEASE_NEXTFLOW_CONFIG' in os.environ and os.path.isfile(os.environ['RELEASE_NEXTFLOW_CONFIG']):
        return os.path.abspath(os.environ['RELEASE_NEXTFLOW_CONFIG'])


def get_run_release_for_assembly_nextflow():
    curr_dir = os.path.dirname(__file__)
    return os.path.join(curr_dir, 'run_release_for_assembly.nf')


def get_release_log_file_name(taxonomy_id, assembly_accession):
    return f"{get_assembly_release_folder(taxonomy_id, assembly_accession)}/release_{taxonomy_id}_{assembly_accession}.log"


@lru_cache
def get_species_release_folder(taxonomy_id):
    folder = os.path.join(cfg.query('release', 'release_output'), get_release_folder_name(taxonomy_id))
    os.makedirs(folder, exist_ok=True)
    return folder


@lru_cache
def get_assembly_release_folder(taxonomy_id, assembly_accession):
    folder = os.path.join(get_species_release_folder(taxonomy_id), assembly_accession)
    os.makedirs(folder, exist_ok=True)
    return folder


def run_release_for_species(taxonomy_id, release_assemblies, release_version, resume=False):
    private_config_xml_file = cfg.query("maven", "settings_file")
    profile = cfg.query("maven", "environment")
    release_species_inventory_table = cfg.query('release', 'inventory_table')
    with get_metadata_connection_handle(profile, private_config_xml_file) as metadata_connection_handle:
        if not release_assemblies:
            release_assemblies = get_release_assemblies_for_taxonomy(
                taxonomy_id, release_species_inventory_table, release_version, metadata_connection_handle
            )

        for assembly_accession in release_assemblies:
            nextflow_params = get_nextflow_params(taxonomy_id, assembly_accession, release_version)
            workflow_file_path = get_run_release_for_assembly_nextflow()
            release_dir = get_assembly_release_folder(taxonomy_id, assembly_accession)
            nextflow_config = get_nextflow_config()
            workflow_command = ' '.join((
                f"cd {release_dir} &&",
                f"{cfg.query('executable', 'nextflow')} run {workflow_file_path}",
                f"-params-file {nextflow_params}",
                f'-c {nextflow_config}' if nextflow_config else '',
                '-resume' if resume else '',
            ))
            logger.info(f"Running workflow file {workflow_file_path} with the following command: "
                        f"\n {workflow_command} \n")
            run_command_with_output(f"Running workflow file {workflow_file_path}", workflow_command)


def list_pending_release(status, release_version, taxonomy_id, assembly_accessions):
    private_config_xml_file = cfg.query("maven", "settings_file")
    profile = cfg.query("maven", "environment")
    release_species_inventory_table = cfg.query('release', 'inventory_table')
    with get_metadata_connection_handle(profile, private_config_xml_file) as metadata_connection_handle:
        header = ['taxonomy', 'assembly_accession', 'release_version']
        table = []
        for taxonomy, assembly_accession, release_version in get_release_for_status_and_version(
                release_species_inventory_table, metadata_connection_handle, status, release_version,
                taxonomy_id, assembly_accessions
        ):
            table.append((taxonomy, assembly_accession, release_version))
        pretty_print(header, table)


def load_config(*args):
    cfg.load_config_file(
        *args,
        os.environ.get('RELEASE_CONFIG'),
        '~/.release_config.yml'
    )


def main():
    argparse = ArgumentParser()
    argparse.add_argument_group()
    argparse.add_argument("--list_status", nargs='+',
                          help="Generate the list of species and assembly that needs to be released",
                          choices=['Pending', 'Started', 'Completed'])
    argparse.add_argument("--taxonomy_id", help="ex: 9913")
    argparse.add_argument("--assembly_accessions", nargs='+', help="ex: GCA_000003055.3")
    argparse.add_argument("--release_version")
    argparse.add_argument("--resume", default=False,
                          help="Resume the nextflow pipeline for the specified taxonomy and assembly")
    args = argparse.parse_args()
    load_config()
    logging_config.add_stdout_handler()
    if args.list_status:
        list_pending_release(args.list, args.release_version, args.taxonomy_id, args.assembly_accessions)
    else:
        if not args.taxonomy_id:
            logger.error('--taxonomy_id is required when running the release')
            sys.exit(1)
        if not args.release_version:
            logger.error('--release_version is required when running the release')
            sys.exit(1)
        run_release_for_species(args.taxonomy_id, args.assembly_accessions, args.release_version, args.resume)


if __name__ == "__main__":
    main()

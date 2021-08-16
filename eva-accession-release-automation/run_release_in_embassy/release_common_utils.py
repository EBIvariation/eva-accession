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

import logging
import os
import psycopg2
import signal
import traceback

from run_release_in_embassy.release_metadata import get_target_mongo_instance_for_taxonomy
from ebi_eva_common_pyutils.config_utils import get_pg_metadata_uri_for_eva_profile
from ebi_eva_common_pyutils.network_utils import get_available_local_port, forward_remote_port_to_local_port
from ebi_eva_common_pyutils.taxonomy import taxonomy

logger = logging.getLogger(__name__)


def open_mongo_port_to_tempmongo(private_config_xml_file, profile, taxonomy_id, release_species_inventory_table,
                                 release_version):
    MONGO_PORT = 27017
    local_forwarded_port = get_available_local_port(MONGO_PORT)
    try:
        with psycopg2.connect(get_pg_metadata_uri_for_eva_profile(profile, private_config_xml_file),
                              user="evadev") as \
                metadata_connection_handle:
            tempmongo_instance = get_target_mongo_instance_for_taxonomy(taxonomy_id, release_species_inventory_table,
                                                                        release_version, metadata_connection_handle)
            logger.info("Forwarding remote MongoDB port 27017 to local port {0}...".format(local_forwarded_port))
            port_forwarding_process_id = forward_remote_port_to_local_port(tempmongo_instance, MONGO_PORT,
                                                                           local_forwarded_port)
            return port_forwarding_process_id, local_forwarded_port
    except Exception:
        raise Exception("Encountered an error while opening a port to the remote MongoDB instance: "
                        + tempmongo_instance + "\n" + traceback.format_exc())


def close_mongo_port_to_tempmongo(port_forwarding_process_id):
    os.kill(port_forwarding_process_id, signal.SIGTERM)
    os.system('echo -e "Killed port forwarding from remote port with signal 1 - SIGTERM. '
              '\\033[31;1;4mIGNORE OS MESSAGE '  # escape sequences for bold red and underlined text
              '\'Killed by Signal 1\' in the preceding/following text\\033[0m".')


def get_bgzip_tabix_commands_for_file(bgzip_path, tabix_path, file):
    commands = ["rm -f {0}.gz".format(file), "({0} < {1} > {1}.gz)".format(bgzip_path, file),
                "({0} -f {1}.gz)".format(tabix_path, file)]
    return commands


def get_bgzip_bcftools_index_commands_for_file(bgzip_path, bcftools_path, file):
    commands = ["rm -f {0}.gz".format(file), "({0} -c {1} > {1}.gz)".format(bgzip_path, file),
                "({0} -cf {1}.gz)".format(bcftools_path, file)]
    return commands


def get_release_vcf_file_name(species_release_folder, assembly_accession, vcf_file_category):
    return os.path.join(species_release_folder, assembly_accession, "{0}_{1}.vcf".format(assembly_accession,
                                                                                         vcf_file_category))


def get_unsorted_release_vcf_file_name(species_release_folder, assembly_accession, vcf_file_category):
    vcf_file_path = get_release_vcf_file_name(species_release_folder, assembly_accession, vcf_file_category)
    filename = os.path.basename(vcf_file_path)
    return vcf_file_path.replace(filename, filename.replace(".vcf", "_unsorted.vcf"))


def get_release_text_file_name(species_release_folder, assembly_accession, release_text_file_category):
    return os.path.join(species_release_folder, assembly_accession, "{0}_{1}.txt".format(assembly_accession,
                                                                                         release_text_file_category))


def get_unsorted_release_text_file_name(species_release_folder, assembly_accession, release_text_file_category):
    release_text_file_path = get_release_text_file_name(species_release_folder, assembly_accession,
                                                        release_text_file_category)
    filename = os.path.basename(release_text_file_path)
    return release_text_file_path.replace(filename, filename.replace(".txt", ".unsorted.txt"))


def get_release_db_name_in_tempmongo_instance(taxonomy_id):
    return "acc_" + str(taxonomy_id)


def get_release_folder_name(taxonomy_id):
    return taxonomy.get_normalized_scientific_name_from_ensembl(taxonomy_id)

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

# The purpose of this script is to validate the mapping weight attribute addition that was performed by
# the script incorporate_mapping_weight_into_accessioning.py

import click
import logging
import psycopg2

from collections import defaultdict
from ebi_eva_common_pyutils.command_utils import run_command_with_output
from ebi_eva_common_pyutils.config_utils import get_pg_metadata_uri_for_eva_profile
from ebi_eva_common_pyutils.metadata_utils import get_species_info, get_db_conn_for_species
from ebi_eva_common_pyutils.pg_utils import get_all_results_for_query, get_result_cursor


logger = logging.getLogger(__name__)


def get_assemblies_with_multimap_snps_for_species(metadata_connection_handle):
    assembly_GCA_accession_map = defaultdict(dict)
    query = "select distinct database_name, assembly, assembly_accession " \
            "from dbsnp_ensembl_species.EVA2015_snpmapinfo_asm_lookup " \
            "where assembly_accession is not null"
    for result in get_all_results_for_query(metadata_connection_handle, query):
        species_name, assembly, GCA_accession = result
        assembly_GCA_accession_map[species_name][assembly] = GCA_accession
    return assembly_GCA_accession_map


def export_all_multimap_snps_from_dbsnp_dumps(private_config_xml_file):
    result_file = "all_multimap_snp_ids_from_dbsnp_dumps.txt"
    with psycopg2.connect(get_pg_metadata_uri_for_eva_profile("production_processing", private_config_xml_file), user="evapro") \
        as metadata_connection_handle:
        assembly_GCA_accession_map = get_assemblies_with_multimap_snps_for_species(metadata_connection_handle)
        for species_info in get_species_info(metadata_connection_handle):
            species_name = species_info["database_name"]
            logger.info("Processing species {0}...".format(species_name))
            if species_name in assembly_GCA_accession_map:
                with get_db_conn_for_species(species_info) as species_connection_handle:
                    export_query = "select snp_id, assembly from dbsnp_{0}.multimap_snps " \
                                   "where assembly in ({1})"\
                        .format(species_name,",".join(["'{0}'".format(assembly) for assembly in
                                                       assembly_GCA_accession_map[species_name].keys()]))
                    logger.info("Running export query: " + export_query)
                    with open(result_file, 'a') as result_file_handle:
                        for snp_id, assembly in get_result_cursor(species_connection_handle, export_query):
                            result_file_handle.write("{0},{1}\n"
                                                     .format(snp_id,
                                                             assembly_GCA_accession_map[species_name][assembly]))

    run_command_with_output("Sorting multimap SNP IDs from dbSNP source dumps...",
                            "sort -u {0} -o {0}".format(result_file))


@click.option("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
@click.command()
def main(private_config_xml_file):
    export_all_multimap_snps_from_dbsnp_dumps(private_config_xml_file)


if __name__ == "__main__":
    main()

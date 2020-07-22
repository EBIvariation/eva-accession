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

from ebi_eva_common_pyutils.pg_utils import get_all_results_for_query


def get_assemblies_to_import_for_dbsnp_species(metadata_connection_handle, dbsnp_species_taxonomy, release_version):
    query = "select distinct assembly_accession from dbsnp_ensembl_species.release_assemblies " \
            "where release_version='{0}' " \
            "and data_source='dbSNP' and tax_id='{1}'".format(release_version, dbsnp_species_taxonomy)
    results = get_all_results_for_query(metadata_connection_handle, query)
    if len(results) > 0:
        return [result[0] for result in results]
    return []
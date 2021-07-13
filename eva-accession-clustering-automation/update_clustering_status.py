# Copyright 2021 EMBL - European Bioinformatics Institute
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
import argparse
import psycopg2
import datetime
import logging

from ebi_eva_common_pyutils.config_utils import get_pg_metadata_uri_for_eva_profile
from ebi_eva_common_pyutils.pg_utils import execute_query


logger = logging.getLogger(__name__)
timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")


def set_progress_end(profile, private_config_xml_file, assembly, tax_id, release_version):
    update_status_query = ('UPDATE eva_progress_tracker.clustering_release_tracker '
                           f"SET clustering_status='done', clustering_end = '{datetime.datetime.now().isoformat()}' "
                           f"WHERE assembly='{assembly}' AND taxonomy_id='{tax_id}' "
                           f"AND release_version={release_version}")
    with psycopg2.connect(get_pg_metadata_uri_for_eva_profile(profile, private_config_xml_file), user="evadev") \
            as metadata_connection_handle:
        execute_query(metadata_connection_handle, update_status_query)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Update clustering progress', add_help=False)
    parser.add_argument("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
    parser.add_argument("--profile", help="Profile to get the properties, e.g.production", required=True)
    parser.add_argument("--release", help="Release version", required=True)
    parser.add_argument("--taxonomy", help="Taxonomy id", required=True)
    args = {}
    try:
        args = parser.parse_args()
        set_progress_end()
    except Exception as ex:
        logger.exception(ex)
        sys.exit(1)

    sys.exit(0)

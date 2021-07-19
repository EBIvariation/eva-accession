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
import datetime
import logging

from ebi_eva_common_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_common_pyutils.pg_utils import execute_query


logger = logging.getLogger(__name__)
timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")


def set_progress_status(profile, private_config_xml_file, assembly, tax_id, release_version, status):
    now = datetime.datetime.now().isoformat()
    update_status_query = 'UPDATE eva_progress_tracker.clustering_release_tracker '
    update_status_query += f"SET clustering_status={status}"
    if status == 'Started':
        update_status_query += f", clustering_start='{now}'"
    elif status == 'Completed':
        update_status_query += f", clustering_end='{now}'"
    update_status_query += (f" WHERE assembly='{assembly}' AND taxonomy_id='{tax_id}' "
                            f"AND release_version={release_version}")
    with get_metadata_connection_handle(profile, private_config_xml_file) as metadata_connection_handle:
        execute_query(metadata_connection_handle, update_status_query)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Update clustering progress', add_help=False)
    parser.add_argument("--private-config-xml-file", help="ex: /path/to/eva-maven-settings.xml", required=True)
    parser.add_argument("--profile", help="Profile to get the properties, e.g.production", required=True)
    parser.add_argument("--release", help="Release version", required=True)
    parser.add_argument("--assembly", help="Assembly accession", required=True)
    parser.add_argument("--taxonomy", help="Taxonomy id", required=True)
    parser.add_argument("--status", help="Status to set", required=True, choices=["Started", "Completed", "Failed"])
    args = {}
    try:
        args = parser.parse_args()
        set_progress_status(args.profile, args.private_config_xml_file, args.assembly, args.taxonomy, args.release, args.status)
    except Exception as ex:
        logger.exception(ex)
        sys.exit(1)

    sys.exit(0)

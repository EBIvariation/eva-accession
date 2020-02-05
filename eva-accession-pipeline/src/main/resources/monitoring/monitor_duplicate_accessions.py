#!/usr/bin/env python3

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
from datetime import datetime
from __init__ import *


def export_mongo_accessions(mongo_connection_properties, collection_name, export_output_filename):
    export_command = 'mongoexport --host {0} --port {1} --db {2} --username {3} --password {4} ' \
                     '--authenticationDatabase={5} --collection {6} --type=csv --fields _id -o "{7}" '\
                    .format(mongo_connection_properties["mongo_host"],
                            mongo_connection_properties["mongo_port"],
                            mongo_connection_properties["mongo_db"],
                            mongo_connection_properties["mongo_username"],
                            mongo_connection_properties["mongo_password"],
                            mongo_connection_properties["mongo_auth_db"],
                            collection_name,
                            export_output_filename) + " --sort '{_id: 1}'"
    run_command_with_output("Exporting accessions in the {0} collection in the {1} database at {2}..."
                            .format(collection_name, mongo_connection_properties["mongo_db"],
                                    mongo_connection_properties["mongo_host"]),
                            export_command)


def check_duplicate_accessions_in_mongo(pipeline_properties_file, ids_export_output_dir, collection_name):
    mongo_connection_properties = get_mongo_connection_details_from_properties_file(pipeline_properties_file)
    logger.info("Checking duplicate accessions in the {0} collection in the {1} database at {2}..."
                .format(collection_name, mongo_connection_properties["mongo_db"],
                        mongo_connection_properties["mongo_host"]))

    export_output_filename = os.path.sep.join([ids_export_output_dir,
                                              "ids_in_{0}_{1}_at_{2}_as_of_{3}.csv"
                                              .format(mongo_connection_properties["mongo_db"], collection_name,
                                                      mongo_connection_properties["mongo_host"],
                                                      datetime.today().strftime('%Y%m%d%H%M%S'))])
    export_mongo_accessions(mongo_connection_properties, collection_name, export_output_filename)
    output = run_command_with_output("Checking for duplicate accessions in the exported file...",
                                     'uniq -d "{0}" | wc -l'.format(export_output_filename))

    if int(output) > 0:
        logger.error("Duplicate accessions found in the {0} collection in the {1} database at {2}..."
                     .format(collection_name, mongo_connection_properties["mongo_db"],
                             mongo_connection_properties["mongo_host"]))
        return 1
    else:
        logger.info("NO duplicate accessions were found in the {0} collection in the {1} database at {2}..."
                    .format(collection_name, mongo_connection_properties["mongo_db"],
                            mongo_connection_properties["mongo_host"]))
        run_command_with_output("Compressing accessions file {0}...".format(export_output_filename),
                                'gzip "{0}"'.format(export_output_filename))
        return 0


@click.option("-p", "--pipeline-properties-file", required=True)
@click.option("-o", "--ids-export-output-dir", required=True)
@click.argument("collection-names", nargs=-1, required=True)
@click.command()
def main(pipeline_properties_file, ids_export_output_dir, collection_names):
    exit_code = 1
    for collection_name in collection_names:
        exit_code = exit_code and \
                    check_duplicate_accessions_in_mongo(pipeline_properties_file, ids_export_output_dir,
                                                        collection_name)
    sys.exit(exit_code)


if __name__ == '__main__':
    main()

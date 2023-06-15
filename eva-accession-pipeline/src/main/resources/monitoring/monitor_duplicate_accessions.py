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
import getpass
import smtplib
from __init__ import *
from urllib.parse import quote_plus


def get_mongo_uri(mongo_connection_properties):
    return "mongodb://{0}:{1}@{2}/{3}?authSource={4}".format(mongo_connection_properties["mongo_username"],
                                                             quote_plus(mongo_connection_properties["mongo_password"]),
                                                             mongo_connection_properties["mongo_host"],
                                                             mongo_connection_properties["mongo_db"],
                                                             mongo_connection_properties["mongo_auth_db"])


def export_mongo_accessions(mongo_connection_properties, collection_name, export_output_filename):
    export_command = 'mongoexport --uri {0} ' \
                     '--collection {1} --type=csv --fields accession ' \
                     "--query  '{{remappedFrom: {{$exists: false}}}}' " \
                     '-o "{2}" 2>&1' \
                    .format(get_mongo_uri(mongo_connection_properties), collection_name, export_output_filename)
    run_command_with_output("Exporting accessions in the {0} collection in the {1} database at {2}..."
                            .format(collection_name, mongo_connection_properties["mongo_db"],
                                    mongo_connection_properties["mongo_host"]), export_command)


def notify_by_email(mongo_connection_properties, collection_name, duplicates_output_filename,
                    number_of_duplicate_accessions, email_recipients):
    error_message = "{0} DUPLICATE ACCESSIONS !!! in the {1} collection in the {2} database at {3}"\
                    .format(number_of_duplicate_accessions, collection_name, mongo_connection_properties["mongo_db"],
                            mongo_connection_properties["mongo_host"])
    logger.error(error_message)
    email_message = "Subject: {0}\n\n" \
                    "Please see {1} for the list of duplicates.".format(error_message, duplicates_output_filename)
    smtplib.SMTP('localhost').sendmail(getpass.getuser(), email_recipients, email_message)


def report_duplicates_in_exported_accessions_file(mongo_connection_properties, collection_name, export_output_filename,
                                                  duplicates_output_filename, email_recipients):
    sorted_export_output_filename = export_output_filename.replace(".csv", "_sorted.csv")
    run_command_with_output("Sorting {0}...".format(duplicates_output_filename),
                            'sort -S 4G -T {0} -o "{1}" "{2}"'
                            .format(os.path.dirname(export_output_filename), sorted_export_output_filename,
                                    export_output_filename))
    run_command_with_output("Exporting duplicates to {0}...".format(duplicates_output_filename),
                            'uniq -d "{0}" > {1}'.format(sorted_export_output_filename, duplicates_output_filename))
    number_of_duplicate_accessions = run_command_with_output("Find duplicate accessions in the exported file...",
                                                             'wc -l < "{0}"'.format(duplicates_output_filename),
                                                             return_process_output=True)
    if int(number_of_duplicate_accessions) > 0:
        notify_by_email(mongo_connection_properties, collection_name, duplicates_output_filename,
                        number_of_duplicate_accessions, email_recipients)
        return 1
    else:
        logger.info("NO duplicate accessions were found in the {0} collection in the {1} database at {2}..."
                    .format(collection_name, mongo_connection_properties["mongo_db"],
                            mongo_connection_properties["mongo_host"]))
        return 0


def report_duplicate_accessions_in_mongo(pipeline_properties_file, accessions_export_output_dir,
                                         collection_name, email_recipients):
    mongo_connection_properties = get_mongo_connection_details_from_properties_file(pipeline_properties_file)
    export_output_filename = os.path.sep.join([accessions_export_output_dir,
                                               "accessions_in_{0}_{1}_at_{2}_as_of_{3}.csv"
                                              .format(mongo_connection_properties["mongo_db"], collection_name,
                                                      mongo_connection_properties["mongo_host"],
                                                      datetime.today().strftime('%Y%m%d%H%M%S'))])
    duplicates_output_filename = export_output_filename.replace("accessions_in", "duplicate_accessions_in")

    logger.info("Checking duplicate accessions in the {0} collection in the {1} database at {2}..."
                .format(collection_name, mongo_connection_properties["mongo_db"],
                        mongo_connection_properties["mongo_host"]))

    export_mongo_accessions(mongo_connection_properties, collection_name, export_output_filename)

    return report_duplicates_in_exported_accessions_file(mongo_connection_properties, collection_name,
                                                         export_output_filename, duplicates_output_filename,
                                                         email_recipients)


@click.option("-p", "--pipeline-properties-file", required=True)
@click.option("-o", "--accessions-export-output-dir", required=True)
@click.option("-e", "--email-recipients", multiple=True, required=True)
@click.argument("collection-names", nargs=-1, required=True)
@click.command()
def main(pipeline_properties_file, accessions_export_output_dir,  email_recipients, collection_names):
    exit_code = 0
    for collection_name in collection_names:
        exit_code = exit_code or \
                    report_duplicate_accessions_in_mongo(pipeline_properties_file, accessions_export_output_dir,
                                                         collection_name, email_recipients)
    sys.exit(exit_code)


if __name__ == '__main__':
    main()

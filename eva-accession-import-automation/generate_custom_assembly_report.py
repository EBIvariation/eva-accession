# Copyright 2019 EMBL - European Bioinformatics Institute
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

from get_assembly_report_url import *
import data_ops
import os
import pandas
import sys
import urllib.request
from __init__ import *


def get_header_line_index(assembly_report_path):
    try:
        with open(assembly_report_path) as assembly_report_file_handle:
            return next(line_index for line_index, line in enumerate(assembly_report_file_handle)
                        if line.lower().startswith("# sequence-name") and "sequence-role" in line.lower())
    except StopIteration:
        raise Exception("Could not locate header row in the assembly report!")


def get_dataframe_for_assembly_report(assembly_report_path):
    return pandas.read_csv(assembly_report_path, skiprows=get_header_line_index(assembly_report_path),
                           dtype=str, sep='\t')


def get_dataframe_for_genbank_equivalents(genbank_equivalents_file_path):
    result_dataframe = pandas.read_csv(genbank_equivalents_file_path, dtype=str, sep='\t')
    result_dataframe = result_dataframe.set_index('rs_acc', drop=False)
    result_dataframe = result_dataframe[~result_dataframe.index.duplicated()]
    result_dataframe = result_dataframe.sort_index()
    return result_dataframe


def get_refseq_accessions_without_equivalent_genbank_accessions(assembly_report_dataframe):
    return set(assembly_report_dataframe[((assembly_report_dataframe["Relationship"] == "<>") |
                                         (assembly_report_dataframe["GenBank-Accn"] == "na")) &
                                         (assembly_report_dataframe["RefSeq-Accn"] != "na")]["RefSeq-Accn"]
               .values.tolist())


def get_genbank_accessions_without_equivalent_refseq_accessions(assembly_report_dataframe):
    return set(assembly_report_dataframe[((assembly_report_dataframe["Relationship"] == "<>") |
                                         (assembly_report_dataframe["RefSeq-Accn"] == "na")) &
                                         (assembly_report_dataframe["GenBank-Accn"] != "na")]["GenBank-Accn"]
               .values.tolist())


def get_refseq_accessions_from_db(species_info):
    pg_conn = data_ops.get_pg_conn_for_species(species_info)
    pg_cursor = pg_conn.cursor()
    contiginfo_tables = data_ops.get_contiginfo_table_list_for_schema(pg_cursor, species_info["database_name"])
    # Can't filter by assembly accession because only some contiginfo tables have that field :(
    contiginfo_query = "select distinct * from ({0}) temp".format(
        " union all ".join(["select contig_name from dbsnp_{0}.{1}"
                           .format(species_info["database_name"], contiginfo_table_name)
                            for contiginfo_table_name in contiginfo_tables]))
    pg_cursor.execute(contiginfo_query)
    accessions = [x[0] for x in pg_cursor.fetchall()]
    pg_cursor.close()
    pg_conn.close()
    return set(accessions)


def get_absent_refseq_accessions(refseq_accessions_from_db, refseq_accessions_from_assembly_report):
    return refseq_accessions_from_db - refseq_accessions_from_assembly_report


def download_assembly_report(assembly_accession):
    assembly_report_url = get_assembly_report_url(assembly_accession)
    assembly_report_path = os.getcwd() + os.path.sep + os.path.basename(assembly_report_url)
    urllib.request.urlretrieve(assembly_report_url, assembly_report_path)
    urllib.request.urlcleanup()
    return assembly_report_path


def insert_entry_into_assembly_report_dataframe(assembly_report_dataframe, entry_index, refseq_accession,
                                                equivalent_genbank_accession, equivalence="="):
    assembly_report_dataframe.loc[entry_index, "# Sequence-Name"] = refseq_accession
    assembly_report_dataframe.loc[entry_index, "Sequence-Role"] = "scaffold"
    assembly_report_dataframe.loc[entry_index, "Assigned-Molecule"] = "na"
    assembly_report_dataframe.loc[entry_index, "Assigned-Molecule-Location/Type"] = "na"
    assembly_report_dataframe.loc[entry_index, "GenBank-Accn"] = \
        equivalent_genbank_accession
    assembly_report_dataframe.loc[entry_index, "Relationship"] = equivalence
    assembly_report_dataframe.loc[entry_index, "RefSeq-Accn"] = refseq_accession
    assembly_report_dataframe.loc[entry_index, "Assembly-Unit"] = "na"
    assembly_report_dataframe.loc[entry_index, "Sequence-Length"] = "na"
    assembly_report_dataframe.loc[entry_index, "UCSC-style-name"] = "na"


def insert_absent_genbank_accessions_in_assembly_report(species, refseq_accessions_from_db,
                                                        assembly_report_dataframe, genbank_equivalents_dataframe):
    absent_refseq_accessions = \
        get_absent_refseq_accessions(refseq_accessions_from_db, set(assembly_report_dataframe["RefSeq-Accn"]))
    if len(absent_refseq_accessions) == 0:
        logger.info("No GenBank accessions are absent in the assembly report for " + species)

    new_assembly_report_entry_index = len(assembly_report_dataframe)
    for refseq_accession in absent_refseq_accessions:
        try:
            equivalent_genbank_accession = \
                genbank_equivalents_dataframe.loc[refseq_accession]["gb_acc"]
            insert_entry_into_assembly_report_dataframe(assembly_report_dataframe, new_assembly_report_entry_index,
                                                        refseq_accession, equivalent_genbank_accession)
        except KeyError:
            logger.warning("Could not find equivalent GenBank accession for RefSeq accession: " + refseq_accession)
            insert_entry_into_assembly_report_dataframe(assembly_report_dataframe, new_assembly_report_entry_index,
                                                        refseq_accession, "na", "<>")
        finally:
            new_assembly_report_entry_index += 1

    return assembly_report_dataframe


def update_non_equivalent_genbank_accessions_in_assembly_report(assembly_report_dataframe,
                                                                genbank_equivalents_dataframe):
    refseq_accessions_with_non_equivalent_genbank_accessions = \
        get_refseq_accessions_without_equivalent_genbank_accessions(assembly_report_dataframe)
    if len(refseq_accessions_with_non_equivalent_genbank_accessions) == 0:
        logger.info("No entries were found in the assembly report with non-equivalent GenBank accessions")

    for accession in refseq_accessions_with_non_equivalent_genbank_accessions:
        try:
            equivalent_genbank_accession = \
                genbank_equivalents_dataframe.loc[accession]["gb_acc"]
            assembly_report_dataframe.loc[assembly_report_dataframe["RefSeq-Accn"] == accession, "GenBank-Accn"] = \
                equivalent_genbank_accession
            assembly_report_dataframe.loc[assembly_report_dataframe["RefSeq-Accn"] == accession, "Relationship"] = "="
        except KeyError:
            logger.warning("Could not find equivalent GenBank accession for RefSeq accession " + accession)

    return assembly_report_dataframe


def update_non_equivalent_refseq_accessions_in_assembly_report(assembly_report_dataframe,
                                                               genbank_equivalents_dataframe):
    genbank_accessions_with_non_equivalent_refseq_accessions = \
        get_genbank_accessions_without_equivalent_refseq_accessions(assembly_report_dataframe)
    if len(genbank_accessions_with_non_equivalent_refseq_accessions) == 0:
        logger.info("No entries were found in the assembly report with non-equivalent RefSeq accessions")

    for accession in genbank_accessions_with_non_equivalent_refseq_accessions:
        equivalent_refseq_accession = \
            genbank_equivalents_dataframe[genbank_equivalents_dataframe["gb_acc"] == accession]["rs_acc"]
        if len(equivalent_refseq_accession) == 0:
            logger.warning("Could not find equivalent RefSeq accession for GenBank accession " + accession)
        else:
            equivalent_refseq_accession = equivalent_refseq_accession.values[0]
            assembly_report_dataframe.loc[assembly_report_dataframe["GenBank-Accn"] == accession, "RefSeq-Accn"] = \
                equivalent_refseq_accession
            assembly_report_dataframe.loc[assembly_report_dataframe["GenBank-Accn"] == accession, "Relationship"] = "="

    return assembly_report_dataframe


def update_assembly_report(assembly_report_dataframe, genbank_equivalents_dataframe, species_db_info):
    assembly_report_dataframe = update_non_equivalent_genbank_accessions_in_assembly_report(assembly_report_dataframe,
                                                                                            genbank_equivalents_dataframe)
    assembly_report_dataframe = update_non_equivalent_refseq_accessions_in_assembly_report(assembly_report_dataframe,
                                                                                           genbank_equivalents_dataframe)
    refseq_accessions_from_db = get_refseq_accessions_from_db(species_db_info)
    assembly_report_dataframe = insert_absent_genbank_accessions_in_assembly_report(args.species,
                                                                                    refseq_accessions_from_db,
                                                                                    assembly_report_dataframe,
                                                                                    genbank_equivalents_dataframe)
    return assembly_report_dataframe


def main(metadb, metauser, metahost, species, assembly_accession, genbank_equivalents_file):
        species_db_info = next(filter(lambda db_info: db_info["database_name"] == species,
                                      data_ops.get_species_pg_conn_info(metadb, metauser, metahost)))
        # Download assembly report
        assembly_report_path = download_assembly_report(assembly_accession)

        # Get dataframes by reading in the assembly report and the file with GenBank equivalents
        assembly_report_dataframe = get_dataframe_for_assembly_report(assembly_report_path)
        genbank_equivalents_dataframe = get_dataframe_for_genbank_equivalents(genbank_equivalents_file)

        # Modify assembly report dataframe by updating N/A or non-equivalent GenBank accessions
        # with the correct equivalents and inserting missing GenBank accessions
        assembly_report_dataframe = update_assembly_report(assembly_report_dataframe, genbank_equivalents_dataframe,
                                                           species_db_info)

        # Write out the modified assembly report
        modified_assembly_report_filename = os.path.dirname(assembly_report_path) + os.path.sep + species + \
                                            "_custom_assembly_report.txt"
        assembly_report_dataframe.to_csv(modified_assembly_report_filename, sep='\t', index=None)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate custom assembly report for a given species and assembly',
                                     add_help=False)
    parser.add_argument("-d", "--metadb", help="Postgres metadata DB", required=True)
    parser.add_argument("-u", "--metauser", help="Postgres metadata DB username", required=True)
    parser.add_argument("-h", "--metahost", help="Postgres metadata DB host", required=True)
    parser.add_argument("-s", "--species",
                        help="Species for which the process has to be run, e.g. chicken_9031",
                        required=True)
    parser.add_argument("-a", "--assembly-accession", help="Assembly for which the process has to be run",
                        required=True)
    parser.add_argument("-g", "--genbank-equivalents-file", help="File with GenBank equivalents for RefSeq accessions",
                        required=True)
    parser.add_argument('--help', action='help', help='Show this help message and exit')

    args = {}
    try:
        args = parser.parse_args()
        main(args.metadb, args.metauser, args.metahost, args.species, args.assembly_accession,
             args.genbank_equivalents_file)
    except Exception as ex:
        logger.exception(ex)
        sys.exit(1)

    sys.exit(0)

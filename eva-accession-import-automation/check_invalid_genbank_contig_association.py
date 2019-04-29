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

# See EVA-1523 for details

from generate_custom_assembly_report import *
from get_assembly_report_url import *
import data_ops
import argparse
from __init__ import *
import os
import subprocess
import sys
import hashlib


assembly_report_dataframe = None


def get_assembly_report_dataframe(assembly_accession):
    assembly_report_path = download_assembly_report(assembly_accession)
    return get_dataframe_for_assembly_report(assembly_report_path)


def persist_all_contigs(species, assembly_accession, contigs, contig_output_folder):
    contig_file_for_species = contig_output_folder + os.path.sep + species + "_" + assembly_accession + "_contigs.txt"
    with open(contig_file_for_species, "w") as contig_file_handle:
        for contig in contigs:
            contig_file_handle.write(contig + os.linesep)
    return contig_file_for_species


def persist_contigs_without_genbank_equivalents(contig_file_species, genbank_equivalents_file):
    contigs_without_genbank_equivalents_command = "grep -w -oFf {0} {1} | " \
                                                  "grep -w -vFf - {0}".format(contig_file_species,
                                                                              genbank_equivalents_file)
    contigs_without_genbank_equivalents_file = contig_file_species.replace("_contigs.txt",
                                                                           "_contigs_without_gb_equiv.txt")
    with subprocess.Popen(contigs_without_genbank_equivalents_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          bufsize=1, universal_newlines=True, shell=True) as process:
        with open(contigs_without_genbank_equivalents_file, "w") as contig_file_handle:
            for line in process.stdout:
                if line.strip():
                    contig_file_handle.write(line.strip() + os.linesep)

        errors = os.linesep.join(process.stderr.readlines())
    if process.returncode != 0 and process.returncode != 1:  # Grep will return 1 exit code for no matches!
        logger.error(contigs_without_genbank_equivalents_command + " failed!" + os.linesep + errors)
        raise subprocess.CalledProcessError(process.returncode, process.args)

    return contigs_without_genbank_equivalents_file


def build_contig_name_index_for_species_mvs(species_info, species_mvs):
    pg_conn = data_ops.get_pg_conn_for_species(species_info)
    pg_cursor = pg_conn.cursor()
    i = 0
    for species_mv in species_mvs:
        i += 1
        create_index_query = "create index if not exists {0}_ctg_name_idx_{1} on dbsnp_{0}.{2} (contig_name)"\
            .format(species_info["database_name"], i, species_mv)
        pg_cursor.execute(create_index_query)
    pg_conn.commit()
    pg_cursor.close()
    pg_conn.close()


def get_ssids_for_contigs_without_genbank_equivalents(species_info, assembly_name,
                                                      contigs_without_genbank_equivalents):
    pg_conn = data_ops.get_pg_conn_for_species(species_info)
    pg_cursor = pg_conn.cursor()

    species_mvs = [result for result in data_ops.get_mv_list_for_schema(pg_cursor, species_info["database_name"])
                   if hashlib.md5(assembly_name.encode("utf-8")).hexdigest() in result]
    build_contig_name_index_for_species_mvs(species_info, species_mvs)

    ssid_contigs_query = "select distinct * from ({0}) temp".format(
        " union all ".join(["select ss_id, string_agg(distinct chromosome, ',') from dbsnp_{0}.{1} "
                            "where contig_name in ({2}) group by 1"
                           .format(species_info["database_name"], species_mv,
                                   ",".join(["'{0}'".format(contig) for contig in contigs_without_genbank_equivalents]))
                            for species_mv in species_mvs]))
    pg_cursor.execute(ssid_contigs_query)
    accessions = [(x[0], x[1].split(",")) for x in pg_cursor.fetchall()]
    pg_cursor.close()
    pg_conn.close()
    return accessions


def get_genbank_contig_for_chr_from_asm_report(assembly_accession, chromosome):
    global assembly_report_dataframe
    if assembly_report_dataframe is None:
        assembly_report_dataframe = get_assembly_report_dataframe(assembly_accession)
    matching_entry_from_assembly_report = assembly_report_dataframe[assembly_report_dataframe["Assigned-Molecule"]
                                                                    == chromosome]
    if not matching_entry_from_assembly_report.empty:
        return matching_entry_from_assembly_report["GenBank-Accn"].values[0]
    else:
        return ""


def persist_to_file(records, file_name):
    with open(file_name, "w") as file_handle:
        for record in records:
            file_handle.write("\t".join([str(elem) for elem in record]) + os.linesep)


def get_eva1523_impacted_ss_id_chr_from_dbsnp(args):
    all_contigs = get_refseq_accessions_from_db(args.species_info)
    all_contigs_file = persist_all_contigs(args.species, args.assembly_accession, all_contigs,
                                           args.contig_output_folder)
    # This comes after writing because we want to explicitly write no results for a given species
    # so as to be searchable by a find command with a 0k criteria in the future
    if len(all_contigs) == 0:
        logger.info("No Contigs without Genbank equivalents for the species: " + args.species)
        sys.exit(0)
    contigs_without_genbank_equivalents_file = \
        persist_contigs_without_genbank_equivalents(all_contigs_file, args.genbank_equivalents_file)
    contigs_without_genbank_equivalents = list(map(str.strip,
                                                   open(contigs_without_genbank_equivalents_file).readlines()))
    if len(contigs_without_genbank_equivalents) > 0:
        ssid_chr_for_contigs_without_genbank_equivalents = \
            get_ssids_for_contigs_without_genbank_equivalents(args.species_info, args.assembly_name,
                                                              contigs_without_genbank_equivalents)
        return ssid_chr_for_contigs_without_genbank_equivalents
    else:
        return []


def main(args):
    args.species_info = next(filter(lambda db_info: db_info["database_name"] == args.species,
                                    data_ops.get_species_pg_conn_info(args.metadb, args.metauser, args.metahost)))

    # Get SS IDs (along with their chromosomes) in dbSNP which have RefSeq contigs without Genbank equivalents
    logger.info("Getting impacted SS IDs and chromosomes from dbSNP "
                "for the species {0} and assembly {1}...".format(args.species, args.assembly_accession))
    impacted_ssid_chr_from_dbsnp = get_eva1523_impacted_ss_id_chr_from_dbsnp(args)
    persist_to_file(impacted_ssid_chr_from_dbsnp, args.contig_output_folder + os.path.sep + args.species + "_" +
                    "_" + args.assembly_accession + "_impacted_ssid_chr_from_dbsnp.txt")
    if len(impacted_ssid_chr_from_dbsnp) == 0:
        logger.info("No impacted SS IDs for the species {0} and assembly {1}".format(args.species,
                                                                                     args.assembly_accession))
        sys.exit(0)

    # Associate the impacted SS IDs with the correct contig from the ASM report
    logger.info("Associating impacted SS IDs and chromosomes with corresponding Genbank contigs "
                "for the species {0} and assembly {1}...".format(args.species, args.assembly_accession))
    impacted_ssid_chr_from_dbsnp_with_contig = [(ss_id, chromosomes,
                                                 [get_genbank_contig_for_chr_from_asm_report(args.assembly_accession,
                                                                                             chromosome)
                                                  for chromosome in chromosomes])
                                                for ss_id, chromosomes in impacted_ssid_chr_from_dbsnp]
    persist_to_file(impacted_ssid_chr_from_dbsnp_with_contig, args.contig_output_folder + os.path.sep + args.species +
                    "_" + args.assembly_accession + "_impacted_ssid_chr_from_dbsnp_with_contig.txt")


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
    parser.add_argument("-n", "--assembly-name", help="Assembly name for which the process has to be run",
                        required=True)
    parser.add_argument("-g", "--genbank-equivalents-file", help="File with GenBank equivalents for RefSeq accessions",
                        required=True)
    parser.add_argument("-c", "--contig-output-folder", help="Folder with all the contigs for the species",
                        required=True)
    parser.add_argument("--mongo-uri", help="MongoDB URI", required=True)
    parser.add_argument("--mongo-db", help="MongoDB database", required=True)
    parser.add_argument('--help', action='help', help='Show this help message and exit')

    try:
        main(parser.parse_args())
    except Exception as ex:
        logger.exception(ex)
        sys.exit(1)

    sys.exit(0)

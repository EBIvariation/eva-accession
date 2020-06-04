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

from get_assembly_report_url import *
import os
import pandas
import sys
import urllib.request
import re
import json
import yaml
from retry import retry
from __init__ import logger


def build_fasta_from_assembly_report(assembly_report_path, assembly_accession, eutils_api_key, directory_path, clear):
    fasta_path = directory_path + assembly_accession + '.fa'
    if clear:
        clear_file_content(fasta_path)
    written_contigs = get_written_contigs(fasta_path)
    for chunk in pandas.read_csv(assembly_report_path, skiprows=get_header_line_index(assembly_report_path),
                                 dtype=str, sep='\t', chunksize=100):
        new_contigs_in_chunk = process_chunk(chunk, written_contigs, eutils_api_key, fasta_path)
        written_contigs.extend(new_contigs_in_chunk)


def clear_file_content(path):
    with open(path, 'w'):
        pass


def get_written_contigs(fasta_path):
    try:
        written_contigs = []
        match = re.compile(r'>(.*?)\s')
        with open(fasta_path, 'r') as file:
            for line in file:
                written_contigs.extend(match.findall(line))
        return written_contigs
    except FileNotFoundError:
        logger.info('FASTA file does not exists, starting from scratch')
        return []


def get_header_line_index(assembly_report_path):
    try:
        with open(assembly_report_path) as assembly_report_file_handle:
            return next(line_index for line_index, line in enumerate(assembly_report_file_handle)
                        if line.lower().startswith("# sequence-name") and "sequence-role" in line.lower())
    except StopIteration:
        raise Exception("Could not locate header row in the assembly report!")


def process_chunk(assembly_report_dataframe, written_contigs, eutils_api_key, fasta_path):
    new_contigs = []
    for index, row in assembly_report_dataframe.iterrows():
        genbank_accession = row['GenBank-Accn']
        refseq_accession = row['RefSeq-Accn']
        relationship = row['Relationship']
        accession = genbank_accession
        if relationship != '=' and genbank_accession == 'na':
            accession = refseq_accession
        if written_contigs is not None and (genbank_accession in written_contigs or refseq_accession in written_contigs):
            logger.info('Accession ' + accession + ' already in the FASTA file, don\'t need to be downloaded')
            continue
        new_contig = get_sequence_from_ncbi(accession, fasta_path, eutils_api_key)
        if new_contig is not None:
            new_contigs.append(new_contig)
    return new_contigs


@retry(tries=4, delay=2, backoff=1.2, jitter=(1, 3))
def get_sequence_from_ncbi(accession, fasta_path, eutils_api_key):
    url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=nuccore&id=' + accession + \
           '&rettype=fasta&retmode=text&api_key=' + eutils_api_key + '&tool=eva&email=eva-dev@ebi.ac.uk'
    logger.info('Downloading ' + accession)
    sequence_tmp_path = os.path.dirname(fasta_path) + '/' + accession
    urllib.request.urlretrieve(url, sequence_tmp_path)
    with open(sequence_tmp_path) as sequence:
        first_line = sequence.readline()
        second_line = sequence.readline()
        if not second_line:
            logger.info('FASTA sequence not available for ' + accession)
            os.remove(sequence_tmp_path)
        else:
            contatenate_sequence_to_fasta(fasta_path, sequence_tmp_path)
            logger.info(accession + " downloaded and added to FASTA sequence")
            os.remove(sequence_tmp_path)
            return accession


def contatenate_sequence_to_fasta(fasta_path, sequence_path):
    with open(fasta_path, 'a+') as fasta:
        with open(sequence_path) as sequence:
            for line in sequence:
                fasta.write(line)


@retry(tries=4, delay=2, backoff=1.2, jitter=(1, 3))
def download_assembly_report(assembly_accession, directory_path):
    assembly_report_url = get_assembly_report_url(assembly_accession)
    assembly_report_path = directory_path + os.path.basename(assembly_report_url)
    os.makedirs(os.path.dirname(assembly_report_path), exist_ok=True)
    urllib.request.urlretrieve(assembly_report_url, assembly_report_path)
    urllib.request.urlcleanup()
    return assembly_report_path


def build_output_directory_path(assembly_accession, private_config_args, output_directory):
    if output_directory is not None:
        directory_path = output_directory
    else:
        eva_root_dir = private_config_args['eva_root_dir']
        directory_path = eva_root_dir
    directory_path += os.path.sep + assembly_accession + os.path.sep
    logger.info('Files will be downloaded in ' + directory_path)
    return directory_path


def get_args_from_private_config_file(private_config_file):
    with open(private_config_file) as private_config_file_handle:
        if 'json' in private_config_file:
            return json.load(private_config_file_handle)
        else:
            if 'yml' in private_config_file:
                return yaml.safe_load(private_config_file_handle)
            else:
                raise TypeError('Configuration file should be either json or yaml')


def main(assembly_accession, output_directory, private_config_file, clear):
    private_config_args = get_args_from_private_config_file(private_config_file)
    eutils_api_key = private_config_args['eutils_api_key']
    directory_path = build_output_directory_path(assembly_accession, private_config_args, output_directory)
    assembly_report_path = download_assembly_report(assembly_accession, directory_path)
    build_fasta_from_assembly_report(assembly_report_path, assembly_accession, eutils_api_key, directory_path, clear)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Genome downloader assembly', add_help=False)
    parser.add_argument("-a", "--assembly-accession",
                        help="Assembly for which the process has to be run, e.g. GCA_000002285.2", required=True)
    parser.add_argument("-o", "--output-directory", help="Output directory for the assembly report and FASTA file",
                        required=False)
    parser.add_argument("-p", "--private-config-file",
                        help="Path to the configuration file with private info (JSON format)", required=True)
    parser.add_argument("-c", "--clear",
                        help="Flag to clear existing data in FASTA file and starting from scratch",
                        action='store_true')
    parser.add_argument('--help', action='help', help='Show this help message and exit')

    args = {}
    try:
        args = parser.parse_args()
        main(args.assembly_accession, args.output_directory, args.private_config_file, args.clear)
    except Exception as ex:
        logger.exception(ex)
        sys.exit(1)

    sys.exit(0)

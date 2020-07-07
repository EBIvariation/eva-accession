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

import http
import logging
import requests
import sys
import subprocess


def retrieve_genbank_equivalent_for_GCF_accession(assembly_accession):
    eutils_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/'
    esearch_url = eutils_url + 'esearch.fcgi'
    esummary_url = eutils_url + 'esummary.fcgi'

    payload = {'db': 'Assembly', 'term': '"{}"'.format(assembly_accession), 'retmode': 'JSON'}
    data = requests.get(esearch_url, params=payload).json()

    assembly_id_list = data.get('esearchresult').get('idlist')
    payload = {'db': 'Assembly', 'id': ','.join(assembly_id_list), 'retmode': 'JSON'}

    summary_list = requests.get(esummary_url, params=payload).json()
    genbank_synonyms = set()
    if summary_list.get('result') is None:
        raise ValueError('No Genbank synonyms found for assembly %s ' % assembly_accession)
    for assembly_id in summary_list.get('result').get('uids'):
        assembly_info = summary_list.get('result').get(assembly_id)
        genbank_synonyms.add(assembly_info.get('synonym').get('genbank'))
    if len(genbank_synonyms) != 1:
        raise ValueError('%s Genbank synonyms found for assembly %s ' % (len(genbank_synonyms), assembly_accession))
    return genbank_synonyms.pop()


def resolve_assembly_name_to_GCA_accession(assembly_name):
    ENA_ASSEMBLY_NAME_QUERY_URL = "https://www.ebi.ac.uk/ena/portal/api/search" \
                                  "?result=assembly&query=assembly_name%3D%22{0}%22&format=json".format(assembly_name)
    response = requests.get(ENA_ASSEMBLY_NAME_QUERY_URL)
    if response.status_code == http.HTTPStatus.OK.value:
        response_json = response.json()
        print(response_json)
        if len(response_json) == 0:
            raise ValueError("Could not resolve assembly name {0} to a GCA accession!".format(assembly_name))
        elif len(response_json) > 1:
            raise ValueError("Assembly name {0} resolved to more than one GCA accession!".format(assembly_name))
        else:
            return response.json()[0]["accession"] + "." + response.json()[0]["version"]
    else:
        raise ValueError("Could not resolve assembly name {0} to a GCA accession!".format(assembly_name))


def init_logger():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)-15s %(levelname)s %(message)s')
    result_logger = logging.getLogger(__name__)
    return result_logger


def run_command_with_output(command_description, command, return_process_output=False):
    process_output = ""

    logger.info("Starting process: " + command_description)
    logger.info("Running command: " + command)

    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1, universal_newlines=True,
                          shell=True) as process:
        for line in iter(process.stdout.readline, ''):
            line = str(line).rstrip()
            logger.info(line)
            if return_process_output:
                process_output += line + "\n"
        for line in iter(process.stderr.readline, ''):
            line = str(line).rstrip()
            logger.error(line)
    if process.returncode != 0:
        logger.error(command_description + " failed! Refer to the error messages for details.")
        raise subprocess.CalledProcessError(process.returncode, process.args)
    else:
        logger.info(command_description + " - completed successfully")
    if return_process_output:
        return process_output


logger = init_logger()

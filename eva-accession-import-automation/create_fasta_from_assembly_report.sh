#!/bin/bash
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

# Usage: create_fasta_from_assembly_report.sh <species> </path/to/assembly_report.txt> <output folder> <eutils API key>

if [ "$#" -ne 4 ]
then
    echo "Please provide the species (eg., tomato_4081), the path to the assembly report, the output folder and the NCBI EUtils API key."
    exit 1
fi

species=$1
assembly_report=$2
output_folder=$3
# Need explicit API key for EUtils to counter any throttling/blocking issues
eutils_api_key=$4

exit_code=0

# To ensure the file exists before running `grep -c`, otherwise it will always fail
touch ${output_folder}/${species}_custom.fa
touch ${output_folder}/written_contigs.txt

# remove the FASTA index because it might get outdated (EVA-1581)
rm ${output_folder}/${species}_custom.fa.fai

# If using a pre-existing FASTA file, identify the sequences already downloaded to avoid doing it again
grep '^>' ${output_folder}/${species}_custom.fa | cut -d' ' -f1 | sed 's/>//g' >> ${output_folder}/written_contigs.txt

download_fasta_to_contig_file () {
    # make $? return the error code of any failed command in the pipe, instead of the last one only
    set -o pipefail
    times_wget_failed=0
    max_allowed_attempts=5

    echo "Downloading from $1"

    while [ $times_wget_failed -lt $max_allowed_attempts ]
    do
        # Download each GenBank accession in the assembly report from ENA into a separate file
        # Delete the accession prefix from the header line
        wget -q -O - "$1" > ${output_folder}/${contig_to_fetch}

        whole_pipe_result=$?
        if [ $whole_pipe_result -eq 0 ]
        then
            # Special checks when NCBI FASTA is retrieved
            if [[ $1 == *entrez* && $1 == *eutils* ]]; then
                # Check if the downloaded FASTA was for the correct contig
                first_line=`head -1 ${output_folder}/${contig_to_fetch}`
                if [[ $first_line != *"${contig_to_fetch}"* ]]; then
                    truncate --size 0 ${output_folder}/${contig_to_fetch}
                    echo "${contig_to_fetch}: Did not get the FASTA for the correct contig!"
                    break
                fi

                # This is needed because eFetch endpoint sometimes returns extra blank lines at the end
                num_lines_in_contig_fasta=`wc -l < ${output_folder}/${contig_to_fetch}`
                while [ $num_lines_in_contig_fasta -ge 1 ]
                do
                    trailing_line=`tail -1 ${output_folder}/${contig_to_fetch}`
                    if [[ $trailing_line == "" ]]; then
                        sed -i '$ d' ${output_folder}/${contig_to_fetch}
                    else
                        break
                    fi
                    num_lines_in_contig_fasta=`wc -l < ${output_folder}/${contig_to_fetch}`
                done
            fi

            # it was correctly downloaded
            break
        fi

        times_wget_failed=$(($times_wget_failed + 1))

        # log the error only once
        if [ $times_wget_failed -eq 1 ]
        then
            echo Download for ${contig_to_fetch} failed. Retrying...
        fi
    done
}

for contig_components in `grep -v -e "^#" ${assembly_report} | awk '{ print $5 "," $6 "," $7}'`;
do
    genbank_contig=`echo $contig_components | cut -d, -f1`
    equivalence=`echo $contig_components | cut -d, -f2`
    refseq_contig=`echo $contig_components | cut -d, -f3`

    contig_to_fetch=$genbank_contig
    if [ "$equivalence" != "=" ] && [ "$genbank_contig" = "na" ]; then
        contig_to_fetch=$refseq_contig
    fi

    echo ${contig_to_fetch}

    times_wget_failed=0
    max_allowed_attempts=5

    # Check if the contig has already been downloaded
    already_downloaded=`grep -F -w -c "${contig_to_fetch}" ${output_folder}/written_contigs.txt`
    if [ $already_downloaded -eq 1 ]
    then
        echo Contig ${contig_to_fetch} is already present in the FASTA file and doesnt need to be downloaded again.
        continue
    fi

    download_fasta_to_contig_file "https://www.ebi.ac.uk/ena/browser/api/fasta/${contig_to_fetch}"
    lines=`head -n 2 ${output_folder}/${contig_to_fetch} | wc -l`
    # Hail Mary pass - Use NCBI FASTA!!
    if [ $lines -le 1 ]
    then
        echo "Failed retrieving ENA FASTA for ${contig_to_fetch}. Therefore, using NCBI FASTA for ${contig_to_fetch}..."
        # Due to NCBI policy of limiting direct EUtils requests to 10 per second (see https://www.ncbi.nlm.nih.gov/books/NBK25497/),
        # introduce a delay of 1 second before calling EFetch just in case these requests line up (unlikely but playing it safe...)
        sleep 1
        download_fasta_to_contig_file "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=nuccore&id=${contig_to_fetch}&rettype=fasta&retmode=text&api_key=${eutils_api_key}&tool=eva&email=eva-dev@ebi.ac.uk"
    fi

    # If a file has more than one line, then it is concatenated into the full assembly FASTA file
    # (empty sequences can't be indexed)
    lines=`head -n 2 ${output_folder}/${contig_to_fetch} | wc -l`
    if [ $lines -le 1 ]
    then
        echo FASTA sequence not available for ${contig_to_fetch}
        exit_code=1
    else
        # if the file is not empty but wget returned an error stop the program!
        if [ $times_wget_failed -eq $max_allowed_attempts ]
        then
            echo Could not download ${contig_to_fetch} completely. FASTA file is left incomplete.
            exit_code=1
        else
            # Write the sequence associated with an accession to a FASTA file only once
            # grep explanation: -m 1 means "stop after finding first match". -c means "output the number of matches"
            matches=`grep -F -w -m 1 -c "${contig_to_fetch}" ${output_folder}/written_contigs.txt`
            if [ $matches -eq 0 ]
            then
                # If the downloaded file contains a WGS, it will be compressed
                is_wgs=$(file "${output_folder}/${contig_to_fetch}" | grep -c 'gzip')
                if [ $is_wgs -eq 1 ]
                then
                    # Uncompress file
                    mv ${output_folder}/${contig_to_fetch} ${output_folder}/${contig_to_fetch}.gz
                    gunzip ${output_folder}/${contig_to_fetch}.gz
                fi
                # Delete the accessions prefix
                sed -i 's/\s*ENA\s*|.*|\s*//g' ${output_folder}/${contig_to_fetch}
                # Add sequence to FASTA file
                cat ${output_folder}/${contig_to_fetch} >> ${output_folder}/${species}_custom.fa
                # Register written contigs
                if [ $is_wgs -eq 1 ]
                then
                    grep '>' ${output_folder}/${contig_to_fetch} | cut -d' ' -f1 | sed 's/>//g' >> ${output_folder}/written_contigs.txt
                else
                    echo "${contig_to_fetch}" >> ${output_folder}/written_contigs.txt
                fi
            fi
        fi
    fi

    # Delete temporary contig file
    rm ${output_folder}/${contig_to_fetch}
done

echo `grep -v "^#" ${assembly_report}  | wc -l` "contigs were present in the assembly report"
echo `cat ${output_folder}/written_contigs.txt | wc -l` "contigs were successfully retrieved and written in the FASTA file"
rm ${output_folder}/written_contigs.txt
echo "Exiting CREATE FASTA script with exit code: ${exit_code}"
exit $exit_code

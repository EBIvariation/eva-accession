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

# Usage: create_fasta_from_assembly_report.sh <species> </path/to/assembly_report.txt> <output folder>

if [ "$#" -ne 3 ]
then
    echo "Please provide the species (eg., tomato_4081), the path to the assembly report and the output folder."
    exit 1
fi

species=$1
assembly_report=$2
output_folder=$3

exit_code=0

# To ensure the file exists before running `grep -c`, otherwise it will always fail
touch ${output_folder}/${species}_custom.fa
touch ${output_folder}/written_contigs.txt

for genbank_contig in `grep -v -e "^#" ${assembly_report} | cut -f5`;
do
    echo ${genbank_contig}

    # make $? return the error code of any failed command in the pipe, instead of the last one only
    set -o pipefail
    times_wget_failed=0
    max_allowed_attempts=5
    while [ $times_wget_failed -lt $max_allowed_attempts ]
    do
        # Download each GenBank accession in the assembly report from ENA into a separate file
        # Delete the accession prefix from the header line
        wget -q -O - "https://www.ebi.ac.uk/ena/browser/api/fasta/${genbank_contig}" | sed 's/ENA|.*|//g' > ${output_folder}/${genbank_contig}
        whole_pipe_result=$?
        if [ $whole_pipe_result -eq 0 ]
        then
            # it was correctly downloaded
            break
        fi
        echo Download for ${genbank_contig} failed. Retrying...
        times_wget_failed=$(($times_wget_failed + 1))
    done

    # If a file has more than one line, then it is concatenated into the full assembly FASTA file
    # (empty sequences can't be indexed)
    lines=`head -n 2 ${output_folder}/${genbank_contig} | wc -l`
    if [ $lines -le 1 ]
    then
        echo FASTA sequence not available for ${genbank_contig}
        exit_code=1
    else
        # if the file is not empty but wget returned an error stop the program!
        if [ $times_wget_failed -eq $max_allowed_attempts ]
        then
            echo Could not download ${genbank_contig} completely. FASTA file is left incomplete.
            exit_code=1
        else
            # Write the sequence associated with an accession to a FASTA file only once
            # grep explanation: -m 1 means "stop after finding first match". -c means "output the number of matches"
            matches=`grep -m 1 -c "${genbank_contig}" ${output_folder}/written_contigs.txt`
            if [ $matches -eq 0 ]
            then
                cat ${output_folder}/${genbank_contig} >> ${output_folder}/${species}_custom.fa
                echo "${genbank_contig}" >> ${output_folder}/written_contigs.txt
            fi
        fi
    fi

    # Delete temporary contig file
    rm ${output_folder}/${genbank_contig}
done

echo `grep -v "^#" ${assembly_report}  | wc -l` "contigs were present in the assembly report"
echo `cat ${output_folder}/written_contigs.txt | wc -l` "contigs were successfully retrieved and written in the FASTA file"
rm ${output_folder}/written_contigs.txt

exit $exit_code

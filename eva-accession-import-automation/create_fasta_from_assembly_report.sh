#!/bin/bash

# Usage: create_fasta_from_assembly_report.sh <assembly accession> </path/to/assembly_report.txt> <output folder>

if [ "$#" -ne 3 ]
then
    echo "Please provide the assembly accession, the path to the assembly report and the output folder."
    exit 1
fi

assembly_accession=$1
assembly_report=$2
output_folder=$3

exit_code=0

# To ensure the file exists before running `grep -c`, otherwise it will always fail
touch ${output_folder}/${assembly_accession}.fa
touch ${output_folder}/written_contigs.txt

for genbank_contig in `grep -v -e "^#" ${assembly_report} | cut -f5`;
do
    echo ${genbank_contig}

    # Download each GenBank accession in the assembly report from ENA into a separate file
    # Delete the accession prefix from the header line
    wget -q -O - "https://www.ebi.ac.uk/ena/browser/api/fasta/${genbank_contig}" | sed 's/ENA|.*|//g' > ${output_folder}/${genbank_contig}

    # If a file has more than one line, then it is concatenated into the full assembly FASTA file
    # (empty sequences can't be indexed)
    lines=`head -n 2 ${output_folder}/${genbank_contig} | wc -l`
    if [ $lines -le 1 ]
    then
        echo FASTA sequence not available for ${genbank_contig}
        exit_code=1
    else
        # Write the sequence associated with an accession to a FASTA file only once
        # grep explanation: -m 1 means "stop after finding first match". -c means "output the number of matches"
        matches=`grep -m 1 -c "${genbank_contig}" ${output_folder}/written_contigs.txt`
        if [ $matches -eq 0 ]
        then
            cat ${output_folder}/${genbank_contig} >> ${output_folder}/${assembly_accession}.fa
            echo "${genbank_contig}" >> ${output_folder}/written_contigs.txt
        fi
    fi

    # Delete temporary contig file
    rm ${output_folder}/${genbank_contig}
done

echo `grep -v "^#" ${assembly_report}  | wc -l` "contigs were present in the assembly report"
echo `cat ${output_folder}/written_contigs.txt | wc -l` "contigs were successfully retrieved and written in the FASTA file"
rm ${output_folder}/written_contigs.txt

exit $exit_code

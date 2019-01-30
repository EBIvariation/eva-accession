#!/bin/bash

# Usage: create_fasta_from_assembly_report.sh </path/to/assembly_report.txt>

if [ "$#" -ne 3 ]
then
    echo "Please provide the assembly accession, the path to the assembly report and the output folder."
    exit 1
fi

assembly_accession=$1
assembly_report=$2
output_folder=$3

for i in `grep -v -e "^#" ${assembly_report} | cut -f5`;
do
    echo ${i}

    # Download each GenBank accession in the assembly report from ENA into a separate file
    # Delete the accession prefix from the header line
    wget -q -O - "https://www.ebi.ac.uk/ena/data/view/${i}&display=fasta" | sed 's/ENA|.*|//g' > ${i}

    # If a file has more than one line, then it is concatenated into the full assembly FASTA file
    # (empty sequences can't be indexed)
    lines=`head -n 2 ${i} | wc -l`
    if [ $lines -ne 1 ]
    then
        cat ${i} >> ${output_folder}/${assembly_accession}.fa
    else
        echo FASTA sequence not available for ${i}
    fi

    # Check that an accession is present no more than once in the output FASTA file, otherwise it 
    # means there are unexpected aliases in the assembly report, which need to be checked
    acc=`head -n 1 ${i} | cut -f1 -d' ' | cut -f1 -d'.' | cut -f2 -d'>'`
    matches=`grep -c "${acc}" ${output_folder}/${assembly_accession}.fa`
    if [ $matches -gt 1 ]
    then
        echo WARNING: Sequence ${i} found more than once in the output FASTA file
    fi
done


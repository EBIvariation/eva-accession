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

for genbank_contig in `grep -v -e "^#" ${assembly_report} | cut -f5`;
do
    echo ${genbank_contig}

    # Download each GenBank accession in the assembly report from ENA into a separate file
    # Delete the accession prefix from the header line
    wget -q -O - "https://www.ebi.ac.uk/ena/browser/api/fasta/${genbank_contig}" | sed 's/ENA|.*|//g' > ${genbank_contig}

    # If a file has more than one line, then it is concatenated into the full assembly FASTA file
    # (empty sequences can't be indexed)
    lines=`head -n 2 ${genbank_contig} | wc -l`
    if [ $lines -gt 1 ]
    then
        cat ${genbank_contig} >> ${output_folder}/${assembly_accession}.fa
    else
        echo FASTA sequence not available for ${genbank_contig}
        continue
    fi

    # Check that an accession is present no more than once in the output FASTA file, otherwise it 
    # means there are unexpected aliases in the assembly report, which need to be checked
    accession=`head -n 1 ${genbank_contig} | cut -f1 -d' ' | cut -f1 -d'.' | cut -f2 -d'>'`
    matches=`grep -c "${accession}" ${output_folder}/${assembly_accession}.fa`
    if [ $matches -gt 1 ]
    then
        echo WARNING: Sequence ${genbank_contig} found more than once in the output FASTA file
        exit_code=1
    fi
done

exit $exit_code


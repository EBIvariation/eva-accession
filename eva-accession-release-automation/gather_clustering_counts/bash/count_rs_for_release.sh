#!/bin/bash

set -e

species_dir=$1
idtype=$2
taxonomy=$3
species_name=$(basename "$species_dir")


for ASSEMBLY_DIR in "${species_dir}"/GC*
do
    ASSEMBLY=$(basename "$ASSEMBLY_DIR")
    zcat  "${species_dir}"/"$ASSEMBLY"/"${taxonomy}_${ASSEMBLY}"_"${idtype}"_ids.vcf.gz | grep -v '^#' | awk -v assembly="$ASSEMBLY" '{print $3" "assembly}'
done | sort > tmp_"${species_name}"_"${idtype}"_rsid_sorted

cat tmp_"${species_name}"_"${idtype}"_rsid_sorted \
    | awk '{if (current_rsid != $1){for (a in assemblies){printf " %s",a} print ""; delete assemblies; current_rsid=$1 }; assemblies[$2]=1} END{for (a in assemblies){printf " %s",a}}'  \
    | grep -v '^$' | sort | uniq -c | sort -nr > "${species_name}"_count_"${idtype}"_rsid.log

rm  tmp_"${species_name}"_"${idtype}"_rsid_sorted

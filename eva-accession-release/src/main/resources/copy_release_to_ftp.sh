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

# Usage: copy_release_to_ftp.sh <input_folder> <output_folder> <intermediate_folder> <unmapped_variants_folder>

set -eu -o pipefail

# This is the brain-damaged way to get the directory of the currently executing script in bash
# See https://stackoverflow.com/questions/59895/get-the-source-directory-of-a-bash-script-from-within-the-script-itself
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ASSEMBLIES="by_assembly"
ALL_SPECIES_FOLDER="by_species"

if [ $# -ne 6 ]
then
  echo -e "\nThis script copies the relevant files of all the species into the FTP folder for the EVA RS release."
  echo "The user needs to be able to run 'become ${FTP_USER}' to run this script."
  echo -e "This script needs 6 parameters. 4 folders (input, output, intermediate, unmapped variants), the postgres connection and the release column"
  echo -e "Also, recommended to run in LSF, e.g:"
  echo -e "$ bsub -o copy_release.log -e copy_release.err $0 <input_folder> <output_folder> <intermediate_folder> <unmapped_variants_folder> <postgres_connection>\n"
  echo -e "Postgres connection should have this format: postgresql://DB_USER:DB_PWD@DB_HOST/DB_NAME"
  exit 1
fi

INPUT_FOLDER=$1
OUTPUT_FOLDER=$2
INTERMEDIATE_FOLDER=$3
UNMAPPED_VARIANTS_FOLDER=$4
POSTGRES_CONNECTION=$5
RELEASE_COLUMN=$6
FTP_USER=$7

if [ ! -d ${OUTPUT_FOLDER} ]
then
  echo "Make sure that the output folder ${OUTPUT_FOLDER} exists. Aborting script, nothing was written."
  exit 1
fi

mkdir ${INTERMEDIATE_FOLDER}
if [ $? -eq 1 ]
then
  echo "Could not create intermediate folder ${INTERMEDIATE_FOLDER}. Aborting script, nothing was written."
  exit 1
fi

# query postgres to get assembly-taxonomy mappings
psql -A -t -F $'\t' ${POSTGRES_CONNECTION} -c "select distinct rs.assembly_accession, p.tax_id from dbsnp_ensembl_species.rs_release_progress rs join dbsnp_ensembl_species.import_progress p on rs.dbsnp_db_name = p.database_name where assembly_accession <> '' and ${RELEASE_COLUMN} = 'done' order by assembly_accession, tax_id" -P pager=off > ${INTERMEDIATE_FOLDER}/assembly_to_taxonomy_map.txt

echo -e "\nCopying to intermediate folder ${INTERMEDIATE_FOLDER}"
# copy only relevant files. Any missing file will be listed in stderr. It's ok if some species don't have the *_issues.txt

mkdir ${INTERMEDIATE_FOLDER}/${ALL_SPECIES_FOLDER}

# copy unmapped_ids reports
cd ${UNMAPPED_VARIANTS_FOLDER}
tail -n +2 ${INPUT_FOLDER}/species_name_mapping.tsv |
while read species_line
do
  taxonomy=`echo "${species_line}" | cut -f 5`
  species_folder=`echo "${species_line}" | cut -f 1`
  dbsnp_database_name=`echo "${species_line}" | cut -f 4`
  mkdir ${INTERMEDIATE_FOLDER}/${ALL_SPECIES_FOLDER}/${species_folder}
  if [ -f ${UNMAPPED_VARIANTS_FOLDER}/${dbsnp_database_name}_unmapped_ids.txt.gz ]
  then
    cp ${dbsnp_database_name}_unmapped_ids.txt.gz ${INTERMEDIATE_FOLDER}/${ALL_SPECIES_FOLDER}/${species_folder}/
    md5sum ${dbsnp_database_name}_unmapped_ids.txt.gz > ${INTERMEDIATE_FOLDER}/${ALL_SPECIES_FOLDER}/${species_folder}/md5checksums.txt
    echo "# Unique RS ID counts" > ${INTERMEDIATE_FOLDER}/${ALL_SPECIES_FOLDER}/${species_folder}/README_unmapped_rs_ids_count.txt
    zcat ${dbsnp_database_name}_unmapped_ids.txt.gz | tail -n +2 | cut -f 1 | sort -u | wc -l | sed 's/^/'${dbsnp_database_name}_unmapped_ids.txt.gz'\t/' >> ${INTERMEDIATE_FOLDER}/${ALL_SPECIES_FOLDER}/${species_folder}/README_unmapped_rs_ids_count.txt
  fi
done
cd -

mkdir ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}
cp $INPUT_FOLDER/README_general_info.txt ${INTERMEDIATE_FOLDER}/
cp $INPUT_FOLDER/species_name_mapping.tsv ${INTERMEDIATE_FOLDER}/

# copy assembly folders
for assembly in `ls $INPUT_FOLDER | grep ^GCA_`
do
  echo "Copying $assembly"
  mkdir ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly}
  cp $INPUT_FOLDER/README_general_info.txt ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly}
  cd $INPUT_FOLDER/${assembly}
  cp README_species_issues.txt ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly} || true
  cp README_rs_ids_counts.txt ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly} || true
  cp ${assembly}_merged_ids.vcf.gz ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly} || true
  md5sum ${assembly}_merged_ids.vcf.gz > ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly}/md5checksums.txt || true
  cp ${assembly}_merged_ids.vcf.gz.tbi ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly} || true
  md5sum ${assembly}_merged_ids.vcf.gz.tbi >> ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly}/md5checksums.txt || true
  cp ${assembly}_current_ids.vcf.gz ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly} || true
  md5sum ${assembly}_current_ids.vcf.gz >> ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly}/md5checksums.txt || true
  cp ${assembly}_current_ids.vcf.gz.tbi ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly} || true
  md5sum ${assembly}_current_ids.vcf.gz.tbi >> ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly}/md5checksums.txt || true
  cp ${assembly}_deprecated_ids.txt.gz ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly} || true
  md5sum ${assembly}_deprecated_ids.txt.gz >> ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly}/md5checksums.txt || true
  cp ${assembly}_merged_deprecated_ids.txt.gz ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly} || true
  md5sum ${assembly}_merged_deprecated_ids.txt.gz >> ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly}/md5checksums.txt || true
  cd -

  for taxonomy in `grep ${assembly} ${INTERMEDIATE_FOLDER}/assembly_to_taxonomy_map.txt | cut -f2`
  do
    dbsnp_database_name=`grep -w "${taxonomy}$" ${INPUT_FOLDER}/species_name_mapping.tsv | cut -f4` || true
    if [ -z "${dbsnp_database_name}" ]
    then
      echo "Warning: taxonomy ${taxonomy} not found in ${INPUT_FOLDER}/species_name_mapping.tsv. Won't copy the unmapped_ids report for that taxonomy."
    else
      cd ${UNMAPPED_VARIANTS_FOLDER}
      cp ${dbsnp_database_name}_unmapped_ids.txt.gz ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly} || true
      species_folder=`grep -w "${taxonomy}$" ${INPUT_FOLDER}/species_name_mapping.tsv | cut -f1` || true
      cat ${INTERMEDIATE_FOLDER}/${ALL_SPECIES_FOLDER}/${species_folder}/md5checksums.txt >> ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly}/md5checksums.txt || true
      tail -n +2 ${INTERMEDIATE_FOLDER}/${ALL_SPECIES_FOLDER}/${species_folder}/README_unmapped_rs_ids_count.txt >> ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly}/README_rs_ids_counts.txt || true
      cd -
    fi
  done
done

echo -e "\nCopying to FTP folder ${OUTPUT_FOLDER}"
become ${FTP_USER} rsync -va --exclude 'assembly_to_taxonomy_map.txt' ${INTERMEDIATE_FOLDER}/* ${OUTPUT_FOLDER}/

# add symlinks to assemblies inside each species' folder
cat ${INTERMEDIATE_FOLDER}/assembly_to_taxonomy_map.txt |
while read assembly_and_species
do
  assembly=`echo "${assembly_and_species}" | cut -f 1`
  taxonomy=`echo "${assembly_and_species}" | cut -f 2`

  assembly_folder=${OUTPUT_FOLDER}/${ASSEMBLIES}/${assembly}
  species_folder=`grep -w "${taxonomy}$" $INPUT_FOLDER/species_name_mapping.tsv | cut -f 1` || true
  if [ -z "${species_folder}" ]
  then
    echo "Warning: taxonomy ${taxonomy} not found in ${INPUT_FOLDER}/species_name_mapping.tsv. Won't add symbolic links to the assembly folders of this taxonomy."
  else
    # only put assembly subfolders for species whose assemblies are present
    if [ -d $assembly_folder ]
    then
      become ${FTP_USER} mkdir -p ${OUTPUT_FOLDER}/${ALL_SPECIES_FOLDER}/${species_folder}

      symbolic_link_name=${OUTPUT_FOLDER}/${ALL_SPECIES_FOLDER}/${species_folder}/${assembly}
      # doesn't work! needs relative paths
      #become ${FTP_USER} ln -sfT ${assembly_folder} ${symbolic_link_name}
      become ${FTP_USER} ln -sfT ../../${ASSEMBLIES}/${assembly} ${symbolic_link_name}
    fi
  fi
done

echo -e "\nFinished copying. Removing intermediate copy at ${INTERMEDIATE_FOLDER}"
rm -rf ${INTERMEDIATE_FOLDER}

become ${FTP_USER} python3 "$SCRIPT_DIR/create_assembly_name_symlinks.py" ${OUTPUT_FOLDER}/${ALL_SPECIES_FOLDER}

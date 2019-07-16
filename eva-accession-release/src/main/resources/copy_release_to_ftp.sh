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

ASSEMBLIES=by_assembly
ALL_SPECIES_FOLDER=by_species

if [ $# -ne 4 ]
then
  echo -e "\nThis script copies the relevant files of all the species into the FTP folder for the EVA RS release."
  echo "The user needs to be able to run 'become ftpadmin' to run this script."
  echo -e "This script needs as parameter 4 folders: input, output, intermediate and unmapped variants."
  echo -e "Also, recommended to run in LSF, e.g:"
  echo -e "$ bsub -o copy_release.log -e copy_release.err $0 <input_folder> <output_folder> <intermediate_folder> <unmapped_variants_folder>\n"
  exit 1
fi

INPUT_FOLDER=$1
OUTPUT_FOLDER=$2
INTERMEDIATE_FOLDER=$3
UNMAPPED_VARIANTS_FOLDER=$4

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

# taken from vrnevadev:
#
# select distinct rs.assembly_accession, p.tax_id
# from dbsnp_ensembl_species.rs_release_progress rs
# join dbsnp_ensembl_species.import_progress p
# on rs.dbsnp_db_name = p.database_name
# where assembly_accession <> ''
# order by assembly_accession, tax_id;
echo "GCA_000001215.4	7227
GCA_000001515.4	9598
GCA_000001545.3	9601
GCA_000001635.4	10090
GCA_000001635.5	10090
GCA_000001635.6	10090
GCA_000001735.1	3702
GCA_000001895.4	10116
GCA_000002035.2	7955
GCA_000002035.3	7955
GCA_000002175.2	9598
GCA_000002195.1	7460
GCA_000002265.1	10116
GCA_000002275.2	9258
GCA_000002285.1	9615
GCA_000002285.2	9615
GCA_000002295.1	13616
GCA_000002305.1	9796
GCA_000002315.3	9031
GCA_000002335.3	7070
GCA_000002655.1	330879
GCA_000002765.1	5833
GCA_000002775.1	3694
GCA_000002985.3	6239
GCA_000003025.4	9823
GCA_000003025.6	9823
GCA_000003055.5	9913
GCA_000003195.1	4558
GCA_000003625.1	9986
GCA_000003745.2	29760
GCA_000004515.2	3847
GCA_000004515.3	3847
GCA_000004665.1	9483
GCA_000005005.6	4577
GCA_000005425.2	4530
GCA_000005575.1	7165
GCA_000146605.3	9103
GCA_000146795.3	61853
GCA_000148765.2	3750
GCA_000151805.2	59729
GCA_000151905.1	9593
GCA_000181335.3	9685
GCA_000188115.2	4081
GCA_000188235.2	8128
GCA_000219495.1	3880
GCA_000224145.2	7719
GCA_000233375.4	8030
GCA_000237925.2	6183
GCA_000247795.2	9915
GCA_000247815.2	59894
GCA_000298735.1	469796
GCA_000298735.2	9940
GCA_000309985.1	3711
GCA_000317375.1	79684
GCA_000317765.1	9923
GCA_000317765.1	9925
GCA_000331145.1	3827
GCA_000355885.1	8839
GCA_000364345.1	9541
GCA_000372685.1	7994
GCA_000409795.2	60711
GCA_000413155.1	42345
GCA_000442705.1	51953
GCA_000686985.1	3708
GCA_000695525.1	3712
GCA_000710875.1	4072
GCA_000772875.3	9544
GCA_000966335.1	7950
GCA_000987745.1	3635
GCA_001433935.1	4530
GCA_001465895.2	105023
GCA_001522545.1	9157
GCA_001577835.1	93934
GCA_001625215.1	79200" > /tmp/assembly_to_taxonomy_map.txt

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
  cp ${UNMAPPED_VARIANTS_FOLDER}/${dbsnp_database_name}* ${INTERMEDIATE_FOLDER}/${ALL_SPECIES_FOLDER}/${species_folder}/
  md5sum ${dbsnp_database_name}* > ${INTERMEDIATE_FOLDER}/${ALL_SPECIES_FOLDER}/${species_folder}/unmapped_md5checksum.txt
done
cd -

mkdir ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}
cp $INPUT_FOLDER/README_general_info.txt ${INTERMEDIATE_FOLDER}/
cp $INPUT_FOLDER/species_name_mapping.tsv ${INTERMEDIATE_FOLDER}/

# copy assembly folders
for assembly in `ls $INPUT_FOLDER | grep GCA_`
do
  echo "Copying $assembly"
  mkdir ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly}
  cp $INPUT_FOLDER/README_general_info.txt ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly}
  cd $INPUT_FOLDER/${assembly}
  cp README_species_issues.txt ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly}
  cp ${assembly}_merged_ids.vcf.gz ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly}
  md5sum ${assembly}_merged_ids.vcf.gz > ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly}/md5checksums.txt
  cp ${assembly}_merged_ids.vcf.gz.tbi ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly}
  md5sum ${assembly}_merged_ids.vcf.gz.tbi >> ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly}/md5checksums.txt
  cp ${assembly}_current_ids.vcf.gz ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly}
  md5sum ${assembly}_current_ids.vcf.gz >> ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly}/md5checksums.txt
  cp ${assembly}_current_ids.vcf.gz.tbi ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly}
  md5sum ${assembly}_current_ids.vcf.gz.tbi >> ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly}/md5checksums.txt
  cp ${assembly}_deprecated_ids.txt.gz ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly}
  md5sum ${assembly}_deprecated_ids.txt.gz >> ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly}/md5checksums.txt
  cp ${assembly}_merged_deprecated_ids.txt.gz ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly}
  md5sum ${assembly}_merged_deprecated_ids.txt.gz >> ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly}/md5checksums.txt
  cd -

  for taxonomy in `grep ${assembly} /tmp/assembly_to_taxonomy_map.txt | cut -f2`
  do
    dbsnp_database_name=`grep -w "${taxonomy}$" ${INPUT_FOLDER}/species_name_mapping.tsv | cut -f4`
    if [ -z "${dbsnp_database_name}" ]
    then
      echo "Warning: taxonomy ${taxonomy} not found in ${INPUT_FOLDER}/species_name_mapping.tsv. Won't copy the unmapped_ids report for that taxonomy."
    else
      cd ${UNMAPPED_VARIANTS_FOLDER}
      cp ${dbsnp_database_name}* ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly}
      species_folder=`grep -w "${taxonomy}$" ${INPUT_FOLDER}/species_name_mapping.tsv | cut -f1`
      cat ${INTERMEDIATE_FOLDER}/${ALL_SPECIES_FOLDER}/${species_folder}/unmapped_md5checksum.txt >> ${INTERMEDIATE_FOLDER}/${ASSEMBLIES}/${assembly}/md5checksums.txt
      cd -
    fi
  done
done

echo -e "\nCopying to FTP folder ${OUTPUT_FOLDER}"
become ftpadmin rsync -va ${INTERMEDIATE_FOLDER}/* ${OUTPUT_FOLDER}/

# add symlinks to assemblies inside each species' folder
cat /tmp/assembly_to_taxonomy_map.txt |
while read assembly_and_species
do
  assembly=`echo "${assembly_and_species}" | cut -f 1`
  taxonomy=`echo "${assembly_and_species}" | cut -f 2`

  assembly_folder=${OUTPUT_FOLDER}/${ASSEMBLIES}/${assembly}
  species_folder=`grep -w "${taxonomy}$" $INPUT_FOLDER/species_name_mapping.tsv | cut -f 1`

  if [ -z "${species_folder}" ]
  then
    echo "Warning: taxonomy ${taxonomy} not found in ${INPUT_FOLDER}/species_name_mapping.tsv. Won't add symbolic links to the assembly folders of this taxonomy."
  else
    # only put assembly subfolders for species whose assemblies are present
    if [ -d $assembly_folder ]
    then
      become ftpadmin mkdir -p ${OUTPUT_FOLDER}/${ALL_SPECIES_FOLDER}/${species_folder}

      symbolic_link_name=${OUTPUT_FOLDER}/${ALL_SPECIES_FOLDER}/${species_folder}/${assembly}
      # doesn't work! needs relative paths
      #become ftpadmin ln -sfT ${assembly_folder} ${symbolic_link_name}
      become ftpadmin ln -sfT ../../${ASSEMBLIES}/${assembly} ${symbolic_link_name}
    fi
  fi
done

echo -e "\nFinished copying. Removing intermediate copy at ${INTERMEDIATE_FOLDER}"
rm -rf ${INTERMEDIATE_FOLDER}

rm /tmp/assembly_to_taxonomy_map.txt

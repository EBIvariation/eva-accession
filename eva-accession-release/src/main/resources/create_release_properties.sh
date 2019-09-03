#!/bin/bash

if (( $# != 9 )); then
	echo  "This script creates a '.properties' file for the eva-accession-release pipeline, taking the properties from the import, and suggesting commands to run the whole release process for an assembly.
This script needs the next parameters in order:
- The path to the import properties file with information about the assembly to release
- The root output folder for the release, where this script will create a subfolder for the assembly
- The path to the eva-accession-release JAR file
- The path to the bgzip executable
- The path to the tabix executable
- The path to the script count_ids_in_vcf.sh
- The path to the vcf-validator executable
- The path to the vcf-assembly-checker executable
- The path to the sort_vcf script
"

else
	IMPORT_FILE=$1
	OUTPUT_RELEASE_FOLDER=$2
	EVA_ACCESSION_JAR_PATH=$3
	BGZIP_PATH=$4
	TABIX_PATH=$5
	COUNT_IDS_PATH=$6
	VCF_VALIDATOR_PATH=$7
	VCF_ASM_CHECKER_PATH=$8
	SORT_SCRIPT_PATH=$9

	if [ ! -f ${IMPORT_FILE} ]; then
		echo "The properties file ${IMPORT_FILE} doesn't exist! Stopping script"
		exit 1
	fi

	OUTPUT_FILE=application.properties
	if [ -f ${OUTPUT_FILE} ]; then
		mv ${OUTPUT_FILE} ${OUTPUT_FILE}.bk
		echo "# Made a backup of \"${OUTPUT_FILE}\" into \"${OUTPUT_FILE}.bk\"";
		if [ ${OUTPUT_FILE} = ${IMPORT_FILE} ]; then
			IMPORT_FILE=${IMPORT_FILE}.bk
		fi
	fi

	# extract values from the existing properties file, trimming surrounding spaces
	assemblyAccession=`grep -m 1 "^parameters.assemblyAccession" $IMPORT_FILE | sed "s/^[^=]\+= *\([^ ]*\) */\1/"`
	fasta=`grep -m 1 "^parameters.fasta" $IMPORT_FILE | sed "s/^[^=]\+= *\([^ ]*\) */\1/"`
	assembly_report=`grep -m 1 "^parameters.assemblyReport" $IMPORT_FILE | sed "s/^[^=]\+= *\([^ ]*\) */\1/"`

	# extract whole lines
	db_url=`grep -m 1 "^spring.datasource.url" $IMPORT_FILE`
	db_user=`grep -m 1 "^spring.datasource.username" $IMPORT_FILE`
	db_pass=`grep -m 1 "^spring.datasource.password" $IMPORT_FILE`
	mongo_host=`grep -m 1 "^spring.data.mongodb.host" $IMPORT_FILE`
	mongo_port=`grep -m 1 "^spring.data.mongodb.port" $IMPORT_FILE`
	mongo_db=`grep -m 1 "^spring.data.mongodb.database" $IMPORT_FILE`
	mongo_user=`grep -m 1 "^spring.data.mongodb.username" $IMPORT_FILE`
	mongo_pass=`grep -m 1 "^spring.data.mongodb.password" $IMPORT_FILE`
	mongo_authdb=`grep -m 1 "^spring.data.mongodb.authentication-database" $IMPORT_FILE`

	OUTPUT_FOLDER=${OUTPUT_RELEASE_FOLDER}/${assemblyAccession}/

	echo ""
	echo "
spring.batch.job.names=ACCESSION_RELEASE_JOB

parameters.assemblyAccession=${assemblyAccession}
parameters.assemblyReport=${assembly_report}
parameters.chunkSize=1000
parameters.fasta=${fasta}
parameters.forceRestart=false
parameters.outputFolder=${OUTPUT_FOLDER}

# job repository datasource
spring.datasource.driver-class-name=org.postgresql.Driver
${db_url}
${db_user}
${db_pass}
spring.datasource.tomcat.max-active=3

# Only to set up the database!
# spring.jpa.generate-ddl=true

# MongoDB for storing imported accessions
${mongo_host}
${mongo_port}
${mongo_db}
${mongo_user}
${mongo_pass}
${mongo_authdb}
mongodb.read-preference=primaryPreferred

spring.main.web-environment=false

logging.level.uk.ac.ebi.eva.accession.release=INFO
" > ${OUTPUT_FILE}

	echo "# you will likely want to run these jobs (CHECK THAT THE VERSION OF THE JAR FILE IS UPDATED!! https://github.com/EBIvariation/eva-accession/tags):"
	echo "
mkdir -p ${OUTPUT_FOLDER}
jobname=\`date +\"%Y%m%d_%H%M%S\"\`_${assemblyAccession}
bsub -J \${jobname} -o release.log -e release.err -M 8192 -R \"rusage[mem=8192]\" \"java -Xmx7g -jar ${EVA_ACCESSION_JAR_PATH}; mv ${OUTPUT_FOLDER}${assemblyAccession}_current_ids.vcf ${OUTPUT_FOLDER}${assemblyAccession}_current_ids_unsorted.vcf; mv ${OUTPUT_FOLDER}${assemblyAccession}_merged_ids.vcf ${OUTPUT_FOLDER}${assemblyAccession}_merged_ids_unsorted.vcf\"
bsub -J \${jobname}_txts -w \${jobname} -o txts.log -e txts.err -M 8192 -R \"rusage[mem=8192]\" \"sort -V ${OUTPUT_FOLDER}${assemblyAccession}_deprecated_ids.unsorted.txt | uniq | gzip > ${OUTPUT_FOLDER}${assemblyAccession}_deprecated_ids.txt.gz; sort -V ${OUTPUT_FOLDER}${assemblyAccession}_merged_deprecated_ids.unsorted.txt | uniq | gzip > ${OUTPUT_FOLDER}${assemblyAccession}_merged_deprecated_ids.txt.gz\"
bsub -J \${jobname}_current_vcf -w \${jobname} -o ${OUTPUT_FOLDER}current_vcf.log -e ${OUTPUT_FOLDER}current_vcf.err \"${SORT_SCRIPT_PATH} ${OUTPUT_FOLDER}${assemblyAccession}_current_ids_unsorted.vcf ${OUTPUT_FOLDER}${assemblyAccession}_current_ids.vcf; ${VCF_VALIDATOR_PATH} -i ${OUTPUT_FOLDER}${assemblyAccession}_current_ids.vcf -o ${OUTPUT_FOLDER}; ${VCF_ASM_CHECKER_PATH} -i ${OUTPUT_FOLDER}${assemblyAccession}_current_ids.vcf -f ${fasta} -a ${assembly_report} -o ${OUTPUT_FOLDER} -r text,summary ; ${BGZIP_PATH} ${OUTPUT_FOLDER}${assemblyAccession}_current_ids.vcf\"
bsub -J \${jobname}_merged_vcf -w \${jobname} -o ${OUTPUT_FOLDER}merged_vcf.log -e ${OUTPUT_FOLDER}merged_vcf.err \"${SORT_SCRIPT_PATH} ${OUTPUT_FOLDER}${assemblyAccession}_merged_ids_unsorted.vcf ${OUTPUT_FOLDER}${assemblyAccession}_merged_ids.vcf; ${VCF_VALIDATOR_PATH} -i ${OUTPUT_FOLDER}${assemblyAccession}_merged_ids.vcf -o ${OUTPUT_FOLDER}; ${VCF_ASM_CHECKER_PATH} -i ${OUTPUT_FOLDER}${assemblyAccession}_merged_ids.vcf -f ${fasta} -a ${assembly_report} -o ${OUTPUT_FOLDER} -r text,summary ; ${BGZIP_PATH} ${OUTPUT_FOLDER}${assemblyAccession}_merged_ids.vcf\"
bsub -J \${jobname}_tabix -w \"(\${jobname}_current_vcf && \${jobname}_merged_vcf)\" -o tabix.log -e tabix.err \"${TABIX_PATH} ${OUTPUT_FOLDER}${assemblyAccession}_current_ids.vcf.gz; ${TABIX_PATH}  ${OUTPUT_FOLDER}${assemblyAccession}_merged_ids.vcf.gz\"
bsub -J \${jobname}_count -w \"(\${jobname}_txts && \${jobname}_current_vcf && \${jobname}_merged_vcf)\" -o count_ids.log -e count_ids.err \"echo '# Unique RS ID counts' > ${OUTPUT_FOLDER}README_rs_ids_counts.txt; ${COUNT_IDS_PATH} ${OUTPUT_FOLDER}${assemblyAccession}_current_ids.vcf.gz >> ${OUTPUT_FOLDER}README_rs_ids_counts.txt; ${COUNT_IDS_PATH} ${OUTPUT_FOLDER}${assemblyAccession}_merged_ids.vcf.gz >> ${OUTPUT_FOLDER}README_rs_ids_counts.txt; zcat ${OUTPUT_FOLDER}${assemblyAccession}_deprecated_ids.txt.gz | wc -l | sed 's/^/'${assemblyAccession}_deprecated_ids.txt.gz'\t/' >> ${OUTPUT_FOLDER}README_rs_ids_counts.txt; zcat ${OUTPUT_FOLDER}${assemblyAccession}_merged_deprecated_ids.txt.gz | cut -f 1 | uniq | wc -l | sed 's/^/'${assemblyAccession}_merged_deprecated_ids.txt.gz'\t/' >> ${OUTPUT_FOLDER}README_rs_ids_counts.txt\"
" | tee release_commands_${assemblyAccession}.txt

fi


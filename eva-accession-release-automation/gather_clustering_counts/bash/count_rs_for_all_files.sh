#!/bin/bash

set -e

FILE_WITH_ALL_INPUTS=$1
OUTPUT_FILE=$2

#Initialise list of tmp output
ALL_TMP_OUTPUT=""

# Uses all input as file to process
for INPUT in `cat $FILE_WITH_ALL_INPUTS`
do
    echo "Process $INPUT"
    ASSEMBLY=$(basename $(dirname ${INPUT}));
    SC_NAME=$(basename $(dirname $(dirname ${INPUT})));
    # SPLIT by GCA is to support format that have a taxonomy prefix (release 5+) or the one that do not (release 4-)
    TYPE=$(echo $(basename ${INPUT}) | awk -F 'GCA' '{print $2}' |cut -f 3- -d '_' | awk '{print substr($0,1,length($0)-11)}')
    OUTPUT=tmp_${SC_NAME}_${ASSEMBLY}_${TYPE}.txt
    if [[ ${INPUT} == *.vcf.gz ]]
    then
        # There are sometime multiple rs (separated by ;) in one line that needs to be split across multiple lines
        zcat  "${INPUT}" | grep -v '^#' | awk -v annotation="${ASSEMBLY}-${SC_NAME}-${TYPE}" '{gsub(";","\n",$3); print $3" "annotation}' > ${OUTPUT}
    elif [[ ${INPUT} == *_unmapped_ids.txt.gz ]]
    then
        SC_NAME=$(basename $(dirname ${INPUT}));
        OUTPUT=tmp_${SC_NAME}_unmapped.txt
        zcat  "${INPUT}" | grep -v '^#' | awk -v annotation="Unmapped-${SC_NAME}-unmapped" '{print $1" "annotation}' > ${OUTPUT}
    else
        zcat  "${INPUT}" | grep -v '^#' | awk -v annotation="${ASSEMBLY}-${SC_NAME}-${TYPE}" '{print $1" "annotation}' > ${OUTPUT}
    fi
    ALL_TMP_OUTPUT=$OUTPUT" "$ALL_TMP_OUTPUT
done

echo "Concatenate all TMP files"

cat $ALL_TMP_OUTPUT | sort  \
    | awk '{if (current_rsid != $1){
             for (a in annotation){printf "%s,",a};
             print "";
             delete annotation; current_rsid=$1
            }; annotation[$2]=1; }
            END{for (a in annotation){printf "%s,",a};
            print ""; }' \
    | grep -v '^$' | sort | uniq -c | sort -nr > "$OUTPUT_FILE"

rm -f $ALL_TMP_OUTPUT


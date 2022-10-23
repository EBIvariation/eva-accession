#!/bin/bash
set -e

species_dir=$1
species_name=$(basename "$species_dir")

zcat  "${species_dir}"/*_unmapped_ids.txt.gz  | grep -v '^#' | sort -u | wc -l > "${species_name}"_count_unmapped_rsid.log

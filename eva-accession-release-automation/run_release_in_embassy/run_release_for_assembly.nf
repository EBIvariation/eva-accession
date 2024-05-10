#!/usr/bin/env nextflow

nextflow.enable.dsl=2

workflow {
    initiate_release_status_for_assembly('initiate') | copy_accessioning_collections_to_embassy | run_release_for_assembly | \
    merge_dbsnp_eva_release_files | sort_bgzip_index_release_files | validate_release_vcf_files | \
    analyze_vcf_validation_results | count_rs_ids_in_release_files | validate_rs_release_files | \
    update_sequence_names_to_ena | update_release_status_for_assembly
}

process initiate_release_status_for_assembly {

    label 'short_time', 'med_mem'

    input:
    val flag

    output:
    val true, emit: flag

    script:
    """
    export PYTHONPATH=$params.python_path
    $params.executable.python.interpreter -m run_release_in_embassy.initiate_release_status_for_assembly --private-config-xml-file $params.maven.settings_file --profile $params.maven.environment --release-species-inventory-table eva_progress_tracker.clustering_release_tracker --taxonomy-id $params.taxonomy --assembly-accession $params.assembly --release-version $params.release_version 1>> $params.log_file 2>&1
    """
}

process copy_accessioning_collections_to_embassy {

    label 'long_time', 'med_mem'

    input:
    val flag

    output:
    val true, emit: flag

    script:
    """
    export PYTHONPATH=$params.python_path
    $params.executable.python.interpreter -m run_release_in_embassy.copy_accessioning_collections_to_embassy --private-config-xml-file $params.maven.settings_file --profile $params.maven.environment --taxonomy-id $params.taxonomy --assembly-accession $params.assembly --release-species-inventory-table eva_progress_tracker.clustering_release_tracker --release-version $params.release_version --dump-dir $params.dump_dir 1>> $params.log_file 2>&1
    """
}

process run_release_for_assembly {

    label 'long_time', 'med_mem'

    input:
    val flag

    output:
    val true, emit: flag

    script:
    """
    export PYTHONPATH=$params.python_path
    $params.executable.python.interpreter -m run_release_in_embassy.run_release_for_assembly --private-config-xml-file $params.maven.settings_file --profile $params.maven.environment --taxonomy-id $params.taxonomy --assembly-accession $params.assembly --release-species-inventory-table eva_progress_tracker.clustering_release_tracker --release-version $params.release_version --species-release-folder $params.assembly_folder --release-jar-path $params.jar.release_pipeline 1>> $params.log_file 2>&1
    """
}

process merge_dbsnp_eva_release_files {

    label 'long_time', 'med_mem'

    input:
    val flag

    output:
    val true, emit: flag

    script:
    """
    export PYTHONPATH=$params.python_path
    $params.executable.python.interpreter -m run_release_in_embassy.merge_dbsnp_eva_release_files --private-config-xml-file $params.maven.settings_file --profile $params.maven.environment --bgzip-path $params.executable.bgzip --bcftools-path $params.executable.bcftools --vcf-sort-script-path $params.executable.sort_vcf_sorted_chromosomes --taxonomy-id $params.taxonomy --assembly-accession $params.assembly --release-species-inventory-table eva_progress_tracker.clustering_release_tracker --release-version $params.release_version --species-release-folder $params.assembly_folder 1>> $params.log_file 2>&1
    """
}

process sort_bgzip_index_release_files {

    label 'long_time', 'med_mem'

    input:
    val flag

    output:
    val true, emit: flag

    script:
    """
    export PYTHONPATH=$params.python_path
    $params.executable.python.interpreter -m run_release_in_embassy.sort_bgzip_index_release_files --bgzip-path $params.executable.bgzip --bcftools-path $params.executable.bcftools --vcf-sort-script-path $params.executable.sort_vcf_sorted_chromosomes --taxonomy-id $params.taxonomy --assembly-accession $params.assembly --species-release-folder $params.assembly_folder 1>> $params.log_file 2>&1
    """
}

process validate_release_vcf_files {

    label 'long_time', 'med_mem'

    input:
    val flag

    output:
    val true, emit: flag

    script:
    """
    export PYTHONPATH=$params.python_path
    $params.executable.python.interpreter -m run_release_in_embassy.validate_release_vcf_files --private-config-xml-file $params.maven.settings_file --profile $params.maven.environment --taxonomy-id $params.taxonomy --assembly-accession $params.assembly --release-species-inventory-table eva_progress_tracker.clustering_release_tracker --release-version $params.release_version --species-release-folder $params.assembly_folder --vcf-validator-path $params.executable.vcf_validator --assembly-checker-path $params.executable.vcf_assembly_checker 1>> $params.log_file 2>&1
    """
}

process analyze_vcf_validation_results {

    label 'long_time', 'med_mem'

    input:
    val flag

    output:
    val true, emit: flag

    script:
    """
    export PYTHONPATH=$params.python_path
    $params.executable.python.interpreter -m run_release_in_embassy.analyze_vcf_validation_results --species-release-folder $params.assembly_folder --assembly-accession $params.assembly 1>> $params.log_file 2>&1
    """
}

process count_rs_ids_in_release_files {

    label 'long_time', 'med_mem'

    input:
    val flag

    output:
    val true, emit: flag

    script:
    """
    export PYTHONPATH=$params.python_path
    $params.executable.python.interpreter -m run_release_in_embassy.count_rs_ids_in_release_files --count-ids-script-path $params.executable.count_ids_in_vcf --taxonomy-id $params.taxonomy --assembly-accession $params.assembly --species-release-folder $params.assembly_folder 1>> $params.log_file 2>&1
    """
}

process validate_rs_release_files {

    label 'long_time', 'med_mem'

    input:
    val flag

    output:
    val true, emit: flag

    script:
    """
    export PYTHONPATH=$params.python_path
    $params.executable.python.interpreter -m run_release_in_embassy.validate_rs_release_files --private-config-xml-file $params.maven.settings_file --profile $params.maven.environment --taxonomy-id $params.taxonomy --assembly-accession $params.assembly --release-species-inventory-table eva_progress_tracker.clustering_release_tracker --release-version $params.release_version --species-release-folder $params.assembly_folder 1>> $params.log_file 2>&1
    """
}

process update_sequence_names_to_ena {

    label 'long_time', 'med_mem'

    input:
    val flag

    output:
    val true, emit: flag

    script:
    """
    export PYTHONPATH=$params.python_path
    $params.executable.python.interpreter -m run_release_in_embassy.update_sequence_names_to_ena --taxonomy-id $params.taxonomy --assembly-accession $params.assembly --species-release-folder $params.assembly_folder --sequence-name-converter-path $params.executable.convert_vcf_file --bcftools-path $params.executable.bcftools 1>> $params.log_file 2>&1
    """
}

process update_release_status_for_assembly {

    label 'short_time', 'med_mem'

    input:
    val flag

    output:
    val true, emit: flag11

    script:
    """
    export PYTHONPATH=$params.python_path
    $params.executable.python.interpreter -m run_release_in_embassy.update_release_status_for_assembly --private-config-xml-file $params.maven.settings_file --profile $params.maven.environment --release-species-inventory-table eva_progress_tracker.clustering_release_tracker --taxonomy-id $params.taxonomy --assembly-accession $params.assembly --release-version $params.release_version 1>> $params.log_file 2>&1
    """
}

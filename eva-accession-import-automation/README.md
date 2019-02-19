# Pre-requisites
* Install the **pandas** and **psycopg2** modules in your local python environment
    ```bash
    pip install -r requirements.txt
    ```
# Usage
## Run the automation script
* Example
    ```bash
    python run_accession_import_jobs.py -s tomato_4081 --scientific-name solanum_lycopersicum -a 139,SL2.40,GCA_000188115.1 148,SL2.50,GCA_000188115.2 -p "/path/to/private-dev-config.json"
    ```
    * Private configuration file example
    ```json
    {
      "env" : "dev",
      "eva_root_dir" : "/dir/for/eva",
      "metadb": "<metadb>",
      "metauser": "<metauser>",
      "metahost": "<metahost>",
      "dbsnp_user" : "<dbsnp_user>",
      "dbsnp_port" : 5432,
      "job_tracker_db" : "<job_tracker_db>",  
      "job_tracker_host" : "<job_tracker_host>",  
      "job_tracker_port" : 5432,
      "job_tracker_user" : "<job_tracker_user>",  
      "mongo_acc_db" : "<mongo_acc_db>",
      "mongo_auth_db" : "<mongo_auth_db>",
      "mongo_user" : "<mongo_user>",
      "mongo_password" : "<mongo_password>",
      "mongo_host" : "<mongo_host>",
      "mongo_port" : 27017,
      "accession_import_jar" : "/path/to/eva-accession-import.jar",
      "python3_path" : "/path/to/python3",
      "validation_script_path": "/path/to/validation/scripts",
      "genbank_equivalents_file" : "/path/to/genbank_equivalents_file"
    }
    ```
## Generate custom assembly report
* Get help
    ```bash 
    python generate_custom_assembly_report.py -h
    ```
* Example
    ```bash
    python generate_custom_assembly_report.py -d metadata_db_name -u metadata_db_user -h metadata_db_host -s bony_fish_7950 -a GCF_000966335.1 -g "/path/to/identical_genbank_refseq_4snp_assembly_report.txt"
    ```
* Running tests
    ```bash
    python -m test_generate_custom_assembly_report -v
    ```

## Generate properties file
* Get help
    ```bash
    python generate_properties.py --help
    ```
* Example
    ```bash
    python generate_properties.py -b 150 -a GCA_000001735.1 -r GCF_000001735.3_TAIR10_assembly_report_CUSTOM.txt  -f /path/to/fasta.fa  -d meadata_db_name -u metadata_db_user -h metadata_db_host   -H job_tracker_host -D job_tracker_db  --mongo-acc-db mongo_accessioning_db --mongo-auth-db mongo_auth_db --mongo-user mongo_user --mongo-password mongo_password  --mongo-host mongo_host --mongo-port mongo_port -s arabidopsis_3702
    ```

# Pre-requisites
* Install the **pandas** and **psycopg2** modules in your local python environment
    ```bash
    pip install -r requirements
    ```
# Usage
## Generate custom assembly report
* Get help
    ```bash 
    python generate_custom_assembly_report.py -h
    ```
* Example
    ```bash
    python generate_custom_assembly_report.py -d metadata_db_name -u metadata_db_user -h metadata_db_host -s bony_fish_7950 -a GCF_000966335.1 -g "/path/to/identical_genbank_refseq_4snp_assembly_report.txt"
    ```
    ## Running tests
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

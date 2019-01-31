# Pre-requisites
* Install the **pandas** module in your local python environment
    ```bash
    pip install pandas --user
    ```
# Usage
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

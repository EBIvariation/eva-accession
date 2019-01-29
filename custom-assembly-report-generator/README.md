#Usage:
* Get help
    ```bash 
    python generate_custom_assembly_report.py -h
    ```
* Example
    ```bash
    python generate_custom_assembly_report.py -d metadata_db -u metadata_user -h metadata_pghost -s bony_fish_7950 -a GCF_000966335.1 -g "/home/dir/identical_genbank_refseq_4snp_assembly_report.txt"
    ```
* Running tests
    ```bash
    python -m test_generate_custom_assembly_report -v
    ```

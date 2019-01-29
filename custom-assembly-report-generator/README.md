#Pre-requisites:
* Install the **pandas** module in your local python environment
    ```bash
    pip install pandas
    ```
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
    * Tests require private credential files (which are not part of this repository)
        * Sample credential file format (ex: **test-config-bony-fish-7950.json**)
        ```json
        {
          "metadb" : "metadb",
          "metauser" : "metauser",
          "metahost" : "metahost",
          "species" : "bony_fish_7950",
          "assembly_accession" : "GCF_000966335.1",
          "genbank_equivalents_file" : "identical_genbank_refseq_4snp_assembly_report.txt",
          "PGPASSFILE" : "/home/dir/.pgpass"
        }
        ```

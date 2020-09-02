# Pre-requisites
* Install the **ebi_eva_common_pyutils** module in your local python environment
    ```bash
    pip install -r requirements.txt
    ```

# Usage
## Cluster multiple assemblies
The clustering automation script have the following parameters:
* **source\*:** The possible sources are Mongo or VCF
* **asm-vcf-prj-list:** Is a list of one or many assembly#vcf#project combinations. This is required is the source is VCF
* **assembly-list:** Is a list of assemblies to process. This is required if the source is Mongo
* **private-config-file\*:** Private configuration file with sensitive information like database connections and internal paths
* **output-directory:** Directory where the generated files will be stored
* **clustering-artifact:** Clustering artifact path is the latest version of the clustering pipeline
* **only-printing:** Is a flag to only get the commands but not run them  

##Examples
* Example using Mongo as source
    ```bash
    python3 ../projects/ebi/eva-accession/eva-accession-clustering-automation/cluster_multiple_assemblies.py --source mongo --assembly-list GCA_000233375.4,GCA_000233375.4 --private-config-file config.json --output-directory /home/asilva/Documents/clustering_automation --only-printing    
    ```

* Example using VCF as source
    ```bash
    python3 ../eva-accession/eva-accession-clustering-automation/cluster_multiple_assemblies.py --source vcf --asm-vcf-prj-list GCA_000233375.4#/nfs/eva/accessioned.vcf.gz#PRJEB1111,GCA_000233375.4#/nfs/eva/file.vcf.gz#PRJEB2222 --private-config-file config.json --output-directory /home/clustering_automation --only-printing
    ```

##Notes
* The **clustering_artifact** can be passed either with the parameter or it can be included in the private configuration file.
If the parameter is specified, the clustering artifact in the configuration will not be taken into account.
* **Private configuration_file** example

    ```json
    {
      "job_tracker_db" : "<job_tracker_db>",  
      "job_tracker_host" : "<job_tracker_host>",  
      "job_tracker_port" : 5432,
      "job_tracker_user" : "<job_tracker_user>",
      "job_tracker_password" : "<job_tracker_password>",
      "mongo_acc_db" : "<mongo_acc_db>",
      "mongo_auth_db" : "<mongo_auth_db>",
      "mongo_user" : "<mongo_user>",
      "mongo_password" : "<mongo_password>",
      "mongo_host" : "<mongo_host>",
      "mongo_port" : 27017,
      "clustering_artifact" : "/path/to/eva-accession-clustering.jar",
      "python3_path" : "/path/to/python3"
    }
    ```
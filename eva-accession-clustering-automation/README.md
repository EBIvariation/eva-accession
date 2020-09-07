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
* **private-config-file\*:** Private configuration file with sensitive information like internal paths or tokens
* **private-config-xml-file\*:** Maven settings.xml file with the profiles that hold database connection data
* **profile\*:** Profile to run the pipeline. e.g. production
* **output-directory:** Directory where the generated files will be stored
* **clustering-artifact:** Clustering artifact path is the latest version of the clustering pipeline
* **only-printing:** Is a flag to only get the commands but not run them  


## Examples
* Example using Mongo as source
    ```bash
    python3 path/to/eva-accession/eva-accession-clustering-automation/cluster_multiple_assemblies.py --source mongo --assembly-list GCA_000233375.4,GCA_000002285.2 --private-config-file path/to/private_config.json --output-directory /output/clustering_automation --only-printing --private-config-xml-file path/to/settings.xml --profile production    
    ```

* Example using VCF as source
    ```bash
    python3 path/to/eva-accession/eva-accession-clustering-automation/cluster_multiple_assemblies.py --source vcf --asm-vcf-prj-list GCA_000233375.4#path/to/accessioned.vcf.gz#PRJEB1111,GCA_000233375.4#/nfs/eva/file.vcf.gz#PRJEB2222 --private-config-file path/to/private_config.json --output-directory /output/clustering_automation --only-printing --private-config-xml-file path/to/settings.xml --profile productio
    ```
  

## Notes
* The **clustering_artifact** can be passed either with the parameter or it can be included in the private configuration file.
If the parameter is specified, the clustering artifact in the configuration will not be taken into account.
* The settings xml file can be passed using the parameter or if the eva settings in github should be used include the 
token in the json/yml config file. The param is prioritized if it is passed in the command.
* **Private configuration file** example
    ```json
    {
      "clustering_artifact" : "/path/to/eva-accession-clustering.jar",
      "github_token" : "token",
      "python3_path" : "/path/to/python3"
    }
    ```
# Pre-requisites
* Install the **ebi_eva_common_pyutils** module in your local python environment
    ```bash
    pip3 install -r requirements.txt
    ```

# Usage
## Cluster multiple assemblies
The clustering automation script have the following parameters:
* **source\*:** The possible sources are Mongo or VCF
* **asm-vcf-prj-list:** Is a list of one or many assembly#vcf#project combinations. This is required if the source is VCF
* **assembly-list:** Is a list of assemblies to process. This is required if the source is Mongo
* **private-config-xml-file\*:** Maven settings.xml file with the profiles that hold database connection data
* **profile\*:** Profile to run the pipeline. e.g. production
* **output-directory:** Directory where the generated files will be stored
* **clustering-artifact\*:** Clustering artifact path is the latest version of the clustering pipeline
* **only-printing:** Is a flag to only get the commands but not run them
* **memory:** Amount of memory to use when running the clustering jobs


## Examples
* Example using Mongo as source
    ```bash
    python3 path/to/eva-accession/eva-accession-clustering-automation/cluster_multiple_assemblies.py --source MONGO --assembly-list GCA_000233375.4 GCA_000002285.2 --output-directory /output/clustering_automation --only-printing --clustering-artifact cluster.jar --profile production --private-config-xml-file /configuration/eva-maven-settings.xml     
    ```

* Example using VCF as source
    ```bash
    python3 path/to/eva-accession/eva-accession-clustering-automation/cluster_multiple_assemblies.py --source VCF --asm-vcf-prj-list GCA_000233375.4#/nfs/eva/accessioned.vcf.gz#PRJEB1111 GCA_000002285.2#/nfs/eva/file.vcf.gz#PRJEB2222 --output-directory /output/clustering_automation --only-printing --clustering-artifact cluster.jar --profile production --private-config-xml-file /configuration/eva-maven-settings.xml 
    ```
  

## Notes
* The **settings xml file** should be passed using the parameter --private-config-xml-file. If it is being run from the
 EBI cluster deploy the configuration repository and point to the eva settings xml file.
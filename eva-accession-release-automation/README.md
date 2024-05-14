# Pre-requisites
Install release automation python module and dependencies:
```bash
pip install -e /path/to/eva-accession/eva-accession-release-automation
```


# Usage
The release automation script `run_release_for_species.py` has the following parameters:
* **common-release-properties-file:** Path to yaml config file, see below
* **taxonomy-id:** Taxonomy to release

You also need to set `PYTHONPATH=/path/to/eva-accession/eva-accession-release-automation/run_release_in_embassy`.


## Config template
```yaml
private-config-xml-file:
profile:

release-version:
release-folder:
public-ftp-release-base-folder:
release-species-inventory-table:

release-jar-path:

bgzip-path:
bcftools-path:
vcf-sort-script-path:
vcf-validator-path:
assembly-checker-path:
count-ids-script-path:
python3-path:

nextflow-binary-path:
nextflow-config-path:
```

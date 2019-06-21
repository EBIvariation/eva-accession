# EVA accession pipeline

This module can be used to assign SubSNP (SS) accessions to a set of variants in a VCF file.

## Compiling

The README in the root of the project contains basic instructions.

## Running

You will need to provide several parameters to run this pipeline. The easiest way is to fill an `application.properties` and provide it as a CLI parameter:

```
java -jar eva-accession-pipeline-0.3.0-SNAPSHOT-c87679a.jar --spring.config.name=application.properties`
```
Note: If a file named `application.properties` is present, Spring will use it automatically even without specifying it in the `spring.config.name` parameter.

### Parameters

Empty template of an `application.properties`:

```
spring.batch.job.names=CREATE_SUBSNP_ACCESSION_JOB

accessioning.instanceId=
accessioning.submitted.categoryId=ss

accessioning.monotonic.ss.blockSize=100000
accessioning.monotonic.ss.blockStartValue=5000000000
accessioning.monotonic.ss.nextBlockInterval=1000000000

parameters.assemblyAccession=
parameters.taxonomyAccession=
parameters.projectAccession=

parameters.vcf=
parameters.vcfAggregation=
parameters.fasta=
parameters.assemblyReportUrl=
parameters.outputVcf=

parameters.chunkSize=
parameters.forceRestart=
parameters.contigNaming=SEQUENCE_NAME

spring.data.mongodb.database=
mongodb.read-preference=|eva.mongo.read-preference|

spring.datasource.driver-class-name=org.postgresql.Driver
spring.datasource.url=
spring.datasource.username=
spring.datasource.password=
spring.datasource.tomcat.max-active=3

# Only to set up the database!
# spring.jpa.generate-ddl=true

spring.main.web-environment=false
```

### `parameters.contigNaming`

This parameter allows selecting which contig/chromosome naming should be used in the accessioned report (the file `parameters.outputVcf`). The mapping is achieved using NCBI's assembly reports.

Available values (from ContigNaming):
- `SEQUENCE_NAME`: Use chromosome or scaffold names.
- `ASSIGNED_MOLECULE`: Usually similar to SEQUENCE_NAME but without prefixes.
- `INSDC`: Use GenBank contig accessions.
- `REFSEQ`
- `UCSC`

If not provided, the default value is `SEQUENCE_NAME`.
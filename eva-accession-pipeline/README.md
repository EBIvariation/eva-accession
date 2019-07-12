# EVA accession pipeline

This module can be used to assign SubSNP (SS) accessions to a set of variants from a VCF file.

The program will reserve the accessions in a database (`spring.datasource` parameters), store the accessioned variants in another database (`spring.data.mongodb` parameters), and write a VCF report with the accessioned variants.

## Build

As the README in the root of the project explains, a basic `mvn clean install` should work.

## Run

You will need to provide several parameters to run this pipeline. The easiest way to specify them is filling an `application.properties` and provide it as a CLI parameter:

```
java -jar eva-accession-pipeline-x.y.z.jar --spring.config.name=application.properties
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
parameters.contigNaming=NO_REPLACEMENT

spring.data.mongodb.database=
spring.data.mongodb.host=
spring.data.mongodb.port=
spring.data.mongodb.username=
spring.data.mongodb.password=
spring.data.mongodb.authentication-database=
mongodb.read-preference=

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
- `REFSEQ`: Use RefSeq contig accessions.
- `UCSC`: Use chromosome names defined by UCSC, usually of the form `chr<name>`.
- `NO_REPLACEMENT`: Do not use any particular naming, just keep whatever contig is provided.

If not provided, the default value is `NO_REPLACEMENT`.

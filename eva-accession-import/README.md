# EVA accession import

This module can be used to import accessions (SS and RS) from a dbsnp mirror into the accessioning service at EVA. In https://github.com/EBIvariation/eva-accession/wiki there's some documentation about the transformations done.

The program can be run with a plain `java -jar eva-accession-import-0.2.0-SNAPSHOT.jar` from a directory with an `application.properties` such as the one under `src/main/resources`.

## Parameters

Tips to fill some parameters:

### `parameters.assemblyAccession`

Look in the dbsnp mirror, in the contiginfo table, the columns "asm_acc" and "asm_version".

### `parameters.assemblyName`

Look in the dbsnp mirror, in the contiginfo table, the column "group_label".

### `parameters.assemblyReportUrl`

Use URLs such as `ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/001/735/GCF_000001735.3_TAIR10/GCF_000001735.3_TAIR10_assembly_report.txt`, using the assembly accession (often the GCF one) that you used in `parameters.assemblyAccession`. You can use the equivalent GCA version in most cases, but check if there's any difference between them.

### `dbsnp.datasource.url`

Use URLs in the format `jdbc:postgresql://`yourhost`:`yourport`/`yourdatabase`?currentSchema=`yourschema

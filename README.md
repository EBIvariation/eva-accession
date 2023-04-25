 eva-accession [![Build Status](https://travis-ci.com/EBIvariation/eva-accession.svg?branch=master)](https://travis-ci.com/EBIvariation/eva-accession)
EVA non-human variant accessioning service.

The EVA is responsible for issuing RS IDs and SS IDs for variants in non-human species. The official announcement is available at https://www.ebi.ac.uk/about/news/press-releases/eva-issues-long-term-ids-non-human-variants.

# Build
To compile the whole project you only need to run `mvn clean install`.

You can use maven profiles (e.g. `mvn clean install -Pproduction`) to fill some required parameters and include them in the compiled jars/wars.

For a quick compilation without tests, you can run `mvn clean install -DskipTests`.

Look at the READMEs of each module for more details.
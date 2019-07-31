EVA RefSNP RELEASE 2019-07 NOTES
================================

1. DEFINITIONS
--------------

1.1. Accession

An accession is a unique and permanent identifier for an entity that enables
cross resource references to submitted objects.

1.2. Variant locus accessions

SS or SubSnp represent an allele change in a genomic study. They are identified
by the reference sequence, project/study, contig, start position, reference
allele and alternate allele.

RS or RefSnp is a cluster of SS IDs. They are identified by reference sequence,
contig, start position and variant class.

1.3. Assembly accessions

An assembly accession starting with the three letters prefix GCA correspond to
INSDC assemblies. The prefix is followed by an underscore, 9 digits and a
version is added at the end. For example, the assembly accession for the INSDC
version of the public horse reference assembly (EquCab2.0) is GCA_000002305.1.


2. EVA RefSNP RELEASE
---------------------

The EVA RS release is a collection of files organized by species that list all
the RS IDs for variants stored in the European Variation Archive (EVA,
https://www.ebi.ac.uk/eva/). The species folders naming is listed in the file
species_name_mapping.tsv in the top folder.

Given a species (e.g. horse, GCA_000002305.1), the following set of files will
be present in the RS release:

- Current IDs (e.g. GCA_000002305.1_current_ids.vcf.gz)
- Unmapped IDs (e.g. horse_9796_unmapped_ids.txt.gz)
- Merged IDs (e.g. GCA_000002305.1_merged_ids.vcf.gz)
- Deprecated IDs (e.g. GCA_000002305.1_deprecated_ids.txt.gz)
- Merged then deprecated IDs (e.g. GCA_000002305.1_merged_deprecated_ids.txt.gz)

The VCF files (containing current and merged variants) of this release can include the same
RS ID in multiple lines because each line will report one alternate allele. Alleles are
associated with SS IDs and one RS ID will group more than one SS ID for the following reasons:

- By definition an RS ID is a cluster of SS IDs.
ss1(A>C) -> rs1
ss2(A>T) -> rs1

- Multiallelic variants under the same SS IDs are linked to the same RS ID.
ss1(A>C) -> rs1
ss1(A>T) -> rs1

- RS IDs can be mapped to multiple start positions in the same chromosome.
ss1(A>C chr:1, start:1000) -> rs1 (chr:1, start:1000)
ss2(A>T chr:1, start:2000) -> rs1 (chr:1, start:2000)

- RS IDs can be mapped to multiple chromosomes
ss1(A>C chr:1, start:1000) -> rs1 (chr:1, start:1000)
ss3(A>T chr:2, start:2000) -> rs1 (chr:2, start:2000)


2.1. Current IDs (e.g. GCA_000002305.1_current_ids.vcf.gz)

Contains the RS IDs which can be browsed from the EVA website
(https://www.ebi.ac.uk/eva/) and web services
(https://www.ebi.ac.uk/eva/webservices/identifiers/swagger-ui.html).

The file is in VCF format and some extra information is available in the INFO
column, using the next keys:

- ALMM: Alleles mismatch flag, present when some of the submitted variants have
  inconsistent allele information. See
  https://github.com/EBIvariation/eva-accession/wiki/Import-accessions-from-dbSNP#alleles-match

- ASMM: Assembly mismatch flag, present when the reference allele doesn't match
  the reference sequence.

- LOE: Lack of evidence flag, present if no submitted variant includes genotype
  or frequency information.

- RS_VALIDATED: RS validated flag, present when the RS was validated by any
  method as indicated by the dbSNP validation status.

- SID: Identifiers of studies that report a variant.

- SS_VALIDATED: Number of submitted variants clustered in an RS that were
  validated by any method as indicated by the dbSNP validation status.

- VC: Variant class according to the Sequence Ontology.


2.2. Unmapped IDs (e.g. horse_9796_unmapped_ids.txt.gz)

Contains RS IDs that couldn't be mapped against an assembly by dbSNP. Flanking
sequences are provided when possible.

Columns SEQ3 and SEQ5 contain the actual sequence in 3' and 5' respectively for
a particular SS ID as submitted originally to dbSNP.


2.3. Merged IDs (e.g. GCA_000002305.1_merged_ids.vcf.gz)

Contains RS IDs that should NOT be used because they have been merged into
other active RS IDs that represent the same variants. The VCF contains the same INFO
attributes than the "current_ids" VCF plus the next one:

- CURR: RS ID that currently represents the variant


2.4. Deprecated IDs (e.g. GCA_000002305.1_deprecated_ids.txt.gz)

Contains a list of RS IDs that should NOT be used since these RS IDs were
deprecated due to the following reason(s):

- The information was incomplete (Missing orientation, reference not among the
  alleles, RS ID variant type does not match type deducted from its SS IDs)

- Other factors that prevent us from describing the variants with certainty


2.5. Merged then deprecated IDs (e.g. GCA_000002305.1_merged_deprecated_ids.txt.gz)

Contains RS IDs that should NOT be used because they have been merged into an
RS ID that was deprecated later on. The deprecated RS ID is listed in the
second column.

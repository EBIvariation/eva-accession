EVA RefSNP release 5

Feedback
==================================
If you access and use the data stored in the release and have feedback, please contact EVA helpdesk eva-helpdesk@ebi.ac.uk

Definitions
==================================

Accession
-----------------------------------------------------------------

An accession is a unique and permanent identifier for an entity that enables
cross resource references to submitted objects.


Variant locus accessions
-----------------------------------------------------------------

SS or SubSnp represent an allele change in a genomic study. They are identified
by the reference sequence, project/study, contig, start position, reference
allele, alternate allele.

RS or RefSnp is a cluster of SS IDs. They are identified by reference sequence,
contig, start position, variant class.


Assembly accessions
-----------------------------------------------------------------

An assembly accession starting with the three letters prefix GCA correspond to
INSDC assemblies. The prefix is followed by an underscore, 9 digits and a
version is added at the end. For example, the assembly accession for the INSDC
version of the public horse reference assembly (EquCab2.0) is GCA_000002305.1.


EVA RefSNP release
==================================

The EVA RS release is a collection of files organized by species that list all
the RS IDs for variants stored in the European Variation Archive (EVA,
https://www.ebi.ac.uk/eva/).


Folder structure
==================================

The top folder contains folders for browsing the release files by species or by
assembly accession.

In each species folder, there are links to the folders of the assemblies
available for that species, both by assembly name (e.g. GRCm38.p4) and by
assembly accession (e.g. GCA_000001635.6).


Released files
==================================

Given a species (e.g. horse, GCA_000002305.1), the following set of files will
be present in the RS release:

- GCA_000002305.1_current_ids.vcf.gz
- horse_9796_unmapped_ids.txt.gz
- GCA_000002305.1_merged_ids.vcf.gz
- GCA_000002305.1_deprecated_ids.txt.gz
- GCA_000002305.1_merged_deprecated_ids.txt.gz

The "unmapped_ids" files is located in the species folder, while the others are
located in the assembly folders.


Current IDs - ex: GCA_000002305.1_current_ids.vcf.gz
-----------------------------------------------------------------

Contains the RS IDs which can be browsed from the EVA website
(https://www.ebi.ac.uk/eva/) and web services
(https://www.ebi.ac.uk/eva/webservices/identifiers/swagger-ui.html).

The file is in VCF format and some extra information is available in the INFO
column, using the next keys:

- REMAPPED: Remapped flag, present if the clustered variant originate only from submitted variants
  remapped to this assembly

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


Unmapped variants - ex: horse_9796_unmapped_variant_report.txt.gz
-----------------------------------------------------------------

Contains RS IDs that couldn't be mapped against an assembly by dbSNP. Flanking
sequences are provided when possible.

Columns SEQ3 and SEQ5 contain the actual sequence in 3' and 5' respectively for
a particular SS ID as submitted originally to dbSNP.


Merged RS IDs - ex: GCA_000002305.1_merged_ids.vcf.gz
-----------------------------------------------------------------

Contains RS IDs that should NOT be used because they have been merged into
other active RS IDs that represent the same variants. The VCF contains the same INFO
attributes than the "current_ids" VCF plus the next one:

- CURR: RS ID that currently represents the variant


Deprecated RS IDs - ex: GCA_000002305.1_deprecated_ids.txt.gz
-----------------------------------------------------------------

Contains a list of RS IDs that should NOT be used since these RS IDs were
deprecated due to the following reason(s):

- The information was incomplete (Missing orientation, reference not among the
  alleles, RS ID variant type does not match type deducted from its SS IDs)

- The position of the variant went beyond the chromosome length

- The position of the variant was imprecise (formerly called multimapped)

- The reference allele provided does not match the reference genome

- Other factors that prevent us from describing the variants with certainty


Merged then deprecated RS IDs - ex: GCA_000002305.1_merged_deprecated_ids.txt.gz
-----------------------------------------------------------------

Contains RS IDs that should NOT be used because they have been merged into an
RS ID that was deprecated later on. The deprecated RS ID is listed in the
second column.


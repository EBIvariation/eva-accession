This document describes technical changes associated with the EVA RefSNP release.

EVA RefSNP release 5
====================

- Release is now performed per taxonomy of the submitted variants:
  Previous release contained variants based on their presence on an assembly.
  From release 5, a variant will only be present in a release file if it was found in the specified taxonomy.
- The variants previously marked as multi-mapped have now been deprecated and are available in the deprecated file
  of each release.
- Previously unreleased variant mismatching the reference genome are now release in the deprecated file of each release.


EVA RefSNP release 4
====================

 - New REMAPPED INFO tag present in the current VCF specifying if the variant is only supported by remapped evidence


EVA RefSNP release 3
====================

 - In this release the variant have been remapped to the latest assembly supported by Ensembl. EVA will endeavour to
   keep the assembly versions in sync with Ensembl versions

EVA RefSNP release 2
====================

 - First release containing data from both dbSNP and EVA

EVA RefSNP release 1
====================

 - Initial release containing only variants defined in dbSNP and ported to EVA
/*
 * Copyright 2018 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.accession.core.persistence;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;

public class DbsnpSubmittedVariantEntity extends SubmittedVariantEntity {

    DbsnpSubmittedVariantEntity() {
        super();
    }

    public DbsnpSubmittedVariantEntity(Long accession, String hashedMessage, ISubmittedVariant model, int version) {
        super(accession, hashedMessage, model, model.getCreatedDate(), version);
    }

    public DbsnpSubmittedVariantEntity(Long accession, String hashedMessage, String referenceSequenceAccession,
                                       int taxonomyAccession, String projectAccession, String contig, long start,
                                       String referenceAllele, String alternateAllele,
                                       Long clusteredVariantAccession, Boolean isSupportedByEvidence,
                                       Boolean matchesAssembly, Boolean allelesMatch, Boolean validated, int version) {
        super(accession, hashedMessage, referenceSequenceAccession, taxonomyAccession, projectAccession, contig, start,
              referenceAllele, alternateAllele, clusteredVariantAccession, isSupportedByEvidence, matchesAssembly,
              allelesMatch, validated, version);
    }
}

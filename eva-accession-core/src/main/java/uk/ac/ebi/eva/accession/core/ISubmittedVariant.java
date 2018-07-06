/*
 *
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
 *
 */
package uk.ac.ebi.eva.accession.core;

import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantEntity;

import java.time.LocalDateTime;

/**
 * Abstract representation of the fields second uniquely identify an accessioned submitted variant. Implemented by the
 * entity serialized into the database {@link SubmittedVariantEntity} and the message/DTO used by the REST API.
 */
public interface ISubmittedVariant {

    String getAssemblyAccession();

    int getTaxonomyAccession();

    String getProjectAccession();

    String getContig();

    long getStart();

    String getReferenceAllele();

    String getAlternateAllele();

    Long getClusteredVariantAccession();

    /**
     * @return True if this submitted variant is supported by genotypes or frequencies
     */
    Boolean getSupportedByEvidence();

    Boolean getMatchesAssembly();

    Boolean getAllelesMatch();

    Boolean getValidated();

    LocalDateTime getCreatedDate();

    static boolean equals(ISubmittedVariant first, ISubmittedVariant second) {
        if (first == second) {
            return true;
        }
        if (first == null || second == null) {
            return false;
        }

        if (first.getTaxonomyAccession() != second.getTaxonomyAccession()) {
            return false;
        }
        if (first.getStart() != second.getStart()) {
            return false;
        }
        if (!first.getAssemblyAccession().equals(second.getAssemblyAccession())) {
            return false;
        }
        if (!first.getProjectAccession().equals(second.getProjectAccession())) {
            return false;
        }
        if (!first.getContig().equals(second.getContig())) {
            return false;
        }
        if (!first.getReferenceAllele().equals(second.getReferenceAllele())) {
            return false;
        }
        if (!first.getAlternateAllele().equals(second.getAlternateAllele())) {
            return false;
        }
        if (first.getClusteredVariantAccession() != null ? !first.getClusteredVariantAccession().equals(
                second.getClusteredVariantAccession()) : second.getClusteredVariantAccession() != null) {
            return false;
        }
        if (first.getSupportedByEvidence() != null ? !first.getSupportedByEvidence().equals(
                second.getSupportedByEvidence()) : second.getSupportedByEvidence() != null) {

            return false;
        }
        if (first.getMatchesAssembly() != null ? !first.getMatchesAssembly().equals(
                second.getMatchesAssembly()) : second.getMatchesAssembly() != null) {

            return false;
        }
        if (first.getAllelesMatch() != null ? !first.getAllelesMatch().equals(second.getAllelesMatch())
                : second.getAllelesMatch() != null) {
            return false;
        }
        if (first.getValidated() != null ? !first.getValidated().equals(second.getValidated())
                : second.getValidated() != null) {
            return false;
        }

        // do NOT take into account the date, don't use the next code:
//        if (first.getCreatedDate() != null ? !first.getCreatedDate().equals(second.getCreatedDate())
//                : second.getCreatedDate() != null) {
//            return false;
//        }
        return true;
    }

    static int hashCode(ISubmittedVariant iSubmittedVariant) {
        int result = iSubmittedVariant.getAssemblyAccession().hashCode();
        result = 31 * result + iSubmittedVariant.getTaxonomyAccession();
        result = 31 * result + iSubmittedVariant.getProjectAccession().hashCode();
        result = 31 * result + iSubmittedVariant.getContig().hashCode();
        result = 31 * result + (int) (iSubmittedVariant.getStart() ^ (iSubmittedVariant.getStart() >>> 32));
        result = 31 * result + iSubmittedVariant.getReferenceAllele().hashCode();
        result = 31 * result + iSubmittedVariant.getAlternateAllele().hashCode();
        result = 31 * result + (iSubmittedVariant.getClusteredVariantAccession() != null ?
                iSubmittedVariant.getClusteredVariantAccession().hashCode() : 0);
        result = 31 * result + (iSubmittedVariant.getSupportedByEvidence() != null ?
                iSubmittedVariant.getSupportedByEvidence().hashCode() : 0);
        result = 31 * result + (iSubmittedVariant.getMatchesAssembly() != null ?
                iSubmittedVariant.getMatchesAssembly().hashCode() : 0);
        result = 31 * result + (iSubmittedVariant.getAllelesMatch() != null ?
                iSubmittedVariant.getAllelesMatch().hashCode() : 0);
        result = 31 * result + (iSubmittedVariant.getValidated() != null ?
                iSubmittedVariant.getValidated().hashCode() : 0);

        // do NOT take into account the date, don't use the next code:
//        result = 31 * result + (iSubmittedVariant.getCreatedDate() != null ?
//                iSubmittedVariant.getCreatedDate().hashCode() : 0);
        return result;
    }
}

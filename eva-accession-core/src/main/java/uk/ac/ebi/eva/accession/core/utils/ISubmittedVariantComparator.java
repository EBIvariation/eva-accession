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
package uk.ac.ebi.eva.accession.core.utils;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;

/**
 * TODO: reduce the number of implementations of {@link uk.ac.ebi.eva.accession.core.ISubmittedVariant} and remove
 * this class
 */
public class ISubmittedVariantComparator {

    public static boolean equals(ISubmittedVariant first, ISubmittedVariant second) {
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
        if (first.isSupportedByEvidence() != null ? !first.isSupportedByEvidence().equals(
                second.isSupportedByEvidence()) : second.isSupportedByEvidence() != null) {

            return false;
        }
        if (first.isAssemblyMatch() != null ? !first.isAssemblyMatch().equals(
                second.isAssemblyMatch()) : second.isAssemblyMatch() != null) {

            return false;
        }
        if (first.isAllelesMatch() != null ? !first.isAllelesMatch().equals(second.isAllelesMatch())
                : second.isAllelesMatch() != null) {
            return false;
        }
        if (first.isValidated() != null ? !first.isValidated().equals(second.isValidated())
                : second.isValidated() != null) {
            return false;
        }

        // do NOT take into account the date

        return true;
    }

    public static int hashCode(ISubmittedVariant iSubmittedVariant) {
        int result = iSubmittedVariant.getAssemblyAccession().hashCode();
        result = 31 * result + iSubmittedVariant.getTaxonomyAccession();
        result = 31 * result + iSubmittedVariant.getProjectAccession().hashCode();
        result = 31 * result + iSubmittedVariant.getContig().hashCode();
        result = 31 * result + (int) (iSubmittedVariant.getStart() ^ (iSubmittedVariant.getStart() >>> 32));
        result = 31 * result + iSubmittedVariant.getReferenceAllele().hashCode();
        result = 31 * result + iSubmittedVariant.getAlternateAllele().hashCode();
        result = 31 * result + (iSubmittedVariant.getClusteredVariantAccession() != null ?
                iSubmittedVariant.getClusteredVariantAccession().hashCode() : 0);
        result = 31 * result + (iSubmittedVariant.isSupportedByEvidence() != null ?
                iSubmittedVariant.isSupportedByEvidence().hashCode() : 0);
        result = 31 * result + (iSubmittedVariant.isAssemblyMatch() != null ?
                iSubmittedVariant.isAssemblyMatch().hashCode() : 0);
        result = 31 * result + (iSubmittedVariant.isAllelesMatch() != null ?
                iSubmittedVariant.isAllelesMatch().hashCode() : 0);
        result = 31 * result + (iSubmittedVariant.isValidated() != null ?
                iSubmittedVariant.isValidated().hashCode() : 0);

        // do NOT take into account the date

        return result;
    }
}

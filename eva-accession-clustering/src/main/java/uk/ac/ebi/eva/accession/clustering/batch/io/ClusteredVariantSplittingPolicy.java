/*
 * Copyright 2021 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.clustering.batch.io;

import org.apache.commons.lang3.tuple.Triple;

import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;

/**
 * This class represents the policy for choosing which RS ID to keep when 2 of them are split.
 */
public class ClusteredVariantSplittingPolicy {

    public static class SplitPriority {

        // Triple containing the RS, corresponding hash and the number of variants with that hash
        public final Triple<ClusteredVariantEntity, String, Integer> hashThatShouldRetainOldRS;

        public final Triple<ClusteredVariantEntity, String, Integer> hashThatShouldGetNewRS;

        public SplitPriority(Triple<ClusteredVariantEntity, String, Integer> hashThatShouldRetainOldRS,
                             Triple<ClusteredVariantEntity, String, Integer> hashThatShouldGetNewRS) {
            this.hashThatShouldRetainOldRS = hashThatShouldRetainOldRS;
            this.hashThatShouldGetNewRS = hashThatShouldGetNewRS;
        }
    }

    /**
     * At the moment, the priority is just to keep the variant locus with the most support and split the other
     * Use lexicographic order of hash components (asm, contig, start, type) as the tie-breaker
     * TODO: Use more sophisticated tie-breakers laid out in the document below if/when possible
     *
     * @see
     * <a href="https://www.ebi.ac.uk/seqdb/confluence/pages/worddav/preview.action?fileName=VAR-RSIDAssignment-100220-1039-34.pdf&pageId=115948132">
     * dbSNP policies for assigning RS IDs</a>
     */
    public static SplitPriority prioritise(Triple<ClusteredVariantEntity, String, Integer>
                                                   firstRSHashAndNumberOfSupportingVariants,
                                           Triple<ClusteredVariantEntity, String, Integer>
                                                   secondRSHashAndNumberOfSupportingVariants) {

        ClusteredVariantEntity firstRS = firstRSHashAndNumberOfSupportingVariants.getLeft();
        ClusteredVariantEntity secondRS = secondRSHashAndNumberOfSupportingVariants.getLeft();

        // Split is only valid for variants sharing the same RS
        if (!firstRS.getAccession().equals(secondRS.getAccession())) {
            throw new IllegalArgumentException("RS split is not valid for two different RS: "
                                                       + firstRS.getAccession() + " and " + secondRS.getAccession());
        }

        // Split is only valid if the hashes are different for the two RS
        if (firstRS.getHashedMessage().equals(secondRS.getHashedMessage())) {
            throw new IllegalArgumentException("RS split is not valid for variants with RS: "
                                                       + firstRS.getAccession() + " that share the same hash: "
                                                       + firstRS.getHashedMessage());
        }

        Integer numSupportingVariantsForFirstHash = firstRSHashAndNumberOfSupportingVariants.getRight();
        Integer numSupportingVariantsForSecondHash = secondRSHashAndNumberOfSupportingVariants.getRight();

        if (numSupportingVariantsForFirstHash > numSupportingVariantsForSecondHash) {
            return new SplitPriority(firstRSHashAndNumberOfSupportingVariants,
                                     secondRSHashAndNumberOfSupportingVariants);
        } else if (numSupportingVariantsForSecondHash > numSupportingVariantsForFirstHash) {
            return new SplitPriority(secondRSHashAndNumberOfSupportingVariants,
                                     firstRSHashAndNumberOfSupportingVariants);
        } else {
            // If two RS have equal number of supporting loci,
            // use lexicographic ordering of hash components as a tie-breaker
            // https://docs.oracle.com/javase/8/docs/api/java/lang/String.html#compareTo-java.lang.String-
            ClusteredVariantSummaryFunction summaryFunction = new ClusteredVariantSummaryFunction();
            if (summaryFunction.apply(firstRS).compareTo(summaryFunction.apply(secondRS)) < 0) {
                return new SplitPriority(firstRSHashAndNumberOfSupportingVariants,
                                         secondRSHashAndNumberOfSupportingVariants);
            } else {
                return new SplitPriority(secondRSHashAndNumberOfSupportingVariants,
                                         firstRSHashAndNumberOfSupportingVariants);
            }
        }
    }
}

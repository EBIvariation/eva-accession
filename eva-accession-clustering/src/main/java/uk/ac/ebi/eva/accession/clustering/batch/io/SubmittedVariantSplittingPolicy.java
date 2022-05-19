/*
 * Copyright 2022 EMBL - European Bioinformatics Institute
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

import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;

import java.util.Objects;

/**
 * This class represents the policy for choosing which SS ID to keep when 2 of them are split.
 */
public class SubmittedVariantSplittingPolicy {

    public static class SplitDeterminants {
        private final SubmittedVariantEntity submittedVariantEntity;
        private final String ssHash;
        private final Long rsID;

        public SplitDeterminants(SubmittedVariantEntity submittedVariantEntity, String ssHash,
                                 Long rsID) {
            this.submittedVariantEntity = submittedVariantEntity;
            this.ssHash = ssHash;
            this.rsID = rsID;
        }

        public SubmittedVariantEntity getSubmittedVarianEntity() {
            return submittedVariantEntity;
        }

        public String getSsHash() {
            return ssHash;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SplitDeterminants that = (SplitDeterminants) o;
            return Objects.equals(rsID, that.rsID)
                    && submittedVariantEntity.equals(that.submittedVariantEntity) && ssHash.equals(that.ssHash);
        }

        @Override
        public int hashCode() {
            return Objects.hash(submittedVariantEntity, ssHash, rsID);
        }
    }

    public static class SplitPriority {

        public final SplitDeterminants hashThatShouldRetainOldSS;

        public final SplitDeterminants hashThatShouldGetNewSS;

        public SplitPriority(SplitDeterminants hashThatShouldRetainOldSS, SplitDeterminants hashThatShouldGetNewSS) {
            this.hashThatShouldRetainOldSS = hashThatShouldRetainOldSS;
            this.hashThatShouldGetNewSS = hashThatShouldGetNewSS;
        }
    }

    public static SplitPriority prioritise(SplitDeterminants firstSSHashSplitDeterminants,
                                           SplitDeterminants secondSSHashSplitDeterminants) {

        SubmittedVariantEntity firstSS = firstSSHashSplitDeterminants.submittedVariantEntity;
        SubmittedVariantEntity secondSS = secondSSHashSplitDeterminants.submittedVariantEntity;

        // Split is only valid for variants sharing the same RS
        if (!firstSS.getAccession().equals(secondSS.getAccession())) {
            throw new IllegalArgumentException("SS split is not valid for two different SS: "
                                                       + firstSS.getAccession() + " and " + secondSS.getAccession());
        }

        // Split is only valid if the hashes are different for the two SS
        if (firstSS.getHashedMessage().equals(secondSS.getHashedMessage())) {
            throw new IllegalArgumentException("SS split is not valid for variants with SS: "
                                                       + firstSS.getAccession() + " that share the same hash: "
                                                       + firstSS.getHashedMessage());
        }

        boolean firstHashHasEvidence = firstSS.isSupportedByEvidence();
        boolean secondHashHasEvidence = secondSS.isSupportedByEvidence();

        // SS entries with RS ID get to retain their ID and the other SS will be selected for split
        if (Objects.nonNull(firstSSHashSplitDeterminants.rsID) &&
                Objects.isNull(secondSSHashSplitDeterminants.rsID)) {
            return new SplitPriority(firstSSHashSplitDeterminants, secondSSHashSplitDeterminants);
        }
        if (Objects.nonNull(secondSSHashSplitDeterminants.rsID) &&
                Objects.isNull(firstSSHashSplitDeterminants.rsID)) {
            return new SplitPriority(secondSSHashSplitDeterminants, firstSSHashSplitDeterminants);
        }
        // If both SS have RS IDs, use the one that has evidence as the tie-breaker
        if (firstHashHasEvidence && !secondHashHasEvidence) {
            return new SplitPriority(firstSSHashSplitDeterminants, secondSSHashSplitDeterminants);
        }
        if (secondHashHasEvidence && !firstHashHasEvidence) {
            return new SplitPriority(secondSSHashSplitDeterminants, firstSSHashSplitDeterminants);
        }
        // If two SS have same evidence and have RS assignments
        // use lexicographic ordering of hash components as a tie-breaker
        // https://docs.oracle.com/javase/8/docs/api/java/lang/String.html#compareTo-java.lang.String-
        SubmittedVariantSummaryFunction summaryFunction = new SubmittedVariantSummaryFunction();
        if (summaryFunction.apply(firstSS).compareTo(summaryFunction.apply(secondSS)) < 0) {
            return new SplitPriority(firstSSHashSplitDeterminants, secondSSHashSplitDeterminants);
        }

        return new SplitPriority(secondSSHashSplitDeterminants, firstSSHashSplitDeterminants);
    }
}

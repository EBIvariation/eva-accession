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

import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;

import java.util.Objects;

/**
 * This class represents the policy for choosing which RS ID to keep when 2 of them are split.
 */
public class ClusteredVariantSplittingPolicy {

    public static class SplitDeterminants {
        private final ClusteredVariantEntity clusteredVariantEntity;
        private final String rsHash;
        private final int numSupportingVariants;
        private final long oldestSSID;

        public SplitDeterminants(ClusteredVariantEntity clusteredVariantEntity, String rsHash,
                                 int numSupportingVariants,
                                 long oldestSSID) {
            this.clusteredVariantEntity = clusteredVariantEntity;
            this.rsHash = rsHash;
            this.numSupportingVariants = numSupportingVariants;
            this.oldestSSID = oldestSSID;
        }

        public ClusteredVariantEntity getClusteredVariantEntity() {
            return clusteredVariantEntity;
        }

        public String getRsHash() {
            return rsHash;
        }

        public int getNumSupportingVariants() {
            return numSupportingVariants;
        }

        public long getOldestSSID() {
            return oldestSSID;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SplitDeterminants that = (SplitDeterminants) o;
            return numSupportingVariants == that.numSupportingVariants && oldestSSID == that.oldestSSID
                    && clusteredVariantEntity.equals(that.clusteredVariantEntity) && rsHash.equals(that.rsHash);
        }

        @Override
        public int hashCode() {
            return Objects.hash(clusteredVariantEntity, rsHash, numSupportingVariants, oldestSSID);
        }
    }

    public static class SplitPriority {

        // Triple containing the RS, corresponding hash and the number of variants with that hash
        public final SplitDeterminants hashThatShouldRetainOldRS;

        public final SplitDeterminants hashThatShouldGetNewRS;

        public SplitPriority(SplitDeterminants hashThatShouldRetainOldRS, SplitDeterminants hashThatShouldGetNewRS) {
            this.hashThatShouldRetainOldRS = hashThatShouldRetainOldRS;
            this.hashThatShouldGetNewRS = hashThatShouldGetNewRS;
        }
    }

    /**
     * At the moment, the priority is just to keep the variant locus with the most support and split the other
     * Use oldest supporting SS ID as the tie-breaker
     * TODO: Use more sophisticated tie-breakers laid out in the document below if/when possible
     *
     * @see
     * <a href="https://www.ebi.ac.uk/seqdb/confluence/pages/worddav/preview.action?fileName=VAR-RSIDAssignment-100220-1039-34.pdf&pageId=115948132">
     * dbSNP policies for assigning RS IDs</a>
     */
    public static SplitPriority prioritise(SplitDeterminants firstRSHashSplitDeterminants,
                                           SplitDeterminants secondRSHashSplitDeterminants) {

        ClusteredVariantEntity firstRS = firstRSHashSplitDeterminants.clusteredVariantEntity;
        ClusteredVariantEntity secondRS = secondRSHashSplitDeterminants.clusteredVariantEntity;

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

        int numSupportingVariantsForFirstHash = firstRSHashSplitDeterminants.numSupportingVariants;
        int numSupportingVariantsForSecondHash = secondRSHashSplitDeterminants.numSupportingVariants;

        if (numSupportingVariantsForFirstHash > numSupportingVariantsForSecondHash) {
            return new SplitPriority(firstRSHashSplitDeterminants,
                                     secondRSHashSplitDeterminants);
        } else if (numSupportingVariantsForSecondHash > numSupportingVariantsForFirstHash) {
            return new SplitPriority(secondRSHashSplitDeterminants,
                                     firstRSHashSplitDeterminants);
        } else {
            // If two hashes have the same number of supporting loci
            // use the oldest supporting SS ID as the tie-breaker and that gets to retain the RS-hash association
            // See https://docs.google.com/spreadsheets/d/1KQLVCUy-vqXKgkCDt2czX6kuMfsjfCc9uBsS19MZ6dY/edit#rangeid=686889730
            if (firstRSHashSplitDeterminants.oldestSSID < secondRSHashSplitDeterminants.oldestSSID) {
                return new SplitPriority(firstRSHashSplitDeterminants, secondRSHashSplitDeterminants);
            } else {
                return new SplitPriority(secondRSHashSplitDeterminants, firstRSHashSplitDeterminants);
            }
        }
    }
}

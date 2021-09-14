/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
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

/**
 * This class represents the policy for choosing which RS ID to keep when 2 of them are merged.
 */
public class ClusteredVariantMergingPolicy {

    public static class Priority {
        public final Long accessionToKeep;
        public final Long accessionToBeMerged;

        public Priority(Long accessionToKeep, Long accessionToBeMerged) {
            this.accessionToKeep = accessionToKeep;
            this.accessionToBeMerged = accessionToBeMerged;
        }
    }

    /**
     * At the moment, the priority is just to keep the oldest accession, but other policies might apply, like choosing
     * the accession with clinical relevance (which we don't store at the moment of writing).
     * TODO: Use clinical relevance also as a priority determinant if that is available
     * @see <a href="https://ncbijira.ncbi.nlm.nih.gov/secure/attachment/95534/95534_VAR-RSIDAssignment-100220-1039-34.pdf">
     *     dbSNP policies for assigning RS IDs</a>
     */
    public static Priority prioritise(Long oneAccession, Long anotherAccession) {
        if (oneAccession < anotherAccession) {
            return new Priority(oneAccession, anotherAccession);
        } else {
            return new Priority(anotherAccession, oneAccession);
        }
    }

}

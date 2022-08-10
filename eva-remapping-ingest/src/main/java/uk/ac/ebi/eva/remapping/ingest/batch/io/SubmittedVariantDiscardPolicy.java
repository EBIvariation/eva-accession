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
package uk.ac.ebi.eva.remapping.ingest.batch.io;

import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;

import java.time.LocalDateTime;

/**
 * This class represents the policy for choosing which SVE to discard when the SS ID is duplicated.
 */
public class SubmittedVariantDiscardPolicy {

    public static class SubmittedVariantDiscardDeterminants {

        private final SubmittedVariantEntity sve;

        private final Long ssId;

        private final String remappedFrom;

        private final LocalDateTime createdDate;

        public SubmittedVariantDiscardDeterminants(SubmittedVariantEntity sve, Long ssId, String remappedFrom,
                                                   LocalDateTime createdDate) {
            this.sve = sve;
            this.ssId = ssId;
            this.remappedFrom = remappedFrom;
            this.createdDate = createdDate;
        }

        public SubmittedVariantEntity getSve() {
            return sve;
        }

        public Long getSsId() {
            return ssId;
        }

        public String getRemappedFrom() {
            return remappedFrom;
        }

        public LocalDateTime getCreatedDate() {
            return createdDate;
        }

    }

    public static class DiscardPriority {
        public final SubmittedVariantDiscardDeterminants sveToKeep;
        public final SubmittedVariantDiscardDeterminants sveToDiscard;

        public DiscardPriority(SubmittedVariantDiscardDeterminants sveToKeep,
                               SubmittedVariantDiscardDeterminants sveToDiscard) {
            this.sveToKeep = sveToKeep;
            this.sveToDiscard = sveToDiscard;
        }
    }

    /**
     * Select which submitted variant to keep and which to discard, according to the following criteria in order:
     * - remappedFrom: null is kept (local to the assembly rather than remapped)
     * - createdDate: earlier is kept
     * - SS ID: smaller is kept
     * If all these attributes are equal, these are genuinely the same variant and an exception is thrown.
     */
    public static DiscardPriority prioritise(SubmittedVariantDiscardDeterminants firstSve,
                                             SubmittedVariantDiscardDeterminants secondSve) {
        if (firstSve.getRemappedFrom() == null && secondSve.getRemappedFrom() != null) {
            return new DiscardPriority(firstSve, secondSve);
        }
        if (firstSve.getRemappedFrom() != null && secondSve.getRemappedFrom() == null) {
            return new DiscardPriority(secondSve, firstSve);
        }
        if (firstSve.getCreatedDate().isBefore(secondSve.getCreatedDate())) {
            return new DiscardPriority(firstSve, secondSve);
        }
        if (firstSve.getCreatedDate().isAfter(secondSve.getCreatedDate())) {
            return new DiscardPriority(secondSve, firstSve);
        }
        if (firstSve.getSsId() < secondSve.getSsId()) {
            return new DiscardPriority(firstSve, secondSve);
        }
        if (firstSve.getSsId() > secondSve.getSsId()) {
            return new DiscardPriority(secondSve, firstSve);
        }
        throw new IllegalArgumentException("Could not prioritise between the following submitted variants:\n"
                                                   + firstSve.getSve() + "\n" + secondSve.getSve());
    }

}

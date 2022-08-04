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
     * Keep the submitted variant without remappedFrom attribute, or earliest createdDate as tie-breaker.
     */
    public static DiscardPriority prioritise(SubmittedVariantDiscardDeterminants firstSve,
                                             SubmittedVariantDiscardDeterminants secondSve) {
        // Discard is only valid for variants sharing the same SS
        if (!firstSve.getSsId().equals(secondSve.getSsId())) {
            throw new IllegalArgumentException("Submitted variant discard is not valid for two different SS: "
                                                       + firstSve.getSsId() + " and " + secondSve.getSsId());
        }

        if (firstSve.getRemappedFrom() == null && secondSve.getRemappedFrom() != null) {
            return new DiscardPriority(firstSve, secondSve);
        }
        if (firstSve.getRemappedFrom() != null && secondSve.getRemappedFrom() == null) {
            return new DiscardPriority(secondSve, firstSve);
        }
        if (firstSve.getCreatedDate().isBefore(secondSve.getCreatedDate())) {
            return new DiscardPriority(firstSve, secondSve);
        }
        return new DiscardPriority(secondSve, firstSve);
    }

}

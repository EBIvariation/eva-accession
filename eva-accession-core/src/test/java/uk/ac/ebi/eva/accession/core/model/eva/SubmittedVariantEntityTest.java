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
package uk.ac.ebi.eva.accession.core.model.eva;

import org.junit.Test;

import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;

import java.time.LocalDateTime;
import java.time.Month;

import static org.junit.Assert.assertEquals;

public class SubmittedVariantEntityTest {

    private static final String ASSEMBLY_ACCESSION = "assembly";

    private static final int TAXONOMY_ACCESSION = 1;

    private static final String PROJECT_ACCESSION = "project";

    private static final String CONTIG = "contig";

    private static final int START = 1;

    private static final String REF_A = "A";

    private static final String ALT_T = "T";

    private static final Long CLUSTERED_VARIANT = 5L;

    private static final String UNUSED_HASHED_MESSAGE = "";

    private static final long UNUSED_ACCESSION = 0L;

    @Test
    public void getModelWithFlagsTrue() {
        SubmittedVariant variant =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, CLUSTERED_VARIANT, true, true, true, true, null);

        SubmittedVariantEntity entity = new SubmittedVariantEntity(UNUSED_ACCESSION, UNUSED_HASHED_MESSAGE, variant, 1);
        assertEquals(variant, entity.getModel());
    }

    @Test
    public void getModelWithFlagsFalse() {
        SubmittedVariant variant =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, CLUSTERED_VARIANT, false, false, false, false, null);

        SubmittedVariantEntity entity = new SubmittedVariantEntity(UNUSED_ACCESSION, UNUSED_HASHED_MESSAGE, variant, 1);
        assertEquals(variant, entity.getModel());
    }

    @Test
    public void getModelWithDefaultFlags() {
        SubmittedVariant variant =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, CLUSTERED_VARIANT);

        SubmittedVariantEntity entity = new SubmittedVariantEntity(UNUSED_ACCESSION, UNUSED_HASHED_MESSAGE, variant, 1);
        variant.setSupportedByEvidence(true);
        variant.setAssemblyMatch(true);
        variant.setAllelesMatch(true);
        variant.setValidated(false);
        assertEquals(variant, entity.getModel());
    }

    @Test(expected = IllegalArgumentException.class)
    public void flagsShouldNotBeNull() {
        new SubmittedVariantEntity(UNUSED_ACCESSION, UNUSED_HASHED_MESSAGE, ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION,
                                   PROJECT_ACCESSION, CONTIG, START, REF_A, ALT_T, CLUSTERED_VARIANT, null, null, null,
                                   null, 1);
    }

    @Test
    public void creationDateIsReturnedInTheModel() {

        LocalDateTime createdDate = LocalDateTime.of(2018, Month.SEPTEMBER, 18, 9, 0);
        SubmittedVariant variant =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, CLUSTERED_VARIANT, false, false, false, false, createdDate);

        SubmittedVariantEntity entity = new SubmittedVariantEntity(UNUSED_ACCESSION, UNUSED_HASHED_MESSAGE, variant, 1);
        assertEquals(createdDate, entity.getModel().getCreatedDate());
    }
}

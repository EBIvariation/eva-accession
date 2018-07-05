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
package uk.ac.ebi.eva.accession.core.summary;

import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.eva.accession.core.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.IClusteredVariant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class DbsnpClusteredVariantSummaryFunctionTest {

    private static final String ASSEMBLY_ACCESSION = "assembly";

    private static final int TAXONOMY_ACCESSION = 1;

    private static final String CONTIG = "contig";

    private static final int START = 1;

    private static final String REF_A = "A";

    private static final String ALT_T = "T";

    private static final Boolean VALIDATED = null;

    private DbsnpClusteredVariantSummaryFunction summaryFunction;

    @Before
    public void setUp() {
        summaryFunction = new DbsnpClusteredVariantSummaryFunction();
    }

    @Test
    public void summaryFunctionMustBeIdempotent() {
        IClusteredVariant ClusteredVariant1 =
                new ClusteredVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, CONTIG, START, REF_A, ALT_T, VALIDATED);

        assertEquals(summaryFunction.apply(ClusteredVariant1), summaryFunction.apply(ClusteredVariant1));
    }

    @Test
    public void differentSummaryWhenAssemblyDiffers() {
        IClusteredVariant ClusteredVariant1 =
                new ClusteredVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, CONTIG, START, REF_A, ALT_T, VALIDATED);

        IClusteredVariant ClusteredVariant2 =
                new ClusteredVariant("anotherAssembly", TAXONOMY_ACCESSION, CONTIG, START, REF_A, ALT_T, VALIDATED);

        assertNotEquals(summaryFunction.apply(ClusteredVariant1), summaryFunction.apply(ClusteredVariant2));
    }

    @Test
    public void differentSummaryWhenTaxonomyDiffers() {
        IClusteredVariant ClusteredVariant1 =
                new ClusteredVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, CONTIG, START, REF_A, ALT_T, VALIDATED);

        int taxonomyAccession2 = 2;
        IClusteredVariant ClusteredVariant2 =
                new ClusteredVariant(ASSEMBLY_ACCESSION, taxonomyAccession2, CONTIG, START, REF_A, ALT_T, VALIDATED);

        assertNotEquals(summaryFunction.apply(ClusteredVariant1), summaryFunction.apply(ClusteredVariant2));
    }

    @Test
    public void differentSummaryWhenContigDiffers() {
        IClusteredVariant ClusteredVariant1 =
                new ClusteredVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, CONTIG, START, REF_A, ALT_T, VALIDATED);

        IClusteredVariant ClusteredVariant2 =
                new ClusteredVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, "anotherContig", START,
                                     REF_A, ALT_T, VALIDATED);

        assertNotEquals(summaryFunction.apply(ClusteredVariant1), summaryFunction.apply(ClusteredVariant2));
    }

    @Test
    public void differentSummaryWhenStartDiffers() {
        IClusteredVariant ClusteredVariant1 =
                new ClusteredVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, CONTIG, START, REF_A, ALT_T, VALIDATED);

        IClusteredVariant ClusteredVariant2 =
                new ClusteredVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, CONTIG, 2, REF_A, ALT_T, VALIDATED);

        assertNotEquals(summaryFunction.apply(ClusteredVariant1), summaryFunction.apply(ClusteredVariant2));
    }

    @Test
    public void differentSummaryWhenReferenceDiffers() {
        IClusteredVariant ClusteredVariant1 =
                new ClusteredVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, CONTIG, START, REF_A, ALT_T, VALIDATED);

        IClusteredVariant ClusteredVariant2 =
                new ClusteredVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, CONTIG, START, "C", ALT_T, VALIDATED);

        assertNotEquals(summaryFunction.apply(ClusteredVariant1), summaryFunction.apply(ClusteredVariant2));
    }

    @Test
    public void differentSummaryWhenAlternateDiffers() {
        IClusteredVariant ClusteredVariant1 =
                new ClusteredVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, CONTIG, START, REF_A, ALT_T, VALIDATED);

        IClusteredVariant ClusteredVariant2 =
                new ClusteredVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, CONTIG, START, REF_A, "G", VALIDATED);

        assertNotEquals(summaryFunction.apply(ClusteredVariant1), summaryFunction.apply(ClusteredVariant2));
    }

    @Test
    public void sameSummaryWhenValidatedDiffers() {
        IClusteredVariant ClusteredVariant1 =
                new ClusteredVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, CONTIG, START, REF_A, ALT_T, VALIDATED);

        IClusteredVariant ClusteredVariant2 =
                new ClusteredVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, CONTIG, START, REF_A, ALT_T, false);

        assertEquals(summaryFunction.apply(ClusteredVariant1), summaryFunction.apply(ClusteredVariant2));
    }

}

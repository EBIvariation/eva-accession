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
import uk.ac.ebi.eva.commons.core.models.VariantType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class DbsnpClusteredVariantSummaryFunctionTest {

    private static final String ASSEMBLY_ACCESSION = "assembly";

    private static final int TAXONOMY_ACCESSION = 1;

    private static final String CONTIG = "contig";

    private static final int START = 1;

    private static final VariantType VARIANT_TYPE = VariantType.SNV;

    private static final Boolean VALIDATED = null;

    private DbsnpClusteredVariantSummaryFunction summaryFunction;

    @Before
    public void setUp() {
        summaryFunction = new DbsnpClusteredVariantSummaryFunction();
    }

    @Test
    public void summaryFunctionMustBeIdempotent() {
        IClusteredVariant clusteredVariant1 =
                new ClusteredVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, CONTIG, START, VARIANT_TYPE, VALIDATED);

        assertEquals(summaryFunction.apply(clusteredVariant1), summaryFunction.apply(clusteredVariant1));
    }

    @Test
    public void differentSummaryWhenAssemblyDiffers() {
        IClusteredVariant clusteredVariant1 =
                new ClusteredVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, CONTIG, START, VARIANT_TYPE, VALIDATED);

        IClusteredVariant clusteredVariant2 =
                new ClusteredVariant("anotherAssembly", TAXONOMY_ACCESSION, CONTIG, START, VARIANT_TYPE, VALIDATED);

        assertNotEquals(summaryFunction.apply(clusteredVariant1), summaryFunction.apply(clusteredVariant2));
    }

    @Test
    public void differentSummaryWhenTaxonomyDiffers() {
        IClusteredVariant clusteredVariant1 =
                new ClusteredVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, CONTIG, START, VARIANT_TYPE, VALIDATED);

        int taxonomyAccession2 = 2;
        IClusteredVariant clusteredVariant2 =
                new ClusteredVariant(ASSEMBLY_ACCESSION, taxonomyAccession2, CONTIG, START, VARIANT_TYPE, VALIDATED);

        assertNotEquals(summaryFunction.apply(clusteredVariant1), summaryFunction.apply(clusteredVariant2));
    }

    @Test
    public void differentSummaryWhenContigDiffers() {
        IClusteredVariant clusteredVariant1 =
                new ClusteredVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, CONTIG, START, VARIANT_TYPE, VALIDATED);

        IClusteredVariant clusteredVariant2 =
                new ClusteredVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, "anotherContig", START,
                                     VARIANT_TYPE, VALIDATED);

        assertNotEquals(summaryFunction.apply(clusteredVariant1), summaryFunction.apply(clusteredVariant2));
    }

    @Test
    public void differentSummaryWhenStartDiffers() {
        IClusteredVariant clusteredVariant1 =
                new ClusteredVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, CONTIG, START, VARIANT_TYPE, VALIDATED);

        IClusteredVariant clusteredVariant2 =
                new ClusteredVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, CONTIG, 2, VARIANT_TYPE, VALIDATED);

        assertNotEquals(summaryFunction.apply(clusteredVariant1), summaryFunction.apply(clusteredVariant2));
    }

    @Test
    public void differentSummaryWhenTypeDiffers() {
        IClusteredVariant clusteredVariant1 =
                new ClusteredVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, CONTIG, START, VARIANT_TYPE, VALIDATED);

        IClusteredVariant clusteredVariant2 =
                new ClusteredVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, CONTIG, START, VariantType.MNV, VALIDATED);

        assertNotEquals(summaryFunction.apply(clusteredVariant1), summaryFunction.apply(clusteredVariant2));
    }

    @Test
    public void sameSummaryWhenValidatedDiffers() {
        IClusteredVariant clusteredVariant1 =
                new ClusteredVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, CONTIG, START, VARIANT_TYPE, VALIDATED);

        IClusteredVariant clusteredVariant2 =
                new ClusteredVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, CONTIG, START, VARIANT_TYPE, false);

        assertEquals(summaryFunction.apply(clusteredVariant1), summaryFunction.apply(clusteredVariant2));
    }

}

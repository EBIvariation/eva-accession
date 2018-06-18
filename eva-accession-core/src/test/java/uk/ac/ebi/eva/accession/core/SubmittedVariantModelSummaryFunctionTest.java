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
package uk.ac.ebi.eva.accession.core;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class SubmittedVariantModelSummaryFunctionTest {

    private static final String ASSEMBLY_ACCESSION = "assembly";

    private static final int TAXONOMY_ACCESSION = 1;

    private static final String PROJECT_ACCESSION = "project";

    private static final String CONTIG = "contig";

    private static final int START = 1;

    private static final String REF_A = "A";

    private static final String ALT_T = "T";

    private static final boolean SUPPORTED_BY_EVIDENCE = true;

    private SubmittedVariantModelSummaryFunction summaryFunction;

    @Before
    public void setUp() {
        summaryFunction = new SubmittedVariantModelSummaryFunction();
    }

    @Test
    public void summaryFunctionMustBeIdempotent() {
        ISubmittedVariant submittedVariant1 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, SUPPORTED_BY_EVIDENCE);

        assertEquals(summaryFunction.apply(submittedVariant1), summaryFunction.apply(submittedVariant1));
    }

    @Test
    public void differentSummaryWhenAssemblyDiffers() {
        ISubmittedVariant submittedVariant1 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                        REF_A, ALT_T, SUPPORTED_BY_EVIDENCE);

        ISubmittedVariant submittedVariant2 =
                new SubmittedVariant("anotherAssembly", TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                        REF_A, ALT_T, SUPPORTED_BY_EVIDENCE);

        assertNotEquals(summaryFunction.apply(submittedVariant1), summaryFunction.apply(submittedVariant2));
    }

    @Test
    public void sameSummaryWhenTaxonomyDiffers() {
        ISubmittedVariant submittedVariant1 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, SUPPORTED_BY_EVIDENCE);

        int taxonomyAccession2 = 2;
        ISubmittedVariant submittedVariant2 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, taxonomyAccession2, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, SUPPORTED_BY_EVIDENCE);

        assertEquals(summaryFunction.apply(submittedVariant1), summaryFunction.apply(submittedVariant2));
    }

    @Test
    public void differentSummaryWhenContigDiffers() {
        ISubmittedVariant submittedVariant1 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, SUPPORTED_BY_EVIDENCE);

        ISubmittedVariant submittedVariant2 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, "anotherContig", START,
                                     REF_A, ALT_T, SUPPORTED_BY_EVIDENCE);

        assertNotEquals(summaryFunction.apply(submittedVariant1), summaryFunction.apply(submittedVariant2));
    }

    @Test
    public void differentSummaryWhenStartDiffers() {
        ISubmittedVariant submittedVariant1 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, SUPPORTED_BY_EVIDENCE);

        ISubmittedVariant submittedVariant2 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, 2,
                                     REF_A, ALT_T, SUPPORTED_BY_EVIDENCE);

        assertNotEquals(summaryFunction.apply(submittedVariant1), summaryFunction.apply(submittedVariant2));
    }

    @Test
    public void differentSummaryWhenReferenceDiffers() {
        ISubmittedVariant submittedVariant1 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, SUPPORTED_BY_EVIDENCE);

        ISubmittedVariant submittedVariant2 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     "C", ALT_T, SUPPORTED_BY_EVIDENCE);

        assertNotEquals(summaryFunction.apply(submittedVariant1), summaryFunction.apply(submittedVariant2));
    }

    @Test
    public void differentSummaryWhenAlternateDiffers() {
        ISubmittedVariant submittedVariant1 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, SUPPORTED_BY_EVIDENCE);

        ISubmittedVariant submittedVariant2 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, "G", SUPPORTED_BY_EVIDENCE);

        assertNotEquals(summaryFunction.apply(submittedVariant1), summaryFunction.apply(submittedVariant2));
    }

    @Test
    public void sameSummaryWhenSupportedByEvidenceDiffers() {
        ISubmittedVariant submittedVariant1 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                        REF_A, ALT_T, SUPPORTED_BY_EVIDENCE);

        ISubmittedVariant submittedVariant2 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                        REF_A, ALT_T, false);

        assertEquals(summaryFunction.apply(submittedVariant1), summaryFunction.apply(submittedVariant2));
    }

}

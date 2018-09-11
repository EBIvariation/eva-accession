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

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class SubmittedVariantSummaryFunctionTest {

    private static final String ASSEMBLY_ACCESSION = "assembly";

    private static final int TAXONOMY_ACCESSION = 1;

    private static final String PROJECT_ACCESSION = "project";

    private static final String CONTIG = "contig";

    private static final int START = 1;

    private static final String REF_A = "A";

    private static final String ALT_T = "T";

    private static final Long CLUSTERED_VARIANT = null;

    private static final Boolean SUPPORTED_BY_EVIDENCE = null;

    private static final Boolean MATCHES_ASSEMBLY = null;

    private static final Boolean ALLELES_MATCH = null;

    private static final Boolean VALIDATED = null;

    private SubmittedVariantSummaryFunction summaryFunction;

    @Before
    public void setUp() {
        summaryFunction = new SubmittedVariantSummaryFunction();
    }

    @Test
    public void summaryFunctionMustBeIdempotent() {
        ISubmittedVariant submittedVariant1 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY,
                                     ALLELES_MATCH, VALIDATED, null);

        assertEquals(summaryFunction.apply(submittedVariant1), summaryFunction.apply(submittedVariant1));
    }

    @Test
    public void differentSummaryWhenAssemblyDiffers() {
        ISubmittedVariant submittedVariant1 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY,
                                     ALLELES_MATCH, VALIDATED, null);

        ISubmittedVariant submittedVariant2 =
                new SubmittedVariant("anotherAssembly", TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY,
                                     ALLELES_MATCH, VALIDATED, null);

        assertNotEquals(summaryFunction.apply(submittedVariant1), summaryFunction.apply(submittedVariant2));
    }

    @Test
    public void sameSummaryWhenTaxonomyDiffers() {
        ISubmittedVariant submittedVariant1 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY,
                                     ALLELES_MATCH, VALIDATED, null);

        int taxonomyAccession2 = 2;
        ISubmittedVariant submittedVariant2 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, taxonomyAccession2, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY,
                                     ALLELES_MATCH, VALIDATED, null);

        assertEquals(summaryFunction.apply(submittedVariant1), summaryFunction.apply(submittedVariant2));
    }

    @Test
    public void differentSummaryWhenProjectDiffers() {
        ISubmittedVariant submittedVariant1 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY,
                                     ALLELES_MATCH, VALIDATED, null);

        ISubmittedVariant submittedVariant2 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, "project_2", CONTIG, START,
                                     REF_A, ALT_T, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY,
                                     ALLELES_MATCH, VALIDATED, null);

        assertNotEquals(summaryFunction.apply(submittedVariant1), summaryFunction.apply(submittedVariant2));
    }

    @Test
    public void differentSummaryWhenContigDiffers() {
        ISubmittedVariant submittedVariant1 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY,
                                     ALLELES_MATCH, VALIDATED, null);

        ISubmittedVariant submittedVariant2 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, "anotherContig", START,
                                     REF_A, ALT_T, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY,
                                     ALLELES_MATCH, VALIDATED, null);

        assertNotEquals(summaryFunction.apply(submittedVariant1), summaryFunction.apply(submittedVariant2));
    }

    @Test
    public void differentSummaryWhenStartDiffers() {
        ISubmittedVariant submittedVariant1 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY,
                                     ALLELES_MATCH, VALIDATED, null);

        ISubmittedVariant submittedVariant2 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, 2,
                                     REF_A, ALT_T, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY,
                                     ALLELES_MATCH, VALIDATED, null);

        assertNotEquals(summaryFunction.apply(submittedVariant1), summaryFunction.apply(submittedVariant2));
    }

    @Test
    public void differentSummaryWhenReferenceDiffers() {
        ISubmittedVariant submittedVariant1 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY,
                                     ALLELES_MATCH, VALIDATED, null);

        ISubmittedVariant submittedVariant2 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     "C", ALT_T, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY,
                                     ALLELES_MATCH, VALIDATED, null);

        assertNotEquals(summaryFunction.apply(submittedVariant1), summaryFunction.apply(submittedVariant2));
    }

    @Test
    public void differentSummaryWhenAlternateDiffers() {
        ISubmittedVariant submittedVariant1 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY,
                                     ALLELES_MATCH, VALIDATED, null);

        ISubmittedVariant submittedVariant2 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, "G", CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY,
                                     ALLELES_MATCH, VALIDATED, null);

        assertNotEquals(summaryFunction.apply(submittedVariant1), summaryFunction.apply(submittedVariant2));
    }

    @Test
    public void sameSummaryWhenClusteredVariantDiffers() {
        ISubmittedVariant submittedVariant1 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY,
                                     ALLELES_MATCH, VALIDATED, null);

        ISubmittedVariant submittedVariant2 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, 5L, SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY, ALLELES_MATCH,
                                     VALIDATED, null);

        assertEquals(summaryFunction.apply(submittedVariant1), summaryFunction.apply(submittedVariant2));
    }

    @Test
    public void sameSummaryWhenSupportedByEvidenceDiffers() {
        ISubmittedVariant submittedVariant1 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY,
                                     ALLELES_MATCH, VALIDATED, null);

        ISubmittedVariant submittedVariant2 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, CLUSTERED_VARIANT, false, MATCHES_ASSEMBLY, ALLELES_MATCH,
                                     VALIDATED, null);

        assertEquals(summaryFunction.apply(submittedVariant1), summaryFunction.apply(submittedVariant2));
    }

    @Test
    public void sameSummaryWhenMatchesAssemblyDiffers() {
        ISubmittedVariant submittedVariant1 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY,
                                     ALLELES_MATCH, VALIDATED, null);

        ISubmittedVariant submittedVariant2 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE, false, ALLELES_MATCH,
                                     VALIDATED, null);

        assertEquals(summaryFunction.apply(submittedVariant1), summaryFunction.apply(submittedVariant2));
    }

    @Test
    public void sameSummaryWhenAllelesMatchDiffers() {
        ISubmittedVariant submittedVariant1 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY,
                                     ALLELES_MATCH, VALIDATED, null);

        ISubmittedVariant submittedVariant2 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY, false,
                                     VALIDATED, null);

        assertEquals(summaryFunction.apply(submittedVariant1), summaryFunction.apply(submittedVariant2));
    }

    @Test
    public void sameSummaryWhenValidatedDiffers() {
        ISubmittedVariant submittedVariant1 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START,
                                     REF_A, ALT_T, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY,
                                     ALLELES_MATCH, VALIDATED, null);

        ISubmittedVariant submittedVariant2 =
                new SubmittedVariant(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION, PROJECT_ACCESSION, CONTIG, START, REF_A,
                                     ALT_T, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY, ALLELES_MATCH,
                                     false, null);

        assertEquals(summaryFunction.apply(submittedVariant1), summaryFunction.apply(submittedVariant2));
    }
}

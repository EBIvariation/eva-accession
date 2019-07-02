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
package uk.ac.ebi.eva.accession.pipeline.steps.processors;

import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class VariantConverterTest {

    private static final String ASSEMBLY = "assembly";

    private static final int TAXONOMY = 1111;

    private static final String PROJECT = "project";

    private static final String CONTIG = "contig";

    private static final long START = 1000;

    private static final String REFERENCE_ALLELE = "A";

    private static final String ALTERNATE_ALLELE = "T";

    private static final Long CLUSTERED_VARIANT = null;

    private static final Boolean SUPPORTED_BY_EVIDENCE = true;

    private static final Boolean MATCHES_ASSEMBLY = true;

    private static final Boolean ALLELES_MATCH = true;

    private static final Boolean VALIDATED = false;

    private VariantConverter processor;

    @Before
    public void setUp() {
        processor = new VariantConverter(ASSEMBLY, TAXONOMY, PROJECT);
    }

    @Test
    public void process() throws Exception {
        Variant variant = new Variant(CONTIG, START, 1001, REFERENCE_ALLELE, ALTERNATE_ALLELE);
        ISubmittedVariant processed = processor.convert(variant);
        ISubmittedVariant expected = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, CONTIG, START, REFERENCE_ALLELE,
                                                          ALTERNATE_ALLELE, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE,
                                                          MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED, null);
        assertEquals(expected, processed);
        assertNull(processed.getCreatedDate());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionAssemblyNull() {
        SubmittedVariant submittedVariant = new SubmittedVariant(null, TAXONOMY, PROJECT, CONTIG, START,
                                                                 REFERENCE_ALLELE, ALTERNATE_ALLELE, CLUSTERED_VARIANT,
                                                                 SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY, ALLELES_MATCH,
                                                                 VALIDATED, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionProjectNull() {
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, null, CONTIG, START,
                                                                 REFERENCE_ALLELE, ALTERNATE_ALLELE, CLUSTERED_VARIANT,
                                                                 SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY, ALLELES_MATCH,
                                                                 VALIDATED, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionContigNull() {
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, null, START,
                                                                 REFERENCE_ALLELE, ALTERNATE_ALLELE, CLUSTERED_VARIANT,
                                                                 SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY, ALLELES_MATCH,
                                                                 VALIDATED, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionReferenceNull() {
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, CONTIG, START, null,
                                                                 ALTERNATE_ALLELE, CLUSTERED_VARIANT,
                                                                 SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY, ALLELES_MATCH,
                                                                 VALIDATED, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionAlternateNull() {
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, CONTIG, START,
                                                                 REFERENCE_ALLELE, null, CLUSTERED_VARIANT,
                                                                 SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY, ALLELES_MATCH,
                                                                 VALIDATED, null);
    }
}

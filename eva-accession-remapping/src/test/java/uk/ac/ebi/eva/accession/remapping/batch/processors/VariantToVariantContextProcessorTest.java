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

package uk.ac.ebi.eva.accession.remapping.batch.processors;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class VariantToVariantContextProcessorTest {

    private static final String GENBANK_ACCESSION_1 = "CM0001.1";

    private static final Long SS_ID = 123L;

    private static final Long RS_ID = 456L;

    private static final String STUDY_1 = "study_1";

    private static final String PROJECT_ID_KEY = "PROJECT";

    private SubmittedVariantToVariantContextProcessor variantConverter;


    @Before
    public void setUp() {
        variantConverter = new SubmittedVariantToVariantContextProcessor();
    }

    @Test
    public void SNV() {
        SubmittedVariantEntity variant = buildVariant(GENBANK_ACCESSION_1, 1000, "C", "A", STUDY_1);
        VariantContext variantContext = variantConverter.process(variant);
        assertVariantContext(variantContext, SS_ID, GENBANK_ACCESSION_1, 1000, "C", "A", STUDY_1);
    }

    private SubmittedVariantEntity buildVariant(String chr1, int start, String reference, String alternate,
                                                String project) {
        SubmittedVariantEntity variant = new SubmittedVariantEntity(SS_ID, "hash", "assembly", 9999, project, chr1,
                                                                    start, reference, alternate, RS_ID,
                                                                    false, false, false, false, 1);
        return variant;
    }

    private void assertVariantContext(VariantContext variantContext, Long expectedId, String expectedChromosome,
                                      int expectedStart,
                                      String expectedReference,
                                      String expectedAlternate,
                                      String expectedStudy) {
        assertEquals(expectedChromosome, variantContext.getContig());
        assertEquals(expectedStart, variantContext.getStart());
        assertEquals(Allele.create(expectedReference, true), variantContext.getReference());
        assertEquals(Collections.singletonList(Allele.create(expectedAlternate, false)),
                     variantContext.getAlternateAlleles());
        assertEquals("ss" + expectedId, variantContext.getID());
        assertTrue(variantContext.getFilters().isEmpty());
        assertEquals(2, variantContext.getCommonInfo().getAttributes().size());

        assertTrue(variantContext.getCommonInfo().hasAttribute(PROJECT_ID_KEY));
        assertEquals(expectedStudy, variantContext.getCommonInfo().getAttribute(PROJECT_ID_KEY));

        assertEquals(0, variantContext.getSampleNames().size());
    }

    @Test
    public void throwsIfAllelesAreEmpty() {
        SubmittedVariantEntity variant = buildVariant(GENBANK_ACCESSION_1, 1100, "", "G", STUDY_1);
        assertThrows(IllegalArgumentException.class, () -> variantConverter.process(variant));
    }

    @Test
    public void singleNucleotideInsertion() {
        SubmittedVariantEntity variant = buildVariant(GENBANK_ACCESSION_1, 1100, "T", "TG", STUDY_1);
        VariantContext variantContext = variantConverter.process(variant);
        assertVariantContext(variantContext, SS_ID, GENBANK_ACCESSION_1, 1100, "T", "TG", STUDY_1);
    }

    @Test
    public void severalNucleotidesInsertion() {
        SubmittedVariantEntity variant = buildVariant(GENBANK_ACCESSION_1, 1100, "T", "TGA", STUDY_1);
        VariantContext variantContext = variantConverter.process(variant);
        assertVariantContext(variantContext, SS_ID, GENBANK_ACCESSION_1, 1100, "T", "TGA", STUDY_1);
    }

    @Test
    public void singleNucleotideDeletion() {
        SubmittedVariantEntity variant = buildVariant(GENBANK_ACCESSION_1, 1100, "TA", "T", STUDY_1);
        VariantContext variantContext = variantConverter.process(variant);
        assertVariantContext(variantContext, SS_ID, GENBANK_ACCESSION_1, 1100, "TA", "T", STUDY_1);
    }

    @Test
    public void severalNucleotidesDeletion() {
        SubmittedVariantEntity variant = buildVariant(GENBANK_ACCESSION_1, 1100, "TAG", "T", STUDY_1);
        VariantContext variantContext = variantConverter.process(variant);
        assertVariantContext(variantContext, SS_ID, GENBANK_ACCESSION_1, 1100, "TAG", "T", STUDY_1);
    }

    @Test
    public void multiAllelicVariant() {
        SubmittedVariantEntity variant = buildVariant(GENBANK_ACCESSION_1, 1100, "C", "A,T", STUDY_1);
        assertThrows(IllegalArgumentException.class, () -> variantConverter.process(variant));
    }

    @Test
    public void singleNucleotideInsertionInPosition1() {
        SubmittedVariantEntity variant = buildVariant(GENBANK_ACCESSION_1, 1, "A", "TA", STUDY_1);
        VariantContext variantContext = variantConverter.process(variant);
        assertVariantContext(variantContext, SS_ID, GENBANK_ACCESSION_1, 1, "A", "TA", STUDY_1);
    }

    @Test
    public void singleNucleotideDeletionInPosition1() {
        SubmittedVariantEntity variant = buildVariant(GENBANK_ACCESSION_1, 1, "AT", "T", STUDY_1);
        VariantContext variantContext = variantConverter.process(variant);
        assertVariantContext(variantContext, SS_ID, GENBANK_ACCESSION_1, 1, "AT", "T", STUDY_1);
    }


    @Test
    public void severalNucleotidesInsertionInPosition1() {
        SubmittedVariantEntity variant = buildVariant(GENBANK_ACCESSION_1, 1, "A", "GGTA", STUDY_1);
        VariantContext variantContext = variantConverter.process(variant);
        assertVariantContext(variantContext, SS_ID, GENBANK_ACCESSION_1, 1, "A", "GGTA", STUDY_1);
    }

    @Test
    public void severalNucleotidesDeletionInPosition1() {
        SubmittedVariantEntity variant = buildVariant(GENBANK_ACCESSION_1, 1, "ATTG", "G", STUDY_1);
        VariantContext variantContext = variantConverter.process(variant);
        assertVariantContext(variantContext, SS_ID, GENBANK_ACCESSION_1, 1, "ATTG", "G", STUDY_1);
    }

}

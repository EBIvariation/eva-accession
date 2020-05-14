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

package uk.ac.ebi.eva.accession.remapping.batch.processors;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ExcludeInvalidVariantsProcessorTest {

    private static final String VALID_ALLELE = "A";

    private static final String ALL_VALID_LETTERS_ALLELE = "ACGTN";

    private static final String ALL_VALID_LETTERS_LOWER_CASE_ALLELE = "acgtn";

    private static final String ALL_VALID_LETTERS_LOWER_AND_UPPER_CASE_ALLELE = "acgtnACGTN";

    private static final String INVALID_LETTERS_ALLELE = "ACGTNF";

    private static final String SPECIAL_CHARACTERS_ALLELE = "A/TT,C.";

    private static final String NAMED_ALLELE = "<1000_BP_DEL>";

    private static final String SPACES_ALLELE = "A CGTN";

    private static final String EMPTY_ALLELE = "";

    private static ExcludeInvalidVariantsProcessor processor;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setUp() {
        processor = new ExcludeInvalidVariantsProcessor();
    }

    @Test
    public void referenceAlleleValid() throws Exception {
        Variant variant = newVariant(ALL_VALID_LETTERS_ALLELE, VALID_ALLELE);
        assertEquals(variant, processor.process(variant));
    }

    private Variant newVariant(String reference, String alternate) {
        return new Variant("contig", 1000, 1001, reference, alternate);
    }

    @Test
    public void alternateAlleleValid() throws Exception {
        Variant variant = newVariant(VALID_ALLELE, ALL_VALID_LETTERS_ALLELE);
        assertEquals(variant, processor.process(variant));
    }

    @Test
    public void referenceAlleleLowerCaseValid() throws Exception {
        Variant variant = newVariant(ALL_VALID_LETTERS_LOWER_CASE_ALLELE, VALID_ALLELE);
        assertEquals(variant, processor.process(variant));
    }

    @Test
    public void alternateAlleleLowerCaseValid() throws Exception {
        Variant variant = newVariant(VALID_ALLELE, ALL_VALID_LETTERS_LOWER_CASE_ALLELE);
        assertEquals(variant, processor.process(variant));
    }

    @Test
    public void referenceAlleleLowerAndUpperCaseValid() throws Exception {
        Variant variant = newVariant(ALL_VALID_LETTERS_LOWER_AND_UPPER_CASE_ALLELE, VALID_ALLELE);
        assertEquals(variant, processor.process(variant));
    }

    @Test
    public void alternateAlleleLowerAndUpperCaseValid() throws Exception {
        Variant variant = newVariant(VALID_ALLELE, ALL_VALID_LETTERS_LOWER_AND_UPPER_CASE_ALLELE);
        assertEquals(variant, processor.process(variant));
    }

    @Test
    public void referenceAlleleInvalidLetters() throws Exception {
        Variant variant = newVariant(INVALID_LETTERS_ALLELE, VALID_ALLELE);
        assertNull(processor.process(variant));
    }

    @Test
    public void alternateAlleleInvalidLetters() throws Exception {
        Variant variant = newVariant(VALID_ALLELE, INVALID_LETTERS_ALLELE);
        assertNull(processor.process(variant));
    }

    @Test
    public void referenceAlleleSpecialCharacters() throws Exception {
        Variant variant = newVariant(SPECIAL_CHARACTERS_ALLELE, VALID_ALLELE);
        assertNull(processor.process(variant));
    }

    @Test
    public void alternateAlleleSpecialCharacters() throws Exception {
        Variant variant = newVariant(VALID_ALLELE, SPECIAL_CHARACTERS_ALLELE);
        assertNull(processor.process(variant));
    }

    @Test
    public void referenceAlleleNamed() throws Exception {
        Variant variant = newVariant(NAMED_ALLELE, VALID_ALLELE);
        assertNotNull(processor.process(variant));
    }

    @Test
    public void alternateAlleleNamed() throws Exception {
        Variant variant = newVariant(VALID_ALLELE, NAMED_ALLELE);
        assertNotNull(processor.process(variant));
    }

    @Test
    public void referenceAlleleSpaces() throws Exception {
        Variant variant = newVariant(SPACES_ALLELE, VALID_ALLELE);
        assertNull(processor.process(variant));
    }

    @Test
    public void alternateAlleleSpaces() throws Exception {
        Variant variant = newVariant(VALID_ALLELE, SPACES_ALLELE);
        assertNull(processor.process(variant));
    }

    @Test
    public void referenceAlleleEmpty() throws Exception {
        Variant variant = newVariant(EMPTY_ALLELE, VALID_ALLELE);
        assertEquals(variant, processor.process(variant));
    }

    @Test
    public void alternateAlleleEmpty() throws Exception {
        Variant variant = newVariant(VALID_ALLELE, EMPTY_ALLELE);
        assertEquals(variant, processor.process(variant));
    }
}
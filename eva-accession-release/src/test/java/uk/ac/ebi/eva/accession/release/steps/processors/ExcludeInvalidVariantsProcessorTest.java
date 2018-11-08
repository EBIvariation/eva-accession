package uk.ac.ebi.eva.accession.release.steps.processors;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import uk.ac.ebi.eva.commons.core.models.IVariant;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static uk.ac.ebi.eva.accession.release.steps.processors.ExcludeInvalidVariantsProcessor.REFERENCE_AND_ALTERNATE_ALLELES_CANNOT_BE_EMPTY;

public class ExcludeInvalidVariantsProcessorTest {

    private static final String VALID_ALLELE = "A";

    private static final String ALL_VALID_LETTERS_ALLELE = "ACGTN";

    private static final String ALL_VALID_LETTERS_LOWER_CASE_ALLELE = "acgtn";

    private static final String ALL_VALID_LETTERS_LOWER_AND_UPPER_CASE_ALLELE = "acgtnACGTN";

    private static final String INVALID_LETTERS_ALLELE = "ACGTNF";

    private static final String SPECIAL_CHARACTERS_ALLELE = "A/TT,C.";

    private static final String NAMED_ALLELE = "(1000 BP DEL)";

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
        assertNull(processor.process(variant));
    }

    @Test
    public void alternateAlleleNamed() throws Exception {
        Variant variant = newVariant(VALID_ALLELE, NAMED_ALLELE);
        assertNull(processor.process(variant));
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
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(REFERENCE_AND_ALTERNATE_ALLELES_CANNOT_BE_EMPTY);
        processor.process(variant);
    }

    @Test
    public void alternateAlleleEmpty() throws Exception {
        Variant variant = newVariant(VALID_ALLELE, EMPTY_ALLELE);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(REFERENCE_AND_ALTERNATE_ALLELES_CANNOT_BE_EMPTY);
        processor.process(variant);
    }
}
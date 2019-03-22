/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
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

package uk.ac.ebi.eva.accession.release.steps.processors;

import org.junit.Test;

import uk.ac.ebi.eva.commons.core.models.IVariant;
import uk.ac.ebi.eva.commons.core.models.VariantType;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import static org.junit.Assert.*;
import static uk.ac.ebi.eva.accession.release.io.VariantMongoAggregationReader.ALLELES_MATCH_KEY;
import static uk.ac.ebi.eva.accession.release.io.VariantMongoAggregationReader.ASSEMBLY_MATCH_KEY;
import static uk.ac.ebi.eva.accession.release.io.VariantMongoAggregationReader.CLUSTERED_VARIANT_VALIDATED_KEY;
import static uk.ac.ebi.eva.accession.release.io.VariantMongoAggregationReader.STUDY_ID_KEY;
import static uk.ac.ebi.eva.accession.release.io.VariantMongoAggregationReader.SUBMITTED_VARIANT_VALIDATED_KEY;
import static uk.ac.ebi.eva.accession.release.io.VariantMongoAggregationReader.SUPPORTED_BY_EVIDENCE_KEY;
import static uk.ac.ebi.eva.accession.release.io.VariantMongoAggregationReader.VARIANT_CLASS_KEY;

public class NamedVariantProcessorTest {

    private static final String ID = "rs123";

    private static final String CHR_1 = "chr1";

    private static final String STUDY_1 = "study_1";

    private static final String FILE_ID = "fileId";

    private static final String REGULAR_ALLELE_REGEX = "[ACTGNactgn]*";

    // inner string without space nor angle brackets, surrounded by angle brackets
    private static final String SYMBOLIC_ALLELE_REGEX = "<[^<> ]+>";

    private NamedVariantProcessor processor = new NamedVariantProcessor();

    @Test
    public void processNamedInsertion() throws Exception {
        assertNamedVariant("A", "(1190 BP INS)");
    }

    private void assertNamedVariant(String reference, String alternate) throws Exception {
        Variant variant = buildVariant(CHR_1, 1000, reference, alternate, VariantType.SEQUENCE_ALTERATION.toString(),
                                       STUDY_1);

        IVariant processed = processor.process(variant);

        assertTrue(processed.getReference().matches(REGULAR_ALLELE_REGEX));
        assertTrue(processed.getAlternate().matches(SYMBOLIC_ALLELE_REGEX));
    }

    private Variant buildVariant(String chr, int start, String reference, String alternate,
                                 String sequenceOntologyTerm, String... studies) {
        return buildVariant(chr, start, reference, alternate, sequenceOntologyTerm, false, false, true, true, true,
                            studies);
    }

    private Variant buildVariant(String chr, int start, String reference, String alternate,
                                 String sequenceOntologyTerm, boolean validated, boolean submittedVariantValidated,
                                 boolean allelesMatch, boolean assemblyMatch, boolean evidence, String... studies) {
        Variant variant = new Variant(chr, start, start + alternate.length(), reference, alternate);
        variant.setMainId(ID);
        for (String study : studies) {
            VariantSourceEntry sourceEntry = new VariantSourceEntry(study, FILE_ID);
            sourceEntry.addAttribute(VARIANT_CLASS_KEY, sequenceOntologyTerm);
            sourceEntry.addAttribute(STUDY_ID_KEY, study);
            sourceEntry.addAttribute(CLUSTERED_VARIANT_VALIDATED_KEY, Boolean.toString(validated));
            sourceEntry.addAttribute(SUBMITTED_VARIANT_VALIDATED_KEY, Boolean.toString(submittedVariantValidated));
            sourceEntry.addAttribute(ALLELES_MATCH_KEY, Boolean.toString(allelesMatch));
            sourceEntry.addAttribute(ASSEMBLY_MATCH_KEY, Boolean.toString(assemblyMatch));
            sourceEntry.addAttribute(SUPPORTED_BY_EVIDENCE_KEY, Boolean.toString(evidence));
            variant.addSourceEntry(sourceEntry);
        }
        return variant;
    }

    @Test
    public void processNamedInsertionWithEmptyReference() throws Exception {
        assertNamedVariant("A", "(1190 BP INS)");
    }

    @Test
    public void processNamedDeletion() throws Exception {
        assertNamedVariant("(1190 BP DEL)", "A");
    }

    @Test
    public void processNamedDeletionWithEmptyAlternate() throws Exception {
        assertNamedVariant("(1190 BP DEL)", "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwIfBothAllelesAreNamed() throws Exception {
        assertNamedVariant("(1190 BP DEL)", "(1190 BP DEL)");
    }

    @Test
    public void keepSnvUnmodified() throws Exception {
        assertUnmodifiedAlleles("A", "T");
    }

    private void assertUnmodifiedAlleles(String reference, String alternate) throws Exception {
        Variant variant = buildVariant(CHR_1, 1000, reference, alternate, VariantType.SEQUENCE_ALTERATION.toString(),
                                       STUDY_1);

        IVariant processed = processor.process(variant);

        assertEquals(reference, processed.getReference());
        assertEquals(alternate, processed.getAlternate());
    }

    @Test
    public void keepInsertionsUnmodified() throws Exception {
        assertUnmodifiedAlleles("A", "AGC");
        assertUnmodifiedAlleles("", "AGC");
    }

    @Test
    public void keepDeletionsUnmodified() throws Exception {
        assertUnmodifiedAlleles("AGC", "A");
        assertUnmodifiedAlleles("AGC", "");
    }

    @Test
    public void keepSymbolicAlternateAllelesUnmodified() throws Exception {
        assertUnmodifiedAlleles("A", "<INS>");
        assertUnmodifiedAlleles("", "<INS>");
    }

    @Test
    public void swapSymbolicReference() throws Exception {
        assertNamedVariant("<DEL>", "A");
        assertNamedVariant("<DEL>", "");
    }
}

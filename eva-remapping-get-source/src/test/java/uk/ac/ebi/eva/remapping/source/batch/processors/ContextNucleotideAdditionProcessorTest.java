/*
 *
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
 *
 */
package uk.ac.ebi.eva.remapping.source.batch.processors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.ebi.eva.accession.core.batch.io.FastaSynonymSequenceReader;
import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.contig.ContigSynonyms;
import uk.ac.ebi.eva.accession.core.exceptions.PositionOutsideOfContigException;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

public class ContextNucleotideAdditionProcessorTest {

    private static final String CONTIG = "22";

    private static final String SCAFFOLD = "scaffold";

    private static final String SCAFFOLD_GENBANK_IN_FASTA = "AADN04000814.1";

    private static final String MISSING_IN_FASTA = "chrMissing";

    private static final int START_OUTSIDE_CHOMOSOME = 5000000;

    private static FastaSynonymSequenceReader fastaSynonymSequenceReader;

    private static ContextNucleotideAdditionProcessor contextNucleotideAdditionProcessor;

    @BeforeClass
    public static void setUpClass() throws Exception {
        Path fastaPath = Paths.get("../eva-accession-core/src/test/resources/input-files/fasta/Gallus_gallus-5.0.test.fa");
        ContigMapping contigMapping = new ContigMapping(
                Arrays.asList(new ContigSynonyms(CONTIG, "", "", "", "", "", true),
                              new ContigSynonyms(SCAFFOLD, "", "", SCAFFOLD_GENBANK_IN_FASTA, "", "", true),
                              new ContigSynonyms(MISSING_IN_FASTA, "", "", "", "", "", true)));
        fastaSynonymSequenceReader = new FastaSynonymSequenceReader(contigMapping, fastaPath);
        contextNucleotideAdditionProcessor = new ContextNucleotideAdditionProcessor(fastaSynonymSequenceReader);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        fastaSynonymSequenceReader.close();
    }

    @Test
    public void testNonEmptyAlleles() throws Exception {
        SubmittedVariantEntity variant = createVariant(CONTIG, 1, "T", "C");
        SubmittedVariantEntity processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(1, processedVariant.getStart());
        assertEquals("T", processedVariant.getReferenceAllele());
        assertEquals("C", processedVariant.getAlternateAllele());

        variant = createVariant(CONTIG, 2, "G", "A");
        processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(2, processedVariant.getStart());
        assertEquals("G", processedVariant.getReferenceAllele());
        assertEquals("A", processedVariant.getAlternateAllele());

        variant = createVariant(CONTIG, 2, "G", "AC");
        processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(2, processedVariant.getStart());
        assertEquals("G", processedVariant.getReferenceAllele());
        assertEquals("AC", processedVariant.getAlternateAllele());

        variant = createVariant(CONTIG, 2, "AC", "G");
        processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(2, processedVariant.getStart());
        assertEquals("AC", processedVariant.getReferenceAllele());
        assertEquals("G", processedVariant.getAlternateAllele());
    }

    private SubmittedVariantEntity createVariant(String chromosome, long start, String reference,
                                                 String alternate) {
        return new SubmittedVariantEntity(1L, "hash", "GCA_x", 9999, "project", chromosome, start, reference, alternate,
                                          100L, false, false, false, false, 1);
    }
    @Test
    public void testINDELStartPos1() throws Exception {
        SubmittedVariantEntity variant = createVariant(CONTIG, 1, "", "A");
        SubmittedVariantEntity processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(1, processedVariant.getStart());
        assertEquals("T", processedVariant.getReferenceAllele());
        assertEquals("AT", processedVariant.getAlternateAllele());

        variant = createVariant(CONTIG, 1, "T", "");
        processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(1, processedVariant.getStart());
        assertEquals("TG", processedVariant.getReferenceAllele());
        assertEquals("G", processedVariant.getAlternateAllele());

        variant = createVariant(CONTIG, 1, "", "CA");
        processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(1, processedVariant.getStart());
        assertEquals("T", processedVariant.getReferenceAllele());
        assertEquals("CAT", processedVariant.getAlternateAllele());

        variant = createVariant(CONTIG, 1, "TG", "");
        processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(1, processedVariant.getStart());
        assertEquals("TGC", processedVariant.getReferenceAllele());
        assertEquals("C", processedVariant.getAlternateAllele());
    }

    @Test
    public void testINDELStartPosNot1() throws Exception {
        SubmittedVariantEntity variant = createVariant(CONTIG, 2, "", "A");
        SubmittedVariantEntity processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(1, processedVariant.getStart());
        assertEquals("T", processedVariant.getReferenceAllele());
        assertEquals("TA", processedVariant.getAlternateAllele());

        variant = createVariant(CONTIG, 2, "G", "");
        processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(1, processedVariant.getStart());
        assertEquals("TG", processedVariant.getReferenceAllele());
        assertEquals("T", processedVariant.getAlternateAllele());

        variant = createVariant(CONTIG, 2, "", "CA");
        processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(1, processedVariant.getStart());
        assertEquals("T", processedVariant.getReferenceAllele());
        assertEquals("TCA", processedVariant.getAlternateAllele());

        variant = createVariant(CONTIG, 2, "GC", "");
        processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(1, processedVariant.getStart());
        assertEquals("TGC", processedVariant.getReferenceAllele());
        assertEquals("T", processedVariant.getAlternateAllele());
    }

    @Test
    public void testNamedVariants() {
        SubmittedVariantEntity variant1 = createVariant(CONTIG, 2, "", "(100_BP_insertion)");
        assertThrows(IllegalStateException.class, () -> contextNucleotideAdditionProcessor.process(variant1));

        SubmittedVariantEntity variant2 = createVariant(CONTIG, 2, "A", "(100_BP_insertion)");
        assertThrows(IllegalStateException.class, () -> contextNucleotideAdditionProcessor.process(variant2));
    }

    @Test
    public void testStartPositionGreaterThanChromosomeEnd() {
        SubmittedVariantEntity variant1 = createVariant(CONTIG, START_OUTSIDE_CHOMOSOME, "", "A");
        assertThrows(PositionOutsideOfContigException.class, () -> {
            contextNucleotideAdditionProcessor.process(variant1);
        });
    }

    @Test
    public void addContextBaseUsingSynonymContig() throws Exception {
        SubmittedVariantEntity variant = createVariant(SCAFFOLD, 7, "", "A");
        SubmittedVariantEntity processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(6, processedVariant.getStart());
        assertEquals("T", processedVariant.getReferenceAllele());
        assertEquals("TA", processedVariant.getAlternateAllele());
    }

    @Test(expected = IllegalArgumentException.class)
    public void contigNotFound() throws Exception {
        SubmittedVariantEntity variant1 = createVariant(MISSING_IN_FASTA, 10, "", "A");
        try {
            contextNucleotideAdditionProcessor.process(variant1);
        } catch (PositionOutsideOfContigException wrongException) {
            fail("The exception (" + wrongException.getClass().getSimpleName()
                 + ") is wrong because the variant doesn't have a position outside of chromosome. The correct "
                 + "exception should be that the contig is not present in the fasta");
        }
    }
}

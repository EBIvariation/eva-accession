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
package uk.ac.ebi.eva.accession.release.batch.processors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.contig.ContigSynonyms;
import uk.ac.ebi.eva.accession.core.batch.io.FastaSynonymSequenceReader;
import uk.ac.ebi.eva.accession.core.exceptions.PositionOutsideOfContigException;
import uk.ac.ebi.eva.commons.core.models.IVariant;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
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
        Variant variant = new Variant(CONTIG, 1, 1, "T", "C");
        IVariant processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(1, processedVariant.getStart());
        assertEquals(1, processedVariant.getEnd());
        assertEquals("T", processedVariant.getReference());
        assertEquals("C", processedVariant.getAlternate());

        variant = new Variant(CONTIG, 2, 2, "G", "A");
        processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(2, processedVariant.getStart());
        assertEquals(2, processedVariant.getEnd());
        assertEquals("G", processedVariant.getReference());
        assertEquals("A", processedVariant.getAlternate());

        variant = new Variant(CONTIG, 2, 3, "G", "AC");
        processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(2, processedVariant.getStart());
        assertEquals(3, processedVariant.getEnd());
        assertEquals("G", processedVariant.getReference());
        assertEquals("AC", processedVariant.getAlternate());

        variant = new Variant(CONTIG, 2, 3, "AC", "G");
        processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(2, processedVariant.getStart());
        assertEquals(3, processedVariant.getEnd());
        assertEquals("AC", processedVariant.getReference());
        assertEquals("G", processedVariant.getAlternate());
    }

    @Test
    public void testINDELStartPos1() throws Exception {
        Variant variant = new Variant(CONTIG, 1, 1, "", "A");
        IVariant processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(1, processedVariant.getStart());
        assertEquals(2, processedVariant.getEnd());
        assertEquals("T", processedVariant.getReference());
        assertEquals("AT", processedVariant.getAlternate());

        variant = new Variant(CONTIG, 1, 1, "T", "");
        processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(1, processedVariant.getStart());
        assertEquals(2, processedVariant.getEnd());
        assertEquals("TG", processedVariant.getReference());
        assertEquals("G", processedVariant.getAlternate());

        variant = new Variant(CONTIG, 1, 2, "", "CA");
        processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(1, processedVariant.getStart());
        assertEquals(3, processedVariant.getEnd());
        assertEquals("T", processedVariant.getReference());
        assertEquals("CAT", processedVariant.getAlternate());

        variant = new Variant(CONTIG, 1, 2, "TG", "");
        processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(1, processedVariant.getStart());
        assertEquals(3, processedVariant.getEnd());
        assertEquals("TGC", processedVariant.getReference());
        assertEquals("C", processedVariant.getAlternate());
    }

    @Test
    public void testINDELStartPosNot1() throws Exception {
        Variant variant = new Variant(CONTIG, 2, 2, "", "A");
        IVariant processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(1, processedVariant.getStart());
        assertEquals(2, processedVariant.getEnd());
        assertEquals("T", processedVariant.getReference());
        assertEquals("TA", processedVariant.getAlternate());

        variant = new Variant(CONTIG, 2, 2, "G", "");
        processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(1, processedVariant.getStart());
        assertEquals(2, processedVariant.getEnd());
        assertEquals("TG", processedVariant.getReference());
        assertEquals("T", processedVariant.getAlternate());

        variant = new Variant(CONTIG, 2, 3, "", "CA");
        processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(1, processedVariant.getStart());
        assertEquals(3, processedVariant.getEnd());
        assertEquals("T", processedVariant.getReference());
        assertEquals("TCA", processedVariant.getAlternate());

        variant = new Variant(CONTIG, 2, 3, "GC", "");
        processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(1, processedVariant.getStart());
        assertEquals(3, processedVariant.getEnd());
        assertEquals("TGC", processedVariant.getReference());
        assertEquals("T", processedVariant.getAlternate());
    }

    @Test
    public void testNamedVariants() throws Exception {
        Variant variant = new Variant(CONTIG, 2, 2, "", "<100_BP_insertion>");
        IVariant processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(1, processedVariant.getStart());
        assertEquals(1, processedVariant.getEnd());
        assertEquals("T", processedVariant.getReference());
        assertEquals("<100_BP_insertion>", processedVariant.getAlternate());

        variant = new Variant(CONTIG, 2, 2, "A", "<100_BP_insertion>");
        processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(2, processedVariant.getStart());
        assertEquals(2, processedVariant.getEnd());
        assertEquals("A", processedVariant.getReference());
        assertEquals("<100_BP_insertion>", processedVariant.getAlternate());
    }

    @Test(expected = PositionOutsideOfContigException.class)
    public void testStartPositionGreaterThanChromosomeEnd() throws Exception {
        Variant variant1 = new Variant(CONTIG, START_OUTSIDE_CHOMOSOME, START_OUTSIDE_CHOMOSOME, "", "A");
        String rs1000 = "rs1000";
        variant1.setMainId(rs1000);
        variant1.setIds(Collections.singleton(rs1000));
        contextNucleotideAdditionProcessor.process(variant1);
    }

    @Test
    public void addContextBaseUsingSynonymContig() throws Exception {
        Variant variant = new Variant(SCAFFOLD, 7, 8, "", "A");
        IVariant processedVariant = contextNucleotideAdditionProcessor.process(variant);
        assertEquals(6, processedVariant.getStart());
        assertEquals(7, processedVariant.getEnd());
        assertEquals("T", processedVariant.getReference());
        assertEquals("TA", processedVariant.getAlternate());
    }

    @Test(expected = IllegalArgumentException.class)
    public void contigNotFound() throws Exception {
        Variant variant1 = new Variant(MISSING_IN_FASTA, 10, 10, "", "A");
        try {
            contextNucleotideAdditionProcessor.process(variant1);
        } catch (PositionOutsideOfContigException wrongException) {
            fail("The exception (" + wrongException.getClass().getSimpleName()
                 + ") is wrong because the variant doesn't have a position outside of chromosome. The correct "
                 + "exception should be that the contig is not present in the fasta");
        }
    }
}

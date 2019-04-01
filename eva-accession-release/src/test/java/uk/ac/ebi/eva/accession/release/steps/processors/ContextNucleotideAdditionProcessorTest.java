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
package uk.ac.ebi.eva.accession.release.steps.processors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.contig.ContigSynonyms;
import uk.ac.ebi.eva.accession.core.io.FastaSynonymSequenceReader;
import uk.ac.ebi.eva.commons.core.models.IVariant;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class ContextNucleotideAdditionProcessorTest {

    private static final String CONTIG = "22";
    private static FastaSynonymSequenceReader fastaSynonymSequenceReader;
    private static ContextNucleotideAdditionProcessor contextNucleotideAdditionProcessor;

    @BeforeClass
    public static void setUpClass() throws Exception {
        Path fastaPath = Paths.get("../eva-accession-core/src/test/resources/input-files/fasta/Gallus_gallus-5.0.test.fa");
        ContigMapping contigMapping = new ContigMapping(Collections.singletonList(
                new ContigSynonyms(CONTIG, "", "", "", "", "", true)));
        fastaSynonymSequenceReader = new FastaSynonymSequenceReader(contigMapping, fastaPath);
        contextNucleotideAdditionProcessor = new ContextNucleotideAdditionProcessor (fastaSynonymSequenceReader);
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
}

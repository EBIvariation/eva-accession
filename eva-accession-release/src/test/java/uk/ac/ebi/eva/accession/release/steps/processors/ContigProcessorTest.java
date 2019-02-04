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

import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.ebi.eva.accession.core.contig.ContigMapping;

import static org.junit.Assert.assertEquals;

public class ContigProcessorTest {

    private static final String GENBANK_CONTIG = "CM001941.2";

    private static final String REFSEQ_CONTIG = "NC_023642.1";

    private static final String CONTIG_NO_SYNONYM = "CONTIG";

    private static ContigProcessor processor;

    @BeforeClass
    public static void setUp() throws Exception {
        String fileString = ContigProcessorTest.class.getResource(
                "/input-files/assembly_report/GCF_000409795.2_Chlorocebus_sabeus_1.1_assembly_report.txt").toString();
        ContigMapping contigMapping = new ContigMapping(fileString);
        processor = new ContigProcessor(contigMapping);
    }

    @Test
    public void processGenBankContig() throws Exception {
        String contig = processor.process(GENBANK_CONTIG);
        assertEquals(GENBANK_CONTIG, contig);
    }

    @Test
    public void processGenBankContigWithRefSeqSynonym() throws Exception {
        String contig = processor.process(REFSEQ_CONTIG);
        assertEquals(GENBANK_CONTIG, contig);
    }

    @Test
    public void processContigWithoutGenBankSynonym() throws Exception {
        String contig = processor.process(CONTIG_NO_SYNONYM);
        assertEquals(CONTIG_NO_SYNONYM, contig);
    }
}
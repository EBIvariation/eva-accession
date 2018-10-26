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
package uk.ac.ebi.eva.accession.dbsnp.processors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.dbsnp.model.DbsnpVariantType;
import uk.ac.ebi.eva.accession.dbsnp.model.Orientation;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;

import static org.junit.Assert.assertEquals;

public class ContigReplacerProcessorTest {

    private static final String ASSEMBLY = "assembly";

    private static final int TAXONOMY = 1111;

    private static final long START = 5;

    private static final String REFERENCE_ALLELE = "C";

    private static final String ALTERNATE_ALLELE = "T";

    private static final String CHROMOSOME = "1";

    private static final String MISSING_CHROMOSOME = "23";

    private static final String SEQNAME_1 = "22";

    private static final String SEQNAME_2 = "chrom1";

    private static final String GENBANK_CONTIG = "CM000994.2";

    private static final String GENBANK_1 = "genbank_example_1";

    private static final String REFSEQ_1 = "refseq_example_1";

    private static final String MISSING_CONTIG = "NT_missing";

    private static final String UCSC_1 = "ucsc_example_1";

    private static final Long SS_ID = 12345L;

    private static final Long RS_ID = 56789L;

    private static final String REFSEQ_WITHOUT_SYNONYM = "NT_without_synonym";

    private static final String GENBANK_WITHOUT_SYNONYM = "GL_without_synonym";

    private static final String SEQNAME_WITHOUT_SYNONYM = "CHR_without_synonym";

    private static final String REFSEQ_ASSEMBLY_ACCESSION = "GCF_000001635.26";

    private static final String GENBANK_ASSEMBLY_ACCESSION = "GCA_000001635.8";

    private ContigReplacerProcessor refseqProcessor;

    private ContigReplacerProcessor genbankProcessor;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        String fileString = ContigReplacerProcessorTest.class.getResource(
                "/input-files/assembly-report/AssemblyReport.txt").toString();
        ContigMapping contigMapping = new ContigMapping(fileString);

        refseqProcessor = new ContigReplacerProcessor(contigMapping, REFSEQ_ASSEMBLY_ACCESSION);
        genbankProcessor = new ContigReplacerProcessor(contigMapping, GENBANK_ASSEMBLY_ACCESSION);
    }

    // test special cases (combinations of "missing in assembly report")

    @Test
    public void shouldThrowIfContigAndChromosomeAreMissingInAssemblyReport() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(MISSING_CHROMOSOME, MISSING_CONTIG);
        thrown.expect(IllegalStateException.class);
        refseqProcessor.process(input);
    }

    @Test
    public void shouldThrowIfContigAndChromosomeAreFoundInDifferentLinesInAssemblyReport() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(SEQNAME_2, GENBANK_1);
        thrown.expect(IllegalStateException.class);
        refseqProcessor.process(input);
    }

    private SubSnpNoHgvs newMockSubSnpNoHgvs(String chromosome, String contig) {
        return new SubSnpNoHgvs(SS_ID, RS_ID, REFERENCE_ALLELE, ALTERNATE_ALLELE, ASSEMBLY, "", "",
                                chromosome, START, contig, START, DbsnpVariantType.SNV,
                                Orientation.FORWARD, Orientation.FORWARD, Orientation.FORWARD, true, true,
                                true, true, null, null, TAXONOMY);
    }

    @Test
    public void convertChromosomeIfContigIsMissingInAssemblyReport() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(CHROMOSOME, MISSING_CONTIG);
        assertEquals(GENBANK_CONTIG, refseqProcessor.process(input).getContigName());
    }

    @Test
    public void convertContigIfChromosomeIsMissingInAssemblyReport() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(MISSING_CHROMOSOME, REFSEQ_1);
        assertEquals(GENBANK_1, refseqProcessor.process(input).getContigName());
    }

    @Test
    public void doNotConvertToGenbankIfGenbankAndRefseqAreNotIdentical() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(SEQNAME_WITHOUT_SYNONYM, REFSEQ_WITHOUT_SYNONYM);
        assertEquals(REFSEQ_WITHOUT_SYNONYM, refseqProcessor.process(input).getContigName());
    }

    @Test
    public void doNotConvertChromosomeToGenbankIfGenbankAndRefseqAreNotIdentical() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(SEQNAME_WITHOUT_SYNONYM, MISSING_CONTIG);
        assertEquals(MISSING_CONTIG, refseqProcessor.process(input).getContigName());
    }

    @Test
    public void convertChromosomeToGenbankIfGenbankAndRefseqAreNotIdenticalAndAssemblyIsGenbank() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(SEQNAME_WITHOUT_SYNONYM, MISSING_CONTIG);
        assertEquals(GENBANK_WITHOUT_SYNONYM, genbankProcessor.process(input).getContigName());
    }

    @Test
    public void ifConvertingFromChromosomeThenChangePositionAsWell() throws Exception {
        long chromosomeStart = 100;
        long contigStart = 200;
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(CHROMOSOME, chromosomeStart, MISSING_CONTIG, contigStart);
        assertEquals(GENBANK_CONTIG, refseqProcessor.process(input).getContigName());
        assertEquals(chromosomeStart, refseqProcessor.process(input).getContigStart());
    }

    private SubSnpNoHgvs newMockSubSnpNoHgvs(String chromosome, long chromosomeStart, String contig, long contigStart) {
        return new SubSnpNoHgvs(SS_ID, RS_ID, REFERENCE_ALLELE, ALTERNATE_ALLELE, ASSEMBLY, "", "",
                                chromosome, chromosomeStart, contig, contigStart, DbsnpVariantType.SNV,
                                Orientation.FORWARD, Orientation.FORWARD, Orientation.FORWARD, true, true,
                                true, true, null, null, TAXONOMY);
    }

    // test regular cases

    @Test
    public void convertVariantFromSeqNameToGenBank() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(SEQNAME_1, SEQNAME_1);
        assertEquals(GENBANK_1, refseqProcessor.process(input).getContigName());
    }

    @Test
    public void convertVariantFromGenBankToGenBank() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(GENBANK_1, GENBANK_1);
        assertEquals(GENBANK_1, refseqProcessor.process(input).getContigName());
    }

    @Test
    public void convertVariantFromRefSeqToGenBank() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(REFSEQ_1, REFSEQ_1);
        assertEquals(GENBANK_1, refseqProcessor.process(input).getContigName());
    }

    @Test
    public void convertVariantFromUcscToGenBank() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(UCSC_1, UCSC_1);
        assertEquals(GENBANK_1, refseqProcessor.process(input).getContigName());
    }

    @Test
    public void prioritiseRefseqCoordinatesOverChromosome() throws Exception {
        long chromosomeStart = 100;
        long contigStart = 200;
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(SEQNAME_1, chromosomeStart, REFSEQ_1, contigStart);
        assertEquals(GENBANK_1, refseqProcessor.process(input).getContigName());
        assertEquals(contigStart, refseqProcessor.process(input).getContigStart());
    }

    @Test
    public void prioritiseGenbankCoordinatesOverChromosome() throws Exception {
        long chromosomeStart = 100;
        long contigStart = 200;
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(SEQNAME_1, chromosomeStart, GENBANK_1, contigStart);
        assertEquals(GENBANK_1, refseqProcessor.process(input).getContigName());
        assertEquals(contigStart, refseqProcessor.process(input).getContigStart());
    }
}

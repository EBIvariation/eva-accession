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

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.batch.io.FastaSynonymSequenceReader;
import uk.ac.ebi.eva.accession.dbsnp.model.DbsnpVariantType;
import uk.ac.ebi.eva.accession.dbsnp.model.Orientation;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AssemblyCheckerProcessorTest {

    private static final String ASSEMBLY = "assembly";

    private static final int TAXONOMY = 1111;

    private static final int START = 5;

    private static final String REFERENCE_ALLELE = "CC";

    private static final String REFERENCE_ALLELE_1 = "T";

    private static final String ALTERNATE_ALLELE = "T";

    private static final String GENBANK_1 = "genbank_example_1";

    private static final String REFSEQ_1 = "refseq_example_1";

    private static final String SEQNAME_1 = "22";

    private static final String UCSC_1 = "ucsc_example_1";

    private static final Long SS_ID = 12345L;

    private static final Long RS_ID = 56789L;

    private static final String SOFT_MASKED_FASTA_CONTIG = "NW_006738765.1";

    private AssemblyCheckerProcessor processorSeqName;

    private AssemblyCheckerProcessor processorGenBank;

    private AssemblyCheckerProcessor processorRefSeq;

    private AssemblyCheckerProcessor processorUcsc;

    private AssemblyCheckerProcessor processorSoftMasked;

    @Before
    public void setUp() throws Exception {
        String fileString = AssemblyCheckerProcessorTest.class.getResource(
                "/input-files/assembly-report/GCA_000001635.8_Mus_musculus-grcm38.p6_assembly_report.txt").toString();
        ContigMapping contigMapping = new ContigMapping(fileString);

        this.processorSeqName = getAssemblyCheckerProcessor(contigMapping, "Gallus_gallus-5.0.test.fa");
        this.processorGenBank = getAssemblyCheckerProcessor(contigMapping, "fasta.genbank.fa");
        this.processorRefSeq = getAssemblyCheckerProcessor(contigMapping, "fasta.refseq.fa");
        this.processorUcsc = getAssemblyCheckerProcessor(contigMapping, "fasta.ucsc.fa");
        this.processorSoftMasked = getAssemblyCheckerProcessor(contigMapping, "fastaWithSoftMasking.fa");
    }

    private AssemblyCheckerProcessor getAssemblyCheckerProcessor(ContigMapping contigMapping, String fastaFile)
            throws IOException {
        Path softMaskedPath = Paths.get("src/test/resources/input-files/fasta/" + fastaFile);
        return new AssemblyCheckerProcessor(new FastaSynonymSequenceReader(contigMapping, softMaskedPath));
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        Files.deleteIfExists(Paths.get("src/test/resources/input-files/fasta/fasta.genbank.fa.fai"));
        Files.deleteIfExists(Paths.get("src/test/resources/input-files/fasta/fasta.refseq.fa.fai"));
        Files.deleteIfExists(Paths.get("src/test/resources/input-files/fasta/fasta.ucsc.fa.fai"));
    }


    private SubSnpNoHgvs newSubSnpNoHgvs(String chromosome, long chromosomeStart, String contig, long contigStart,
                                         String referenceAllele, DbsnpVariantType variantClass) {
        return new SubSnpNoHgvs(SS_ID, RS_ID, referenceAllele, ALTERNATE_ALLELE, ASSEMBLY, "", "", chromosome,
                                chromosomeStart, contig, contigStart, variantClass, Orientation.FORWARD,
                                Orientation.FORWARD, Orientation.FORWARD, false, false, true, true, null, null,
                                TAXONOMY);
    }

    //SeqName Fasta

    @Test
    public void validReferenceAlleleSeqNameFastaSeqName() throws Exception {
        SubSnpNoHgvs input = newSubSnpNoHgvs(SEQNAME_1, START, SEQNAME_1, START, REFERENCE_ALLELE, DbsnpVariantType.DIV);
        assertTrue(processorSeqName.process(input).isAssemblyMatch());
    }

    @Test
    public void notValidReferenceAlleleSeqNameFastaSeqName() throws Exception {
        SubSnpNoHgvs input = newSubSnpNoHgvs(SEQNAME_1, START, SEQNAME_1, START, REFERENCE_ALLELE_1, DbsnpVariantType.SNV);
        assertFalse(processorSeqName.process(input).isAssemblyMatch());
    }

    @Test
    public void validReferenceAlleleGenBankFastaSeqName() throws Exception {
        SubSnpNoHgvs input = newSubSnpNoHgvs(null, 0, GENBANK_1, START, REFERENCE_ALLELE, DbsnpVariantType.DIV);
        assertTrue(processorSeqName.process(input).isAssemblyMatch());
    }

    @Test
    public void notValidReferenceAlleleGenBankFastaSeqName() throws Exception {
        SubSnpNoHgvs input = newSubSnpNoHgvs(null, 0, GENBANK_1, START, REFERENCE_ALLELE_1, DbsnpVariantType.SNV);
        assertFalse(processorSeqName.process(input).isAssemblyMatch());
    }

    @Test
    public void validReferenceAlleleRefSeqFastaSeqName() throws Exception {
        SubSnpNoHgvs input = newSubSnpNoHgvs(null, 0, REFSEQ_1, START, REFERENCE_ALLELE, DbsnpVariantType.DIV);
        assertTrue(processorSeqName.process(input).isAssemblyMatch());
    }

    @Test
    public void notValidReferenceAlleleRefSeqFastaSeqName() throws Exception {
        SubSnpNoHgvs input = newSubSnpNoHgvs(null, 0, REFSEQ_1, START, REFERENCE_ALLELE_1, DbsnpVariantType.SNV);
        assertFalse(processorSeqName.process(input).isAssemblyMatch());
    }

    @Test
    public void validReferenceAlleleUcscFastaSeqName() throws Exception {
        SubSnpNoHgvs input = newSubSnpNoHgvs(null, 0, UCSC_1, START, REFERENCE_ALLELE, DbsnpVariantType.DIV);
        assertTrue(processorSeqName.process(input).isAssemblyMatch());
    }

    @Test
    public void notValidReferenceAlleleUcscFastaSeqName() throws Exception {
        SubSnpNoHgvs input = newSubSnpNoHgvs(null, 0, UCSC_1, START, REFERENCE_ALLELE_1, DbsnpVariantType.SNV);
        assertFalse(processorSeqName.process(input).isAssemblyMatch());
    }

    //GenBank Fasta

    @Test
    public void validReferenceAlleleSeqNameFastaGenBank() throws Exception {
        SubSnpNoHgvs input = newSubSnpNoHgvs(SEQNAME_1, START, SEQNAME_1, START, REFERENCE_ALLELE, DbsnpVariantType.DIV);
        assertTrue(processorGenBank.process(input).isAssemblyMatch());
    }

    //RefSeq Fasta

    @Test
    public void validReferenceAlleleSeqNumFastaRefSeq() throws Exception {
        SubSnpNoHgvs input = newSubSnpNoHgvs(SEQNAME_1, START, SEQNAME_1, START, REFERENCE_ALLELE, DbsnpVariantType.DIV);
        assertTrue(processorRefSeq.process(input).isAssemblyMatch());
    }

    //Ucsc Fasta

    @Test
    public void validReferenceAlleleSeqNumFastaUcsc() throws Exception {
        SubSnpNoHgvs input = newSubSnpNoHgvs(SEQNAME_1, START, SEQNAME_1, START, REFERENCE_ALLELE, DbsnpVariantType.DIV);
        assertTrue(processorUcsc.process(input).isAssemblyMatch());
    }

    // Other

    @Test
    public void validReferenceSeqNameInvalidCoordinates() throws Exception {
        SubSnpNoHgvs input = newSubSnpNoHgvs(null, 0, UCSC_1, Integer.MAX_VALUE, REFERENCE_ALLELE_1, DbsnpVariantType.SNV);
        assertFalse(processorSeqName.process(input).isAssemblyMatch());
    }

    @Test
    public void softMaskingShouldNotMatter() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = newSubSnpNoHgvs(SOFT_MASKED_FASTA_CONTIG, 1, SOFT_MASKED_FASTA_CONTIG, 1, "G",
                                                    DbsnpVariantType.SNV);
        assertTrue(processorSoftMasked.process(subSnpNoHgvs).isAssemblyMatch());
    }
}

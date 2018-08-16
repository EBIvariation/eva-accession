package uk.ac.ebi.eva.accession.dbsnp.processors;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.eva.accession.core.io.FastaSequenceReader;
import uk.ac.ebi.eva.accession.dbsnp.contig.ContigMapping;
import uk.ac.ebi.eva.accession.dbsnp.contig.ContigMappingTest;
import uk.ac.ebi.eva.accession.dbsnp.model.DbsnpVariantType;
import uk.ac.ebi.eva.accession.dbsnp.model.Orientation;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;

import java.nio.file.Files;
import java.nio.file.Paths;

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

    private AssemblyCheckerProcessor processorSeqName;

    private AssemblyCheckerProcessor processorGenBank;

    private AssemblyCheckerProcessor processorRefSeq;

    private AssemblyCheckerProcessor processorUcsc;

    @Before
    public void setUp() throws Exception {
        String fileString = ContigMappingTest.class.getResource("/input-files/assembly-report/AssemblyReport.txt").toString();
        ContigMapping contigMapping = new ContigMapping(fileString);

        FastaSequenceReader fastaSequenceReaderSeqName = new FastaSequenceReader(
                Paths.get("src/test/resources/input-files/fasta/Gallus_gallus-5.0.test.fa"));
        processorSeqName = new AssemblyCheckerProcessor(contigMapping, fastaSequenceReaderSeqName);

        FastaSequenceReader fastaSequenceReaderGenBank = new FastaSequenceReader(
                Paths.get("src/test/resources/input-files/fasta/fasta.genbank.fa"));
        processorGenBank = new AssemblyCheckerProcessor(contigMapping, fastaSequenceReaderGenBank);

        FastaSequenceReader fastaSequenceReaderRefSeq = new FastaSequenceReader(
                Paths.get("src/test/resources/input-files/fasta/fasta.refseq.fa"));
        processorRefSeq = new AssemblyCheckerProcessor(contigMapping, fastaSequenceReaderRefSeq);

        FastaSequenceReader fastaSequenceReaderUcsc = new FastaSequenceReader(
                Paths.get("src/test/resources/input-files/fasta/fasta.ucsc.fa"));
        processorUcsc = new AssemblyCheckerProcessor(contigMapping, fastaSequenceReaderUcsc);
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
        SubSnpNoHgvs input = newSubSnpNoHgvs(SEQNAME_1, START, null, 0, REFERENCE_ALLELE, DbsnpVariantType.DIV);
        assertTrue(processorSeqName.process(input).isAssemblyMatch());
    }

    @Test
    public void notValidReferenceAlleleSeqNameFastaSeqName() throws Exception {
        SubSnpNoHgvs input = newSubSnpNoHgvs(SEQNAME_1, START, null, 0, REFERENCE_ALLELE_1, DbsnpVariantType.SNV);
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
        SubSnpNoHgvs input = newSubSnpNoHgvs(REFSEQ_1, START, null, 0, REFERENCE_ALLELE, DbsnpVariantType.DIV);
        assertTrue(processorSeqName.process(input).isAssemblyMatch());
    }

    @Test
    public void notValidReferenceAlleleRefSeqFastaSeqName() throws Exception {
        SubSnpNoHgvs input = newSubSnpNoHgvs(REFSEQ_1, START, null, 0, REFERENCE_ALLELE_1, DbsnpVariantType.SNV);
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
        SubSnpNoHgvs input = newSubSnpNoHgvs(SEQNAME_1, START, null, 0, REFERENCE_ALLELE, DbsnpVariantType.DIV);
        assertTrue(processorGenBank.process(input).isAssemblyMatch());
    }

    //RefSeq Fasta

    @Test
    public void validReferenceAlleleSeqNumFastaRefSeq() throws Exception {
        SubSnpNoHgvs input = newSubSnpNoHgvs(SEQNAME_1, START, null, 0, REFERENCE_ALLELE, DbsnpVariantType.DIV);
        assertTrue(processorRefSeq.process(input).isAssemblyMatch());
    }

    //Ucsc Fasta

    @Test
    public void validReferenceAlleleSeqNumFastaUcsc() throws Exception {
        SubSnpNoHgvs input = newSubSnpNoHgvs(SEQNAME_1, START, null, 0, REFERENCE_ALLELE, DbsnpVariantType.DIV);
        assertTrue(processorUcsc.process(input).isAssemblyMatch());
    }

    // Other

    @Test
    public void validReferenceSeqNameInvalidCoordinates() throws Exception {
        SubSnpNoHgvs input = newSubSnpNoHgvs(null, 0, UCSC_1, Integer.MAX_VALUE, REFERENCE_ALLELE_1, DbsnpVariantType.SNV);
        assertFalse(processorSeqName.process(input).isAssemblyMatch());
    }

}

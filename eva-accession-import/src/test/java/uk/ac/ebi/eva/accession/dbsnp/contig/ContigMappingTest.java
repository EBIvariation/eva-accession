package uk.ac.ebi.eva.accession.dbsnp.contig;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ContigMappingTest {

    private static final String SEQNAME_CH1 = "ch1";

    private static final String GENBANK_CONTIG = "CM000994.2";

    private static final String REFSEQ_CONTIG = "NC_000067.6";

    private static final String SEQNAME_CONTIG = "1";

    private static final String UCSC_CONTIG = "1";

    private static final String CONTIG_WITHOUT_SYNONYM = "NT_without_synonym";

    private ContigMapping contigMapping;

    @Before
    public void setUp() throws Exception {
        String fileString = ContigMappingTest.class.getResource("/input-files/assembly-report/AssemblyReport.txt").toString();
        contigMapping = new ContigMapping(fileString);
    }

    @Test
    public void matchWhenVcfHasPrefixes() {
        assertEquals(GENBANK_CONTIG, contigMapping.getContigSynonyms(SEQNAME_CH1).getGenBank());
    }

    @Test
    public void noSynonym() throws Exception {
        assertNull(contigMapping.getContigSynonyms(CONTIG_WITHOUT_SYNONYM));
    }

    //SEQNAME

    @Test
    public void getSeqNameFromSeqName() {
        assertEquals(SEQNAME_CONTIG, contigMapping.getContigSynonyms(SEQNAME_CONTIG).getSequenceName());
    }

    @Test
    public void getSeqNameFromGenBank() {
        assertEquals(SEQNAME_CONTIG, contigMapping.getContigSynonyms(GENBANK_CONTIG).getSequenceName());
    }

    @Test
    public void getSeqNameFromRefSeq() {
        assertEquals(SEQNAME_CONTIG, contigMapping.getContigSynonyms(REFSEQ_CONTIG).getSequenceName());
    }

    @Test
    public void getSeqNameFromUcsc() {
        assertEquals(SEQNAME_CONTIG, contigMapping.getContigSynonyms(UCSC_CONTIG).getSequenceName());
    }

    //GENBANK

    @Test
    public void getGenBankFromSeqName() {
        assertEquals(GENBANK_CONTIG, contigMapping.getContigSynonyms(SEQNAME_CONTIG).getGenBank());
    }

    @Test
    public void getGenBankFromGenBank() {
        assertEquals(GENBANK_CONTIG, contigMapping.getContigSynonyms(GENBANK_CONTIG).getGenBank());
    }

    @Test
    public void getGenBankFromRefSeq() {
        assertEquals(GENBANK_CONTIG, contigMapping.getContigSynonyms(REFSEQ_CONTIG).getGenBank());
    }

    @Test
    public void getGenBankFromUcsc() {
        assertEquals(GENBANK_CONTIG, contigMapping.getContigSynonyms(UCSC_CONTIG).getGenBank());
    }

    //REFSEQ

    @Test
    public void getRefSeqFromSeqName() {
        assertEquals(REFSEQ_CONTIG, contigMapping.getContigSynonyms(SEQNAME_CONTIG).getRefSeq());
    }

    @Test
    public void getRefSeqFromGenBank() {
        assertEquals(REFSEQ_CONTIG, contigMapping.getContigSynonyms(GENBANK_CONTIG).getRefSeq());
    }

    @Test
    public void getRefSeqFromRefSeq() {
        assertEquals(REFSEQ_CONTIG, contigMapping.getContigSynonyms(REFSEQ_CONTIG).getRefSeq());
    }

    @Test
    public void getRefSeqFromUcsc() {
        assertEquals(REFSEQ_CONTIG, contigMapping.getContigSynonyms(UCSC_CONTIG).getRefSeq());
    }

    //UCSC

    @Test
    public void getUcscFromSeqName() {
        assertEquals(UCSC_CONTIG, contigMapping.getContigSynonyms(SEQNAME_CONTIG).getUcsc());
    }

    @Test
    public void getUcscFromGenBank() {
        assertEquals(UCSC_CONTIG, contigMapping.getContigSynonyms(GENBANK_CONTIG).getUcsc());
    }

    @Test
    public void getUcscFromRefSeq() {
        assertEquals(UCSC_CONTIG, contigMapping.getContigSynonyms(REFSEQ_CONTIG).getUcsc());
    }

    @Test
    public void getUcscFromUcsc() {
        assertEquals(UCSC_CONTIG, contigMapping.getContigSynonyms(UCSC_CONTIG).getUcsc());
    }

}

package uk.ac.ebi.eva.accession.dbsnp.contig;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.dbsnp.contig.ContigNameConvention.GEN_BANK;
import static uk.ac.ebi.eva.accession.dbsnp.contig.ContigNameConvention.REF_SEQ;
import static uk.ac.ebi.eva.accession.dbsnp.contig.ContigNameConvention.SEQUENCE_NAME;
import static uk.ac.ebi.eva.accession.dbsnp.contig.ContigNameConvention.UCSC;

public class ContigMappingTest {

    private static final String GENBANK_1 = "genbank_example_1";

    private static final String GENBANK_2 = "genbank_example_2";

    private static final String GENBANK_3 = "genbank_example_3";

    private static final String REFSEQ_1 = "refseq_example_1";

    private static final String REFSEQ_2 = "refseq_example_2";

    private static final String REFSEQ_3 = "refseq_example_3";

    private static final String SEQNAME_1 = "1";

    private static final String SEQNAME_2 = "2";

    private static final String SEQNAME_3 = "3";

    private static final String SEQNAME_ch1 = "ch1";

    private static final String UCSC_1 = "ucsc_example_1";

    private static final String UCSC_2 = "ucsc_example_2";

    private static final String UCSC_3 = "ucsc_example_3";

    private static final String GENBANK_CONTIG = "CM000994.2";

    private static final String REFSEQ_CONTIG = "NC_000067.6";

    private static final String SEQNAME_CONTIG = "1";

    private static final String UCSC_CONTIG = "1";

    private static final String CONTIG_WITHOUT_SYNONYM = "NT_without_synonym";

    private ContigMapping contigMapping;

    @Before
    public void setUp() throws Exception {
        String fileString = Thread.currentThread().getContextClassLoader().getResource("input-files/AssemblyReport.txt")
                                  .toString();
        contigMapping = new ContigMapping(fileString);
    }

    @Test
    public void useMapContructor() throws Exception {
        HashMap<String, ContigSynonyms> contigMapSeqName = new HashMap<>();
        contigMapSeqName.put(SEQNAME_1, new ContigSynonyms(SEQNAME_1, GENBANK_1, REFSEQ_1, UCSC_1));
        contigMapSeqName.put(SEQNAME_2, new ContigSynonyms(SEQNAME_2, GENBANK_2, REFSEQ_2, UCSC_2));
        contigMapSeqName.put(SEQNAME_3, new ContigSynonyms(SEQNAME_3, GENBANK_3, REFSEQ_3, UCSC_3));

        HashMap<String, ContigSynonyms> contigMapGenBank = new HashMap<>();
        contigMapGenBank.put(GENBANK_1, new ContigSynonyms(SEQNAME_1, GENBANK_1, REFSEQ_1, UCSC_1));
        contigMapGenBank.put(GENBANK_2, new ContigSynonyms(SEQNAME_2, GENBANK_2, REFSEQ_2, UCSC_2));
        contigMapGenBank.put(GENBANK_3, new ContigSynonyms(SEQNAME_3, GENBANK_3, REFSEQ_3, UCSC_3));

        HashMap<String, ContigSynonyms> contigMapRefSeq = new HashMap<>();
        contigMapRefSeq.put(REFSEQ_1, new ContigSynonyms(SEQNAME_1, GENBANK_1, REFSEQ_1, UCSC_1));
        contigMapRefSeq.put(REFSEQ_2, new ContigSynonyms(SEQNAME_2, GENBANK_2, REFSEQ_2, UCSC_2));
        contigMapRefSeq.put(REFSEQ_3, new ContigSynonyms(SEQNAME_3, GENBANK_3, REFSEQ_3, UCSC_3));

        HashMap<String, ContigSynonyms> contigMapUcsc = new HashMap<>();
        contigMapUcsc.put(UCSC_1, new ContigSynonyms(SEQNAME_1, GENBANK_1, REFSEQ_1, UCSC_1));
        contigMapUcsc.put(UCSC_2, new ContigSynonyms(SEQNAME_2, GENBANK_2, REFSEQ_2, UCSC_2));
        contigMapUcsc.put(UCSC_3, new ContigSynonyms(SEQNAME_3, GENBANK_3, REFSEQ_3, UCSC_3));

        ContigMapWrapper contigMapWrapper = new ContigMapWrapper();
        contigMapWrapper.setSequenceNameMap(contigMapSeqName);
        contigMapWrapper.setGenBankMap(contigMapGenBank);
        contigMapWrapper.setRefSeqMap(contigMapRefSeq);
        contigMapWrapper.setUcscMap(contigMapUcsc);

        ContigMapping contigMapping = new ContigMapping(contigMapWrapper);

        assertEquals(SEQNAME_1, contigMapping.getContigOrDefault(REFSEQ_1, SEQUENCE_NAME));
        assertEquals(SEQNAME_2, contigMapping.getContigOrDefault(REFSEQ_2, SEQUENCE_NAME));
        assertEquals(SEQNAME_3, contigMapping.getContigOrDefault(REFSEQ_3, SEQUENCE_NAME));

        assertEquals(GENBANK_1, contigMapping.getContigOrDefault(SEQNAME_1, GEN_BANK));
        assertEquals(GENBANK_2, contigMapping.getContigOrDefault(SEQNAME_2, GEN_BANK));
        assertEquals(GENBANK_3, contigMapping.getContigOrDefault(SEQNAME_3, GEN_BANK));

        assertEquals(REFSEQ_1, contigMapping.getContigOrDefault(UCSC_1, REF_SEQ));
        assertEquals(REFSEQ_2, contigMapping.getContigOrDefault(UCSC_2, REF_SEQ));
        assertEquals(REFSEQ_3, contigMapping.getContigOrDefault(UCSC_3, REF_SEQ));

        assertEquals(UCSC_1, contigMapping.getContigOrDefault(GENBANK_1, UCSC));
        assertEquals(UCSC_2, contigMapping.getContigOrDefault(REFSEQ_2, UCSC));
        assertEquals(UCSC_3, contigMapping.getContigOrDefault(SEQNAME_3, UCSC));
        assertEquals("does not exist", contigMapping.getContigOrDefault("does not exist", UCSC));
    }

    @Test
    public void matchWhenVcfHasPrefixes() {
        HashMap<String, ContigSynonyms> contigMapSeqName = new HashMap<>();
        contigMapSeqName.put(SEQNAME_1, new ContigSynonyms(SEQNAME_1, GENBANK_1, REFSEQ_1, UCSC_1));

        HashMap<String, ContigSynonyms> contigMapGenBank = new HashMap<>();
        contigMapGenBank.put(GENBANK_1, new ContigSynonyms(SEQNAME_1, GENBANK_1, REFSEQ_1, UCSC_1));

        ContigMapWrapper contigMapWrapper = new ContigMapWrapper();
        contigMapWrapper.setSequenceNameMap(contigMapSeqName);
        contigMapWrapper.setGenBankMap(contigMapGenBank);

        ContigMapping contigMapping = new ContigMapping(contigMapWrapper);

        assertEquals(GENBANK_1, contigMapping.getContigOrDefault(SEQNAME_ch1, GEN_BANK));
    }

    @Test
    public void noSynonym() throws Exception {
        assertEquals(CONTIG_WITHOUT_SYNONYM, contigMapping.getContigOrDefault(CONTIG_WITHOUT_SYNONYM, GEN_BANK));
    }

    //SEQNAME

    @Test
    public void getSeqNameFromSeqName() {
        assertEquals(SEQNAME_CONTIG, contigMapping.getContigOrDefault(SEQNAME_CONTIG, SEQUENCE_NAME));
    }

    @Test
    public void getSeqNameFromGenBank() {
        assertEquals(SEQNAME_CONTIG, contigMapping.getContigOrDefault(GENBANK_CONTIG, SEQUENCE_NAME));
    }

    @Test
    public void getSeqNameFromRefSeq() {
        assertEquals(SEQNAME_CONTIG, contigMapping.getContigOrDefault(REFSEQ_CONTIG, SEQUENCE_NAME));
    }

    @Test
    public void getSeqNameFromUcsc() {
        assertEquals(SEQNAME_CONTIG, contigMapping.getContigOrDefault(UCSC_CONTIG, SEQUENCE_NAME));
    }

    //GENBANK

    @Test
    public void getGenBankFromSeqName() {
        assertEquals(GENBANK_CONTIG, contigMapping.getContigOrDefault(SEQNAME_CONTIG, GEN_BANK));
    }

    @Test
    public void getGenBankFromGenBank() {
        assertEquals(GENBANK_CONTIG, contigMapping.getContigOrDefault(GENBANK_CONTIG, GEN_BANK));
    }

    @Test
    public void getGenBankFromRefSeq() {
        assertEquals(GENBANK_CONTIG, contigMapping.getContigOrDefault(REFSEQ_CONTIG, GEN_BANK));
    }

    @Test
    public void getGenBankFromUcsc() {
        assertEquals(GENBANK_CONTIG, contigMapping.getContigOrDefault(UCSC_CONTIG, GEN_BANK));
    }

    //REFSEQ

    @Test
    public void getRefSeqFromSeqName() {
        assertEquals(REFSEQ_CONTIG, contigMapping.getContigOrDefault(SEQNAME_CONTIG, REF_SEQ));
    }

    @Test
    public void getRefSeqFromGenBank() {
        assertEquals(REFSEQ_CONTIG, contigMapping.getContigOrDefault(GENBANK_CONTIG, REF_SEQ));
    }

    @Test
    public void getRefSeqFromRefSeq() {
        assertEquals(REFSEQ_CONTIG, contigMapping.getContigOrDefault(REFSEQ_CONTIG, REF_SEQ));
    }

    @Test
    public void getRefSeqFromUcsc() {
        assertEquals(REFSEQ_CONTIG, contigMapping.getContigOrDefault(UCSC_CONTIG, REF_SEQ));
    }

    //UCSC

    @Test
    public void getUcscFromSeqName() {
        assertEquals(UCSC_CONTIG, contigMapping.getContigOrDefault(SEQNAME_CONTIG, UCSC));
    }

    @Test
    public void getUcscFromGenBank() {
        assertEquals(UCSC_CONTIG, contigMapping.getContigOrDefault(GENBANK_CONTIG, UCSC));
    }

    @Test
    public void getUcscFromRefSeq() {
        assertEquals(UCSC_CONTIG, contigMapping.getContigOrDefault(REFSEQ_CONTIG, UCSC));
    }

    @Test
    public void getUcscFromUcsc() {
        assertEquals(UCSC_CONTIG, contigMapping.getContigOrDefault(UCSC_CONTIG, UCSC));
    }

}

package uk.ac.ebi.eva.accession.dbsnp.processors;

import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.eva.accession.dbsnp.contig.ContigMapping;
import uk.ac.ebi.eva.accession.dbsnp.contig.ContigMappingTest;
import uk.ac.ebi.eva.accession.dbsnp.model.DbsnpClass;
import uk.ac.ebi.eva.accession.dbsnp.model.Orientation;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;

import static org.junit.Assert.assertEquals;

public class ContigReplacerProcessorTest {

    private static final String ASSEMBLY = "assembly";

    private static final int TAXONOMY = 1111;

    private static final long START = 5;

    private static final String REFERENCE_ALLELE = "C";

    private static final String ALTERNATE_ALLELE = "T";

    private static final String GENBANK_1 = "genbank_example_1";

    private static final String REFSEQ_1 = "refseq_example_1";

    private static final String SEQNAME_1 = "22";

    private static final String UCSC_1 = "ucsc_example_1";

    private static final Long SS_ID = 12345L;

    private static final Long RS_ID = 56789L;

    private ContigReplacerProcessor processor;

    @Before
    public void setUp() throws Exception {
        String fileString = ContigMappingTest.class.getResource("/input-files/AssemblyReport.txt").toString();
        ContigMapping contigMapping = new ContigMapping(fileString);

        processor = new ContigReplacerProcessor(contigMapping);
    }

    @Test
    public void convertVariantFromSeqNameToSeqName() throws Exception {
        SubSnpNoHgvs input = new SubSnpNoHgvs(SS_ID, RS_ID, ALTERNATE_ALLELE, ASSEMBLY, "", "", SEQNAME_1, START,
                                              SEQNAME_1, DbsnpClass.SNV, Orientation.FORWARD, Orientation.FORWARD,
                                              Orientation.FORWARD, START, true, true, REFERENCE_ALLELE, null, TAXONOMY);
        assertEquals(SEQNAME_1, processor.process(input).getContigName());
    }

    @Test
    public void convertVariantFromGenBankToSeqName() throws Exception {
        SubSnpNoHgvs input = new SubSnpNoHgvs(SS_ID, RS_ID, ALTERNATE_ALLELE, ASSEMBLY, "", "", null, 0L, GENBANK_1,
                                              DbsnpClass.SNV, Orientation.FORWARD, Orientation.FORWARD,
                                              Orientation.FORWARD, START, true, true, REFERENCE_ALLELE, null, TAXONOMY);
        assertEquals(SEQNAME_1, processor.process(input).getContigName());
    }

    @Test
    public void convertVariantFromRefSeqToSeqName() throws Exception {
        SubSnpNoHgvs input = new SubSnpNoHgvs(SS_ID, RS_ID, ALTERNATE_ALLELE, ASSEMBLY, "", "", REFSEQ_1, START, null,
                                              DbsnpClass.SNV, Orientation.FORWARD, Orientation.FORWARD,
                                              Orientation.FORWARD, 0L, true, true, REFERENCE_ALLELE, null, TAXONOMY);
        assertEquals(SEQNAME_1, processor.process(input).getContigName());
    }

    @Test
    public void convertVariantFromUcscToSeqName() throws Exception {
        SubSnpNoHgvs input = new SubSnpNoHgvs(SS_ID, RS_ID, ALTERNATE_ALLELE, ASSEMBLY, "", "", UCSC_1, START, UCSC_1,
                                              DbsnpClass.SNV, Orientation.FORWARD, Orientation.FORWARD,
                                              Orientation.FORWARD, START, true, true, REFERENCE_ALLELE, null, TAXONOMY);
        assertEquals(SEQNAME_1, processor.process(input).getContigName());
    }
}

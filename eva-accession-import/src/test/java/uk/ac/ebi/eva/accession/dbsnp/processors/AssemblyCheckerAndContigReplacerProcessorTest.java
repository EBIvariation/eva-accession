package uk.ac.ebi.eva.accession.dbsnp.processors;

import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.dbsnp.contig.ContigSynonym;
import uk.ac.ebi.eva.accession.dbsnp.contig.ContigMapWrapper;
import uk.ac.ebi.eva.accession.dbsnp.contig.ContigMapping;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class AssemblyCheckerAndContigReplacerProcessorTest {

    private static final String ASSEMBLY = "assembly";

    private static final int TAXONOMY = 1111;

    private static final String PROJECT = "project";

    private static final String CONTIG = "contig";

    private static final long START = 1000;

    private static final String REFERENCE_ALLELE = "A";

    private static final String ALTERNATE_ALLELE = "T";

    private static final String GENBANK_1 = "genbank_example_1";

    private static final String REFSEQ_1 = "refseq_example_1";

    private static final String SEQNAME_1 = "1";

    private static final String UCSC_1 = "ucsc_example_1";

    private AssemblyCheckerAndContigReplacerProcessor assemblyCheckerAndContigReplacerProcessor;

    @Before
    public void setUp() {

        HashMap<String, ContigSynonym> contigMapSeqName = new HashMap<>();
        contigMapSeqName.put(SEQNAME_1, new ContigSynonym(SEQNAME_1, GENBANK_1, REFSEQ_1, UCSC_1));

        HashMap<String, ContigSynonym> contigMapGenBank = new HashMap<>();
        contigMapGenBank.put(GENBANK_1, new ContigSynonym(SEQNAME_1, GENBANK_1, REFSEQ_1, UCSC_1));

        HashMap<String, ContigSynonym> contigMapRefSeq = new HashMap<>();
        contigMapRefSeq.put(REFSEQ_1, new ContigSynonym(SEQNAME_1, GENBANK_1, REFSEQ_1, UCSC_1));

        HashMap<String, ContigSynonym> contigMapUcsc = new HashMap<>();
        contigMapUcsc.put(UCSC_1, new ContigSynonym(SEQNAME_1, GENBANK_1, REFSEQ_1, UCSC_1));

        ContigMapWrapper contigMapWrapper = new ContigMapWrapper();
        contigMapWrapper.setGenBankMap(contigMapGenBank);
        contigMapWrapper.setRefSeqMap(contigMapRefSeq);
        contigMapWrapper.setSequenceNameMap(contigMapSeqName);
        contigMapWrapper.setUcscMap(contigMapUcsc);

        ContigMapping contigMapping = new ContigMapping(contigMapWrapper);

        assemblyCheckerAndContigReplacerProcessor = new AssemblyCheckerAndContigReplacerProcessor(contigMapping);
    }

    @Test
    public void process() throws Exception {
        SubmittedVariant input = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, GENBANK_1, START, REFERENCE_ALLELE, ALTERNATE_ALLELE, true);
        assemblyCheckerAndContigReplacerProcessor.process(input);
        SubmittedVariant expected = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, SEQNAME_1, START, REFERENCE_ALLELE, ALTERNATE_ALLELE, true);
        assertEquals(expected, input);
    }

}

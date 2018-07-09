package uk.ac.ebi.eva.accession.dbsnp.processors;

import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.fasta.FastaSequenceReader;
import uk.ac.ebi.eva.accession.dbsnp.contig.ContigMapWrapper;
import uk.ac.ebi.eva.accession.dbsnp.contig.ContigMapping;
import uk.ac.ebi.eva.accession.dbsnp.contig.ContigSynonyms;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AssemblyCheckerAndContigReplacerProcessorTest {

    private static final String ASSEMBLY = "assembly";

    private static final int TAXONOMY = 1111;

    private static final String PROJECT = "project";

    private static final String CONTIG = "22";

    private static final long START = 5;

    private static final String REFERENCE_ALLELE = "CC";

    private static final String REFERENCE_ALLELE_1 = "T";

    private static final String ALTERNATE_ALLELE = "T";

    private static final String GENBANK_1 = "genbank_example_1";

    private static final String REFSEQ_1 = "refseq_example_1";

    private static final String SEQNAME_1 = "22";

    private static final String UCSC_1 = "ucsc_example_1";

    private AssemblyCheckerAndContigReplacerProcessor processorSeqName;

    private AssemblyCheckerAndContigReplacerProcessor processorGenBank;

    private AssemblyCheckerAndContigReplacerProcessor processorRefSeq;

    private AssemblyCheckerAndContigReplacerProcessor processorUcsc;

    @Before
    public void setUp() throws Exception {
        ContigSynonyms contigSynonyms = new ContigSynonyms(SEQNAME_1, GENBANK_1, REFSEQ_1, UCSC_1);
        ContigMapWrapper contigMapWrapper = new ContigMapWrapper();
        contigMapWrapper.fillContigConventionMaps(contigSynonyms);
        ContigMapping contigMapping = new ContigMapping(contigMapWrapper);

        FastaSequenceReader fastaSequenceReaderSeqName = new FastaSequenceReader(
                Paths.get("src/test/resources/input-files/fasta/Gallus_gallus-5.0.test.fa"));
        processorSeqName = new AssemblyCheckerAndContigReplacerProcessor(contigMapping, fastaSequenceReaderSeqName);

        FastaSequenceReader fastaSequenceReaderGenBank = new FastaSequenceReader(
                Paths.get("src/test/resources/input-files/fasta/fasta.genbank.fa"));
        processorGenBank = new AssemblyCheckerAndContigReplacerProcessor(contigMapping, fastaSequenceReaderGenBank);

        FastaSequenceReader fastaSequenceReaderRefSeq = new FastaSequenceReader(
                Paths.get("src/test/resources/input-files/fasta/fasta.refseq.fa"));
        processorRefSeq = new AssemblyCheckerAndContigReplacerProcessor(contigMapping, fastaSequenceReaderRefSeq);

        FastaSequenceReader fastaSequenceReaderUcsc = new FastaSequenceReader(
                Paths.get("src/test/resources/input-files/fasta/fasta.ucsc.fa"));
        processorUcsc = new AssemblyCheckerAndContigReplacerProcessor(contigMapping, fastaSequenceReaderUcsc);
    }

    @Test
    public void process() throws Exception {
        SubmittedVariant input = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, GENBANK_1, START, REFERENCE_ALLELE,
                                                      ALTERNATE_ALLELE, true);
        processorSeqName.process(input);
        SubmittedVariant expected = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, SEQNAME_1, START,
                                                         REFERENCE_ALLELE, ALTERNATE_ALLELE, true);
        assertEquals(expected, input);
    }

    //SeqName Fasta

    @Test
    public void validReferenceAlleleSeqNameFastaSeqName() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, CONTIG, START,
                                                                 REFERENCE_ALLELE, ALTERNATE_ALLELE, true);
        assertNotNull(processorSeqName.process(submittedVariant));
        // TODO assertTrue(processorSeqName.process(submittedVariant).getMatchesAssembly())
    }

    @Test
    public void notValidReferenceAlleleSeqNameFastaSeqName() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, CONTIG, START,
                                                                 REFERENCE_ALLELE_1, ALTERNATE_ALLELE, true);
        assertNotNull(processorSeqName.process(submittedVariant));
        // TODO assertFalse(processorSeqName.process(submittedVariant).getMatchesAssembly())
    }

    @Test
    public void validReferenceAlleleGenBankFastaSeqName() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, GENBANK_1, START,
                                                                 REFERENCE_ALLELE, ALTERNATE_ALLELE, true);
        assertNotNull(processorSeqName.process(submittedVariant));
        // TODO assertTrue(processorSeqName.process(submittedVariant).getMatchesAssembly())
    }

    @Test
    public void notValidReferenceAlleleGenBankFastaSeqName() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, GENBANK_1, START,
                                                                 REFERENCE_ALLELE_1, ALTERNATE_ALLELE, true);
        assertNotNull(processorSeqName.process(submittedVariant));
        // TODO assertFalse(processorSeqName.process(submittedVariant).getMatchesAssembly())
    }

    @Test
    public void validReferenceAlleleRefSeqFastaSeqName() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, REFSEQ_1, START,
                                                                 REFERENCE_ALLELE, ALTERNATE_ALLELE, true);
        assertNotNull(processorSeqName.process(submittedVariant));
        // TODO assertTrue(processorSeqName.process(submittedVariant).getMatchesAssembly())
    }

    @Test
    public void notValidReferenceAlleleRefSeqFastaSeqName() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, REFSEQ_1, START,
                                                                 REFERENCE_ALLELE_1, ALTERNATE_ALLELE, true);
        assertNotNull(processorSeqName.process(submittedVariant));
        // TODO assertFalse(processorSeqName.process(submittedVariant).getMatchesAssembly())
    }

    @Test
    public void validReferenceAlleleUcscFastaSeqName() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, UCSC_1, START,
                                                                 REFERENCE_ALLELE, ALTERNATE_ALLELE, true);
        assertNotNull(processorSeqName.process(submittedVariant));
        // TODO assertTrue(processorSeqName.process(submittedVariant).getMatchesAssembly())

    }

    @Test
    public void notValidReferenceAlleleUcscFastaSeqName() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, UCSC_1, START,
                                                                 REFERENCE_ALLELE_1, ALTERNATE_ALLELE, true);
        assertNotNull(processorSeqName.process(submittedVariant));
        // TODO assertFalse(processorSeqName.process(submittedVariant).getMatchesAssembly())
    }

    //GenBank Fasta
    @Test
    public void validReferenceAlleleSeqNameFastaGenBank() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, SEQNAME_1, START,
                                                                 REFERENCE_ALLELE, ALTERNATE_ALLELE, true);
        assertNotNull(processorGenBank.process(submittedVariant));
        // TODO assertTrue(processorSeqName.process(submittedVariant).getMatchesAssembly())
    }

    //RefSeq Fasta
    @Test
    public void validReferenceAlleleSeqNumFastaRefSeq() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, SEQNAME_1, START,
                                                                 REFERENCE_ALLELE, ALTERNATE_ALLELE, true);
        assertNotNull(processorRefSeq.process(submittedVariant));
        // TODO assertTrue(processorSeqName.process(submittedVariant).getMatchesAssembly())

    }

    //Ucsc Fasta
    @Test
    public void validReferenceAlleleSeqNumFastaUcsc() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, UCSC_1, START,
                                                                 REFERENCE_ALLELE, ALTERNATE_ALLELE, true);
        assertNotNull(processorUcsc.process(submittedVariant));
        // TODO assertTrue(processorSeqName.process(submittedVariant).getMatchesAssembly())

    }

}

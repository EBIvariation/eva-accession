package uk.ac.ebi.eva.accession.dbsnp.processors;

import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.fasta.FastaSequenceReader;
import uk.ac.ebi.eva.accession.dbsnp.contig.ContigMapWrapper;
import uk.ac.ebi.eva.accession.dbsnp.contig.ContigMapping;
import uk.ac.ebi.eva.accession.dbsnp.contig.ContigSynonyms;

import java.nio.file.Paths;
import java.util.HashMap;

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

    private AssemblyCheckerAndContigReplacerProcessor assemblyCheckerAndContigReplacerProcessor;

    private AssemblyCheckerAndContigReplacerProcessor assemblyCheckerAndContigReplacerProcessorGenBank;

    private AssemblyCheckerAndContigReplacerProcessor assemblyCheckerAndContigReplacerProcessorRefSeq;

    private AssemblyCheckerAndContigReplacerProcessor assemblyCheckerAndContigReplacerProcessorUcsc;

    @Before
    public void setUp() throws Exception{
        ContigSynonyms contigSynonyms = new ContigSynonyms(SEQNAME_1, GENBANK_1, REFSEQ_1, UCSC_1);
        HashMap<String, ContigSynonyms> contigMapSeqName = new HashMap<>();
        contigMapSeqName.put(SEQNAME_1, contigSynonyms);

        HashMap<String, ContigSynonyms> contigMapGenBank = new HashMap<>();
        contigMapGenBank.put(GENBANK_1, contigSynonyms);

        HashMap<String, ContigSynonyms> contigMapRefSeq = new HashMap<>();
        contigMapRefSeq.put(REFSEQ_1, contigSynonyms);

        HashMap<String, ContigSynonyms> contigMapUcsc = new HashMap<>();
        contigMapUcsc.put(UCSC_1, contigSynonyms);

        ContigMapWrapper contigMapWrapper = new ContigMapWrapper();
        contigMapWrapper.setGenBankMap(contigMapGenBank);
        contigMapWrapper.setRefSeqMap(contigMapRefSeq);
        contigMapWrapper.setSequenceNameMap(contigMapSeqName);
        contigMapWrapper.setUcscMap(contigMapUcsc);

        ContigMapping contigMapping = new ContigMapping(contigMapWrapper);

        FastaSequenceReader fastaSequenceReader = new FastaSequenceReader(Paths.get("src/test/resources/input-files/fasta/Gallus_gallus-5.0.test.fa"));
        assemblyCheckerAndContigReplacerProcessor = new AssemblyCheckerAndContigReplacerProcessor(contigMapping, fastaSequenceReader);

        FastaSequenceReader fastaSequenceReaderGenBank = new FastaSequenceReader(Paths.get("src/test/resources/input-files/fasta/fasta.genbank.fa"));
        assemblyCheckerAndContigReplacerProcessorGenBank = new AssemblyCheckerAndContigReplacerProcessor(contigMapping, fastaSequenceReaderGenBank);

        FastaSequenceReader fastaSequenceReaderRefSeq = new FastaSequenceReader(Paths.get("src/test/resources/input-files/fasta/fasta.refseq.fa"));
        assemblyCheckerAndContigReplacerProcessorRefSeq = new AssemblyCheckerAndContigReplacerProcessor(contigMapping, fastaSequenceReaderRefSeq);

        FastaSequenceReader fastaSequenceReaderUcsc = new FastaSequenceReader(Paths.get("src/test/resources/input-files/fasta/fasta.ucsc.fa"));
        assemblyCheckerAndContigReplacerProcessorUcsc = new AssemblyCheckerAndContigReplacerProcessor(contigMapping, fastaSequenceReaderUcsc);
    }

    @Test
    public void process() throws Exception {
        SubmittedVariant input = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, GENBANK_1, START, REFERENCE_ALLELE, ALTERNATE_ALLELE, true);
        assemblyCheckerAndContigReplacerProcessor.process(input);
        SubmittedVariant expected = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, SEQNAME_1, START, REFERENCE_ALLELE, ALTERNATE_ALLELE, true);
        assertEquals(expected, input);
    }

    //SeqName Fasta

    @Test
    public void validReferenceAlleleSeqNameFastaSeqName() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, CONTIG, START, REFERENCE_ALLELE, ALTERNATE_ALLELE, true);
        assertNotNull(assemblyCheckerAndContigReplacerProcessor.process(submittedVariant));
    }

    @Test
    public void notValidReferenceAlleleSeqNameFastaSeqName() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, CONTIG, START, REFERENCE_ALLELE_1, ALTERNATE_ALLELE, true);
        assertEquals(submittedVariant, assemblyCheckerAndContigReplacerProcessor.process(submittedVariant));
        // TODO assertEquals(matchesAssembly)
    }

    @Test
    public void validReferenceAlleleGenBankFastaSeqName() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, GENBANK_1, START, REFERENCE_ALLELE, ALTERNATE_ALLELE, true);
        assertNotNull(assemblyCheckerAndContigReplacerProcessor.process(submittedVariant));
    }

    @Test
    public void notValidReferenceAlleleGenBankFastaSeqName() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, GENBANK_1, START, REFERENCE_ALLELE_1, ALTERNATE_ALLELE, true);
        assertEquals(submittedVariant, assemblyCheckerAndContigReplacerProcessor.process(submittedVariant));
        // TODO assertEquals(matchesAssembly)
    }

    @Test
    public void validReferenceAlleleRefSeqFastaSeqName() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, REFSEQ_1, START, REFERENCE_ALLELE, ALTERNATE_ALLELE, true);
        assertNotNull(assemblyCheckerAndContigReplacerProcessor.process(submittedVariant));
    }

    @Test
    public void notValidReferenceAlleleRefSeqFastaSeqName() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, REFSEQ_1, START, REFERENCE_ALLELE_1, ALTERNATE_ALLELE, true);
        assertEquals(submittedVariant, assemblyCheckerAndContigReplacerProcessor.process(submittedVariant));
        // TODO assertEquals(matchesAssembly)
    }

    @Test
    public void validReferenceAlleleUcscFastaSeqName() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, UCSC_1, START, REFERENCE_ALLELE, ALTERNATE_ALLELE, true);
        assertNotNull(assemblyCheckerAndContigReplacerProcessor.process(submittedVariant));
    }

    @Test
    public void notValidReferenceAlleleUcscFastaSeqName() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, UCSC_1, START, REFERENCE_ALLELE_1, ALTERNATE_ALLELE, true);
        assertEquals(submittedVariant, assemblyCheckerAndContigReplacerProcessor.process(submittedVariant));
        // TODO assertEquals(matchesAssembly)
    }

    //GenBank Fasta
    @Test
    public void validReferenceAlleleSeqNameFastaGenBank() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, SEQNAME_1, START, REFERENCE_ALLELE, ALTERNATE_ALLELE, true);
        assertNotNull(assemblyCheckerAndContigReplacerProcessorGenBank.process(submittedVariant));
    }

    //RefSeq Fasta
    @Test
    public void validReferenceAlleleSeqNumFastaRefSeq() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, SEQNAME_1, START, REFERENCE_ALLELE, ALTERNATE_ALLELE, true);
        assertNotNull(assemblyCheckerAndContigReplacerProcessorRefSeq.process(submittedVariant));
    }

    //Ucsc Fasta
    @Test
    public void validReferenceAlleleSeqNumFastaUcsc() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, UCSC_1, START, REFERENCE_ALLELE, ALTERNATE_ALLELE, true);
        assertNotNull(assemblyCheckerAndContigReplacerProcessorUcsc.process(submittedVariant));
    }

}

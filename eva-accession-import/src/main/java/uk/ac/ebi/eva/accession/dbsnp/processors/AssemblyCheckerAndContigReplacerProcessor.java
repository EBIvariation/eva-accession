package uk.ac.ebi.eva.accession.dbsnp.processors;

import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.dbsnp.contig.ContigMapping;

import static uk.ac.ebi.eva.accession.dbsnp.contig.ContigNameConvention.GEN_BANK;
import static uk.ac.ebi.eva.accession.dbsnp.contig.ContigNameConvention.REF_SEQ;
import static uk.ac.ebi.eva.accession.dbsnp.contig.ContigNameConvention.SEQUENCE_NAME;

public class AssemblyCheckerAndContigReplacerProcessor implements ItemProcessor<SubmittedVariant, SubmittedVariant> {

    private ContigMapping contigMapping;

    public AssemblyCheckerAndContigReplacerProcessor(ContigMapping contigMapping) {
        this.contigMapping = contigMapping;
    }

    @Override
    public SubmittedVariant process(SubmittedVariant submittedVariant) throws Exception {
        String sequenceName = contigMapping.getContigOrDefault(submittedVariant.getContig(), SEQUENCE_NAME);
        submittedVariant.setContig(sequenceName);
        //check whether reference matches with fastasequence reader
        // if contig is not present in fasta, try genbank
        String genbank = contigMapping.getContigOrDefault(submittedVariant.getContig(), GEN_BANK);
        // if contig is still not present in fasta, try refseq
        String refseq = contigMapping.getContigOrDefault(submittedVariant.getContig(), REF_SEQ);
        // if still doesn't match, flag sv.setMatchesAssembly(false)
        return submittedVariant;
    }
}

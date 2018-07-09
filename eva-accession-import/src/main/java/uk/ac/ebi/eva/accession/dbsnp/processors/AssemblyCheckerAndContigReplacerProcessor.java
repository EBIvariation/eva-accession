package uk.ac.ebi.eva.accession.dbsnp.processors;

import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.fasta.FastaSequenceReader;
import uk.ac.ebi.eva.accession.dbsnp.contig.ContigMapping;
import uk.ac.ebi.eva.accession.dbsnp.contig.ContigSynonyms;

public class AssemblyCheckerAndContigReplacerProcessor implements ItemProcessor<SubmittedVariant, SubmittedVariant> {

    private ContigMapping contigMapping;

    private FastaSequenceReader fastaReader;

    public AssemblyCheckerAndContigReplacerProcessor(ContigMapping contigMapping, FastaSequenceReader fastaReader) {
        this.contigMapping = contigMapping;
        this.fastaReader = fastaReader;
    }

    @Override
    public SubmittedVariant process(SubmittedVariant submittedVariant) throws Exception {
        ContigSynonyms contigSynonyms = contigMapping.getContigSynonyms(submittedVariant.getContig());
        if (contigSynonyms != null) {
            long end = calculateReferenceAlleleEndPosition(submittedVariant.getReferenceAllele(), submittedVariant.getStart());
            String sequence = getSequenceUsingSynonyms(contigSynonyms, submittedVariant.getStart(), end);
            if (sequence.equals(submittedVariant.getReferenceAllele())) {
                submittedVariant.setContig(contigSynonyms.getSequenceName());
//              TODO: submittedVariant.setMatchesAssembly(true);
            } else {
//              TODO: submittedVariant.setMatchesAssembly(false);
            }
            return submittedVariant;
        }
        throw new IllegalArgumentException("Contig '" + submittedVariant.getContig() + "' not found in the ASSEMBLY REPORT");
    }

    private String getSequenceUsingSynonyms(ContigSynonyms contigSynonyms, long start, long end) {
        String sequence = "";
        if ((sequence = getSequence(contigSynonyms.getSequenceName(), start, end)) != null) {
            return sequence;
        }
        if ((sequence = getSequence(contigSynonyms.getGenBank(), start, end)) != null) {
            return sequence;
        }
        if ((sequence = getSequence(contigSynonyms.getRefSeq(), start, end)) != null) {
            return sequence;
        }
        if ((sequence = getSequence(contigSynonyms.getUcsc(), start, end)) != null) {
            return sequence;
        }
        throw new IllegalArgumentException("Contig " + contigSynonyms.getSequenceName() + " not found in FASTA file");
    }

    private String getSequence(String contig, long start, long end) {
        try {
            return fastaReader.getSequence(contig, start, end);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private long calculateReferenceAlleleEndPosition(String referenceAllele, long start) {
        long referenceLength = referenceAllele.length() - 1;
        return start + referenceLength;
    }
}
package uk.ac.ebi.eva.accession.dbsnp.processors;

import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.accession.core.io.FastaSequenceReader;
import uk.ac.ebi.eva.accession.dbsnp.contig.ContigMapping;
import uk.ac.ebi.eva.accession.dbsnp.contig.ContigSynonyms;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;

public class AssemblyCheckerProcessor implements ItemProcessor<SubSnpNoHgvs, SubSnpNoHgvs> {

    private ContigMapping contigMapping;

    private FastaSequenceReader fastaReader;

    public AssemblyCheckerProcessor(ContigMapping contigMapping, FastaSequenceReader fastaReader) {
        this.contigMapping = contigMapping;
        this.fastaReader = fastaReader;
    }

    @Override
    public SubSnpNoHgvs process(SubSnpNoHgvs subSnpNoHgvs) throws Exception {
        ContigSynonyms contigSynonyms;
        long start;
        if (subSnpNoHgvs.getChromosome() != null) {
            contigSynonyms = contigMapping.getContigSynonyms(subSnpNoHgvs.getChromosome());
            start = subSnpNoHgvs.getChromosomeStart();
        } else {
            contigSynonyms = contigMapping.getContigSynonyms(subSnpNoHgvs.getContigName());
            start = subSnpNoHgvs.getContigStart();
        }

        if (contigSynonyms == null) {
            throw new IllegalArgumentException(
                    "Contig '" + subSnpNoHgvs.getContigName() + "' not found in the assembly report");
        }

        long end = calculateReferenceAlleleEndPosition(subSnpNoHgvs.getReference(), start);
        String sequence = getSequenceUsingSynonyms(contigSynonyms, start, end);
        if (sequence.equals(subSnpNoHgvs.getReference())) {
            subSnpNoHgvs.setAssemblyMatch(true);
        } else {
            subSnpNoHgvs.setAssemblyMatch(false);
        }
        return subSnpNoHgvs;
    }

    private String getSequenceUsingSynonyms(ContigSynonyms contigSynonyms, long start, long end) {
        String sequence;
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
        throw new IllegalArgumentException("Contig " + contigSynonyms.toString() + " not found in FASTA file");
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
package uk.ac.ebi.eva.accession.dbsnp.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.accession.core.io.FastaSequenceReader;
import uk.ac.ebi.eva.accession.dbsnp.contig.ContigMapping;
import uk.ac.ebi.eva.accession.dbsnp.contig.ContigSynonyms;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;

public class AssemblyCheckerProcessor implements ItemProcessor<SubSnpNoHgvs, SubSnpNoHgvs> {

    private static Logger logger = LoggerFactory.getLogger(AssemblyCheckerProcessor.class);

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

        long end = calculateReferenceAlleleEndPosition(subSnpNoHgvs.getReferenceInForwardStrand(), start);
        String sequence;
        boolean matches = false;
        try {
            sequence = getSequenceUsingSynonyms(contigSynonyms, start, end);
            matches = sequence.equals(subSnpNoHgvs.getReferenceInForwardStrand());
        } catch (IllegalArgumentException ex) {
            logger.warn(ex.getLocalizedMessage());
        } finally {
            subSnpNoHgvs.setAssemblyMatch(matches);
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
        throw new IllegalArgumentException("Contig " + contigSynonyms.toString() + " not found in the FASTA file");
    }

    /**
     * Tries to retrieve a sequence from a FASTA file. If the sequence can't be found using that name, it could be that
     * the FASTA uses a different nomenclature; in that case a synonym could be used.
     *
     * But if the sequence can be found and the coordinates are not valid, the error is unrecoverable and an exception
     * is thrown.
     *
     * @param contig Name of the sequence to be retrieved
     * @param start Start coordinate of the sequence to be retrieved
     * @param end End coordinate of the sequence to be retrieved
     * @return The sequence or null if not found
     * @throws IllegalArgumentException if the sequence name can be found in the FASTA but the coordinates are too large
     */
    private String getSequence(String contig, long start, long end) {
        try {
            return fastaReader.getSequence(contig, start, end);
        } catch (IllegalArgumentException e1) {
            /*
             The same exception type could be caused because the contig was not found, or the requested coordinates
             were greater than the last position in the contig. In order to differentiate between them, we make another
             request for position 1 of the sequence.
             */
            try {
                fastaReader.getSequence(contig, 1, 1);
            } catch (IllegalArgumentException e2) {
                return null;
            }

            // The exception is only thrown when the sequence name was found but the coordinates are not valid.
            throw e1;
        }
    }

    private long calculateReferenceAlleleEndPosition(String referenceAllele, long start) {
        long referenceLength = referenceAllele.length() - 1;
        return start + referenceLength;
    }
}
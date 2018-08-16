/*
 * Copyright 2018 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

        long end = calculateReferenceAlleleEndPosition(subSnpNoHgvs.getReferenceInForwardStrand(), start);
        String sequence = getSequenceUsingSynonyms(contigSynonyms, start, end);
        if (sequence.equals(subSnpNoHgvs.getReferenceInForwardStrand())) {
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
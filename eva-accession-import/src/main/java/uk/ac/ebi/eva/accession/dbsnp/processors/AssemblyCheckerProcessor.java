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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.accession.dbsnp.io.FastaSynonymSequenceReader;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;

public class AssemblyCheckerProcessor implements ItemProcessor<SubSnpNoHgvs, SubSnpNoHgvs> {

    private static Logger logger = LoggerFactory.getLogger(AssemblyCheckerProcessor.class);

    private FastaSynonymSequenceReader fastaReader;

    public AssemblyCheckerProcessor(FastaSynonymSequenceReader fastaReader) {
        this.fastaReader = fastaReader;
    }

    @Override
    public SubSnpNoHgvs process(SubSnpNoHgvs subSnpNoHgvs) throws Exception {
        String referenceAllele = subSnpNoHgvs.getReferenceInForwardStrand();
        if (referenceAllele.isEmpty()) {
            return subSnpNoHgvs;
        }

        String contig;
        long start;
        if (subSnpNoHgvs.getChromosome() != null) {
            contig = subSnpNoHgvs.getChromosome();
            start = subSnpNoHgvs.getChromosomeStart();
        } else {
            contig = subSnpNoHgvs.getContigName();
            start = subSnpNoHgvs.getContigStart();
        }

        boolean matches = false;
        try {
            long end = calculateReferenceAlleleEndPosition(referenceAllele, start);
            String sequence = fastaReader.getSequence(contig, start, end);
            matches = sequence.equals(referenceAllele);
        } catch (IllegalArgumentException ex) {
            logger.warn(ex.getLocalizedMessage());
        } finally {
            subSnpNoHgvs.setAssemblyMatch(matches);
        }

        return subSnpNoHgvs;
    }

    private long calculateReferenceAlleleEndPosition(String referenceAllele, long start) {
        long referenceLength = referenceAllele.length() - 1;
        return start + referenceLength;
    }
}

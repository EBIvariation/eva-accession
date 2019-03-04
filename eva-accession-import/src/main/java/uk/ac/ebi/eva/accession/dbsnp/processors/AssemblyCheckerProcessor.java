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

import uk.ac.ebi.eva.accession.core.io.FastaSynonymSequenceReader;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;

import java.util.HashSet;
import java.util.Set;

public class AssemblyCheckerProcessor implements ItemProcessor<SubSnpNoHgvs, SubSnpNoHgvs> {

    private static Logger logger = LoggerFactory.getLogger(AssemblyCheckerProcessor.class);

    private FastaSynonymSequenceReader fastaReader;

    private Set<String> processedContigs;

    public AssemblyCheckerProcessor(FastaSynonymSequenceReader fastaReader) {
        this.fastaReader = fastaReader;
        this.processedContigs = new HashSet<>();
    }

    @Override
    public SubSnpNoHgvs process(SubSnpNoHgvs subSnpNoHgvs) throws Exception {
        String referenceAllele = subSnpNoHgvs.getReferenceInForwardStrand();
        if (referenceAllele.isEmpty()) {
            subSnpNoHgvs.setAssemblyMatch(true);
            return subSnpNoHgvs;
        }

        String contig = subSnpNoHgvs.getVariantRegion().getChromosome();
        long start = subSnpNoHgvs.getVariantRegion().getStart();

        boolean matches = false;
        try {
            long end = calculateReferenceAlleleEndPosition(referenceAllele, start);
            String sequence = fastaReader.getSequenceToUpperCase(contig, start, end);
            matches = sequence.equals(referenceAllele.toUpperCase());
        } catch (IllegalArgumentException ex) {
            if (!processedContigs.contains(contig)) {
                processedContigs.add(contig);
                logger.warn(ex.getLocalizedMessage());
            }
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

/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
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

import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.contig.ContigSynonyms;
import uk.ac.ebi.eva.accession.dbsnp.exceptions.NonIdenticalChromosomeAccessionsException;
import uk.ac.ebi.eva.accession.dbsnp.model.CoordinatesPresence;

import static org.springframework.util.StringUtils.hasText;

/**
 * This Spring Batch processor fails when the equivalent INSDC accession for a contig can't be found among a set of mappings.
 */
public class ContigSynonymValidationProcessor implements ItemProcessor<CoordinatesPresence, Boolean> {

    private ContigMapping contigMapping;

    public ContigSynonymValidationProcessor(ContigMapping contigMapping) {
        this.contigMapping = contigMapping;
    }

    @Override
    public Boolean process(CoordinatesPresence coordinatesPresence) throws Exception {
        String chromosome = coordinatesPresence.getChromosome();
        boolean chromosomeStartPresent = coordinatesPresence.isChromosomeStartPresent();
        String contig = coordinatesPresence.getContig();

        ContigSynonyms chromosomeSynonyms = contigMapping.getContigSynonyms(chromosome);
        ContigSynonyms contigSynonyms = contigMapping.getContigSynonyms(contig);

        StringBuilder reasons = new StringBuilder();
        if (!isChromosomeReplaceable(chromosome, chromosomeStartPresent, chromosomeSynonyms, reasons)) {
            if (!isContigReplaceable(contigSynonyms, reasons)) {
                throw new IllegalArgumentException("Variants with chromosome '" + chromosome + "' and contig '" + contig
                                                   + "' can NOT use an INSDC contig accession. Reasons: "
                                                   + reasons.toString());
            }
        }

        // an INSDC accession can be used, everything is alright
        return true;
    }

    public static boolean isChromosomeReplaceable(String chromosome, boolean chromosomeStartPresent,
                                                  ContigSynonyms chromosomeSynonyms, StringBuilder reason) {
        if (chromosome == null || !chromosomeStartPresent) {
            reason.append("Chromosome coordinates not available for at least one variant. ");
            return false;
        }

        if (chromosomeSynonyms == null) {
            reason.append("The assembly report does not contain any synonyms for the chromosome. ");
            return false;
        }

        if (!hasText(chromosomeSynonyms.getGenBank())) {
            reason.append("The equivalent INSDC accession for the chromosome is empty. ");
            return false;
        }

        if (NonIdenticalChromosomeAccessionsException.isExceptionApplicable(
                chromosomeSynonyms.isIdenticalGenBankAndRefSeq(), chromosomeSynonyms.getRefSeq())) {
            throw new NonIdenticalChromosomeAccessionsException(chromosome,
                                                                chromosomeSynonyms.getGenBank(),
                                                                chromosomeSynonyms.getRefSeq());
        }

        return true;
    }

    public static boolean isContigReplaceable(ContigSynonyms contigSynonyms, StringBuilder reason) {
        if (contigSynonyms == null) {
            reason.append("The assembly report does not contain any synonyms for the contig accession. ");
            return false;
        }

        if (!contigSynonyms.isIdenticalGenBankAndRefSeq() || !hasText(contigSynonyms.getGenBank())) {
            reason.append("The contig accession has non-identical or empty INSDC accession. ");
            return false;
        }

        return true;
    }
}

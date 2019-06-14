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

import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.contig.ContigSynonyms;
import uk.ac.ebi.eva.accession.dbsnp.exceptions.NonIdenticalChromosomeAccessionsException;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;

import java.util.HashSet;
import java.util.Set;

import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.IMPORT_DBSNP_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.VALIDATE_CONTIGS_STEP;

/**
 * This class replaces the RefSeq contig accessions or the chromosome names into INSDC (Genbank) accessions.
 *
 * This class should only be used when importing from dbSNP! The replacement strategy is subtle in the details, so
 * don't try to reuse the class as it is for other purposes.
 */
public class ContigReplacerProcessor implements ItemProcessor<SubSnpNoHgvs, SubSnpNoHgvs> {

    private static final Logger logger = LoggerFactory.getLogger(ContigReplacerProcessor.class);

    private ContigMapping contigMapping;

    private String assemblyAccession;

    private Set<String> processedContigs;

    private Set<String> nonIdenticalChromosomes;

    public ContigReplacerProcessor(ContigMapping contigMapping, String assemblyAccession) {
        this.contigMapping = contigMapping;
        this.assemblyAccession = assemblyAccession;
        this.processedContigs = new HashSet<>();
        this.nonIdenticalChromosomes = new HashSet<>();
    }

    @Override
    public SubSnpNoHgvs process(SubSnpNoHgvs subSnpNoHgvs) throws Exception {
        String contigName = subSnpNoHgvs.getContigName();
        ContigSynonyms contigSynonyms = contigMapping.getContigSynonyms(contigName);
        ContigSynonyms chromosomeSynonyms = contigMapping.getContigSynonyms(subSnpNoHgvs.getChromosome());

        boolean chromosomePresentInAssemblyReport = chromosomeSynonyms != null;
        boolean contigPresentInAssemblyReport = contigSynonyms != null;

        if (!contigPresentInAssemblyReport && !chromosomePresentInAssemblyReport) {
            throw new IllegalStateException(
                    "Neither contig '" + contigName + "' nor chromosome '"
                            + subSnpNoHgvs.getChromosome()
                            + "' were found in the assembly report! Is the assembly accession '"
                            + assemblyAccession + "' correct?");
        }

        if (chromosomePresentInAssemblyReport
                && contigPresentInAssemblyReport
                && !processedContigs.contains(contigName)
                && !contigSynonyms.equals(chromosomeSynonyms)) {
            logger.warn(
                    "Contig '" + contigName + "' and chromosome '" + subSnpNoHgvs.getChromosome()
                            + "' do not appear in the same line in the assembly report!");
            processedContigs.add(contigName);
        }

        if (isChromosomeReplaceable(subSnpNoHgvs, chromosomeSynonyms)) {
            replaceChromosomeWithGenbankAccession(subSnpNoHgvs, chromosomeSynonyms);
        } else if (isContigReplaceable(contigSynonyms)) {
            replaceContigWithGenbankAccession(subSnpNoHgvs, contigSynonyms);
        } else {
            // No replacement is possible. We must keep the original RefSeq accession
        }

        return subSnpNoHgvs;
    }

    private boolean isChromosomeReplaceable(SubSnpNoHgvs subSnpNoHgvs, ContigSynonyms chromosomeSynonyms) {
        StringBuilder reason = new StringBuilder();

        boolean replaceable;
        try {
            replaceable = ContigSynonymValidationProcessor.isChromosomeReplaceable(
                    subSnpNoHgvs.getChromosome(),
                    subSnpNoHgvs.getChromosomeStart() != null,
                    chromosomeSynonyms,
                    reason);
        } catch (NonIdenticalChromosomeAccessionsException e) {
            replaceable = true;
            if (!nonIdenticalChromosomes.contains(chromosomeSynonyms.getGenBank())) {
                nonIdenticalChromosomes.add(chromosomeSynonyms.getGenBank());
                logger.warn("Performing replacement even if the equivalence is dubious. This should have failed in the "
                            + VALIDATE_CONTIGS_STEP + ", but now we are in the " + IMPORT_DBSNP_VARIANTS_STEP
                            + ", which means the 'forceImport' flag was set. Details:" + e.getMessage());
            }
        }
        return replaceable;
    }

    private void replaceChromosomeWithGenbankAccession(SubSnpNoHgvs subSnpNoHgvs, ContigSynonyms chromosomeSynonyms) {
        subSnpNoHgvs.setContigName(chromosomeSynonyms.getGenBank());
        subSnpNoHgvs.setContigStart(subSnpNoHgvs.getChromosomeStart());
    }

    private boolean isContigReplaceable(ContigSynonyms contigSynonyms) {
        return ContigSynonymValidationProcessor.isContigReplaceable(contigSynonyms, new StringBuilder());
    }

    private void replaceContigWithGenbankAccession(SubSnpNoHgvs subSnpNoHgvs, ContigSynonyms contigSynonyms) {
        subSnpNoHgvs.setContigName(contigSynonyms.getGenBank());
    }
}

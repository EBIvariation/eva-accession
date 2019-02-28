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
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;

import java.util.HashSet;
import java.util.Set;

public class ContigReplacerProcessor implements ItemProcessor<SubSnpNoHgvs, SubSnpNoHgvs> {

    private static final Logger logger = LoggerFactory.getLogger(ContigReplacerProcessor.class);

    private ContigMapping contigMapping;

    private String assemblyAccession;

    private Set<String> processedContigs;

    public ContigReplacerProcessor(ContigMapping contigMapping, String assemblyAccession) {
        this.contigMapping = contigMapping;
        this.assemblyAccession = assemblyAccession;
        this.processedContigs = new HashSet<>();
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

        if (!processedContigs.contains(contigName)
                && chromosomePresentInAssemblyReport
                && contigPresentInAssemblyReport
                && !contigSynonyms.equals(chromosomeSynonyms)) {
            logger.warn(
                    "Contig '" + contigName + "' and chromosome '" + subSnpNoHgvs.getChromosome()
                            + "' do not appear in the same line in the assembly report!");
            processedContigs.add(contigName);
        }

        if (contigPresentInAssemblyReport) {
            replaceContigWithGenbankAccession(subSnpNoHgvs, contigSynonyms);
        } else {
            replaceChromosomeWithGenbankAccession(subSnpNoHgvs, chromosomeSynonyms);
        }

        return subSnpNoHgvs;
    }

    private void replaceContigWithGenbankAccession(SubSnpNoHgvs subSnpNoHgvs, ContigSynonyms contigSynonyms) {
        if (contigSynonyms.isIdenticalGenBankAndRefSeq() || isGenbank(assemblyAccession)) {
            subSnpNoHgvs.setContigName(contigSynonyms.getGenBank());
        } else {
            // genbank is not identical to refseq and the assembly is not genbank, so
            // we must keep the original refseq
        }
    }

    private void replaceChromosomeWithGenbankAccession(SubSnpNoHgvs subSnpNoHgvs, ContigSynonyms chromosomeSynonyms) {
        if (chromosomeSynonyms.isIdenticalGenBankAndRefSeq() || isGenbank(assemblyAccession)) {
            subSnpNoHgvs.setContigName(chromosomeSynonyms.getGenBank());
            subSnpNoHgvs.setContigStart(subSnpNoHgvs.getChromosomeStart());
        } else {
            // genbank is not identical to refseq and the assembly is not genbank, so
            // we must keep the original refseq, even if the refseq was not found in the assembly report
        }
    }

    private boolean isGenbank(String assemblyAccession) {
        return assemblyAccession.startsWith("GCA_");
    }


}

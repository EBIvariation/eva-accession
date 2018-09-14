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

import uk.ac.ebi.eva.accession.dbsnp.contig.ContigMapping;
import uk.ac.ebi.eva.accession.dbsnp.contig.ContigSynonyms;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;

public class ContigReplacerProcessor implements ItemProcessor<SubSnpNoHgvs, SubSnpNoHgvs> {

    private ContigMapping contigMapping;

    private String assemblyAccession;

    public ContigReplacerProcessor(ContigMapping contigMapping, String assemblyAccession) {
        this.contigMapping = contigMapping;
        this.assemblyAccession = assemblyAccession;
    }

    @Override
    public SubSnpNoHgvs process(SubSnpNoHgvs subSnpNoHgvs) throws Exception {
        ContigSynonyms contigSynonyms = contigMapping.getContigSynonyms(subSnpNoHgvs.getContigName());
        ContigSynonyms chromosomeSynonyms = contigMapping.getContigSynonyms(subSnpNoHgvs.getChromosome());

        if (isPresentInAssemblyReport(chromosomeSynonyms)
                && isPresentInAssemblyReport(contigSynonyms)
                && !contigSynonyms.equals(chromosomeSynonyms)) {
            throw new IllegalStateException(
                    "Contig '" + subSnpNoHgvs.getContigName() + "' and chromosome '" + subSnpNoHgvs.getChromosome()
                            + "' do not appear in the same line in the assembly report!");
        }

        if (isPresentInAssemblyReport(contigSynonyms)) {
            convertToGenbankIfIdentical(subSnpNoHgvs, contigSynonyms);
        } else {
            if (isPresentInAssemblyReport(chromosomeSynonyms)) {
                convertToGenbankIfIdentical(subSnpNoHgvs, chromosomeSynonyms);
            } else {
                throw new IllegalStateException(
                        "Neither contig '" + subSnpNoHgvs.getContigName() + "' nor chromosome '"
                                + subSnpNoHgvs.getChromosome()
                                + "' were found in the assembly report! Is the assembly accession '"
                                + assemblyAccession + "' correct?");
            }
        }

        return subSnpNoHgvs;
    }

    private boolean isPresentInAssemblyReport(ContigSynonyms synonyms) {
        return synonyms != null;
    }

    private void convertToGenbankIfIdentical(SubSnpNoHgvs subSnpNoHgvs, ContigSynonyms contigSynonyms) {
        if (contigSynonyms.isIdenticalGenBankAndRefSeq() || isGenbank(assemblyAccession)) {
            subSnpNoHgvs.setContigName(contigSynonyms.getGenBank());
        } else {
            // genbank is not identical to refseq and the assembly is not genbank, so
            // must keep the original refseq
        }
    }

    private boolean isGenbank(String assemblyAccession) {
        return assemblyAccession.startsWith("GCA_");
    }


}

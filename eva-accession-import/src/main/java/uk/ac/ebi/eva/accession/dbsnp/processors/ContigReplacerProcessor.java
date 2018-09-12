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

    public ContigReplacerProcessor(ContigMapping contigMapping) {
        this.contigMapping = contigMapping;
    }

    @Override
    public SubSnpNoHgvs process(SubSnpNoHgvs subSnpNoHgvs) throws Exception {
        ContigSynonyms contigSynonyms;
        if (subSnpNoHgvs.getChromosome() != null) {
           contigSynonyms = contigMapping.getContigSynonyms(subSnpNoHgvs.getChromosome());
        } else {
            contigSynonyms = contigMapping.getContigSynonyms(subSnpNoHgvs.getContigName());
        }

        if (contigSynonyms == null) {
            throw new IllegalArgumentException(
                    "Contig '" + subSnpNoHgvs.getContigName() + "' not found in the assembly report");
        }

        if (contigSynonyms.isIdenticalGenBankAndRefSeq()) {
            if (subSnpNoHgvs.getChromosome() != null) {
                subSnpNoHgvs.setChromosome(contigSynonyms.getSequenceName());
            }
            subSnpNoHgvs.setContigName(contigSynonyms.getSequenceName());
        }
        return subSnpNoHgvs;
    }
}

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

import static org.springframework.util.StringUtils.hasText;

public class ContigSynonymValidationProcessor implements ItemProcessor<String, String> {

    private ContigMapping contigMapping;

    public ContigSynonymValidationProcessor(ContigMapping contigMapping) {
        this.contigMapping = contigMapping;
    }

    @Override
    public String process(String contig) throws Exception {
        ContigSynonyms contigSynonyms = contigMapping.getContigSynonyms(contig);
        if (contigSynonyms == null
            || !contigSynonyms.isIdenticalGenBankAndRefSeq()
            || !hasText(contigSynonyms.getGenBank())) {
            throw new IllegalArgumentException("The contig " + contig + " has no equivalent INDSC accession");
        }
        return contig;
    }
}

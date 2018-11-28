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

import uk.ac.ebi.eva.accession.dbsnp.contig.ContigMapping;
import uk.ac.ebi.eva.accession.dbsnp.contig.ContigSynonyms;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;

public class ContigReplacerProcessor implements ItemProcessor<SubSnpNoHgvs, SubSnpNoHgvs> {

    private static final Logger logger = LoggerFactory.getLogger(ContigReplacerProcessor.class);

    private ContigMapping contigMapping;

    private String assemblyAccession;

    public ContigReplacerProcessor(ContigMapping contigMapping, String assemblyAccession) {
        this.contigMapping = contigMapping;
        this.assemblyAccession = assemblyAccession;
        if (!isRefseq(assemblyAccession)) {
            throw new IllegalArgumentException(
                    "The parameter for the assembly accession '" + assemblyAccession
                            + "' is a non-RefSeq assembly. The code in " + this.getClass().getSimpleName()
                            + " assumes dbSNP uses Refseq assembly accessions for every species.");
        }
    }

    private boolean isRefseq(String assemblyAccession) {
        return assemblyAccession.startsWith("GCF_");
    }

    @Override
    public SubSnpNoHgvs process(SubSnpNoHgvs subSnpNoHgvs) throws Exception {
        ContigSynonyms contigSynonyms = contigMapping.getContigSynonyms(subSnpNoHgvs.getContigName());

        if (isContigPresentInAssemblyReport(subSnpNoHgvs, contigSynonyms)) {
            if (contigSynonyms.isIdenticalGenBankAndRefSeq()) {
                subSnpNoHgvs.setContigName(contigSynonyms.getGenBank());
            }
        }

        return subSnpNoHgvs;
    }

    private boolean isContigPresentInAssemblyReport(SubSnpNoHgvs subSnpNoHgvs, ContigSynonyms contigSynonyms) {
        boolean contigPresentInAssemblyReport = contigSynonyms != null;
        if (!contigPresentInAssemblyReport) {
            logger.warn(
                    "CONTIG NOT FOUND: Contig '" + subSnpNoHgvs.getContigName() +
                            "' was not found in the assembly report! Is the " + "assembly accession '" +
                            assemblyAccession + "' correct?");
        }
        return contigPresentInAssemblyReport;
    }

}

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
package uk.ac.ebi.eva.accession.pipeline.steps.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.contig.ContigSynonyms;
import uk.ac.ebi.eva.commons.core.models.IVariant;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Converts the contig to it's GenBank synonym when possible. If the synonym can't be determined it keeps the contig as
 * is
 */
public class ContigReplacerProcessor implements ItemProcessor<IVariant, IVariant> {

    private static final Logger logger = LoggerFactory.getLogger(ContigReplacerProcessor.class);

    private ContigMapping contigMapping;

    private Set<String> processedContigs;

    public ContigReplacerProcessor(ContigMapping contigMapping) {
        this.contigMapping = contigMapping;
        this.processedContigs = new HashSet<>();
    }

    @Override
    public IVariant process(IVariant variant) throws Exception {
        String contigName = variant.getChromosome();
        ContigSynonyms contigSynonyms = contigMapping.getContigSynonyms(contigName);
        boolean contigPresentInAssemblyReport = contigSynonyms != null;

        if (!contigPresentInAssemblyReport) {
            if (!processedContigs.contains(contigName)) {
                logger.warn("Contig '" + variant.getChromosome() + "' was not found in the assembly report!");
                processedContigs.add(contigName);
            }
            return variant;
        } else {
            return replaceContigWithSynonym(variant, contigSynonyms);
        }
    }

    /**
     * The conversion will be performed only if the 'relationship' column of the assembly report is '='
     */
    private IVariant replaceContigWithSynonym(IVariant variant, ContigSynonyms contigSynonyms) {
        if (contigSynonyms.isIdenticalGenBankAndRefSeq()) {
            Variant newVariant = new Variant(getContigSynonym(contigSynonyms), variant.getStart(), variant.getEnd(),
                                             variant.getReference(), variant.getAlternate());
            Collection<VariantSourceEntry> sourceEntries = variant.getSourceEntries().stream()
                                                                  .map(VariantSourceEntry.class::cast)
                                                                  .collect(Collectors.toList());
            newVariant.addSourceEntries(sourceEntries);
            return newVariant;
        } else {
            if (!processedContigs.contains(variant.getChromosome())) {
                logger.warn("Genbank and refseq not identical in the assembly report for contig "
                                    + variant.getChromosome() + ". No conversion performed");
                processedContigs.add(variant.getChromosome());
            }
            return variant;
        }
    }

    private String getContigSynonym(ContigSynonyms contigSynonyms) {
        String contig;
        if (contigSynonyms.getGenBank() != null) {
            contig = contigSynonyms.getGenBank();
        } else {
            contig = contigSynonyms.getRefSeq();
        }
        return contig;
    }

}

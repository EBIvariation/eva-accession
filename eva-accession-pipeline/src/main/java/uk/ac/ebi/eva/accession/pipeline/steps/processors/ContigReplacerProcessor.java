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
 * Converts the contig to its GenBank synonym when possible. If the synonym can't be determined it keeps the contig as
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

        StringBuilder message = new StringBuilder();
        if (isReplacementPossible(variant, contigSynonyms, message)) {
            return replaceContigWithGenbankAccession(variant, contigSynonyms);
        } else {
            if (!processedContigs.contains(contigName)) {
                logger.warn(message.toString());
                processedContigs.add(contigName);
            }
            return variant;
        }
    }

    /**
     * Replacement only possible if:
     * - Contig has synonyms
     * - Contig has Genbank synonym
     * - Genbank and Refseq are identical
     */
    private boolean isReplacementPossible(IVariant variant, ContigSynonyms contigSynonyms, StringBuilder message) {
        if (contigSynonyms == null) {
            message.append("Contig '" + variant.getChromosome() + "' was not found in the assembly report!");
        } else if(contigSynonyms.getGenBank() == null) {
            message.append("No Genbank equivalent found for contig '" + variant.getChromosome()
                                   + "' in the assembly report");
        } else if(!contigSynonyms.isIdenticalGenBankAndRefSeq()) {
            message.append("Genbank and refseq not identical in the assembly report for contig '"
                                   + variant.getChromosome() + "'. No conversion performed");
        }
        return message.toString().isEmpty();
    }

    private IVariant replaceContigWithGenbankAccession(IVariant variant, ContigSynonyms contigSynonyms) {
        Variant newVariant = new Variant(contigSynonyms.getGenBank(), variant.getStart(), variant.getEnd(),
                                         variant.getReference(), variant.getAlternate());
        Collection<VariantSourceEntry> sourceEntries = variant.getSourceEntries().stream()
                                                              .map(VariantSourceEntry.class::cast)
                                                              .collect(Collectors.toList());
        newVariant.addSourceEntries(sourceEntries);
        return newVariant;
    }
}

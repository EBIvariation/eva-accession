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
package uk.ac.ebi.eva.accession.pipeline.batch.processors;

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
public class ContigToGenbankReplacerProcessor implements ItemProcessor<IVariant, IVariant> {

    private static final Logger logger = LoggerFactory.getLogger(ContigToGenbankReplacerProcessor.class);

    public static final String ORIGINAL_CHROMOSOME = "CHR";

    private ContigMapping contigMapping;

    private Set<String> processedContigs;

    public ContigToGenbankReplacerProcessor(ContigMapping contigMapping) {
        this.contigMapping = contigMapping;
        this.processedContigs = new HashSet<>();
    }

    @Override
    public IVariant process(IVariant variant) throws Exception {
        String contigName = variant.getChromosome();
        ContigSynonyms contigSynonyms = contigMapping.getContigSynonyms(contigName);

        StringBuilder message = new StringBuilder();
        if (contigMapping.isGenbankReplacementPossible(contigName, contigSynonyms, message)) {
            return replaceContigWithGenbankAccession(variant, contigSynonyms);
        } else {
            throw new IllegalArgumentException(message.toString() );
        }
    }

    private IVariant replaceContigWithGenbankAccession(IVariant variant, ContigSynonyms contigSynonyms) {
        Variant newVariant = new Variant(contigSynonyms.getGenBank(), variant.getStart(), variant.getEnd(),
                                         variant.getReference(), variant.getAlternate());
        Collection<VariantSourceEntry> sourceEntries = variant.getSourceEntries().stream()
                                                              .map(VariantSourceEntry.class::cast)
                                                              .peek(e -> e.addAttribute(ORIGINAL_CHROMOSOME,
                                                                                        variant.getChromosome()))
                                                              .collect(Collectors.toList());
        if (sourceEntries.isEmpty()) {
            throw new IllegalArgumentException("This class can only process variants with at least 1 source entry. "
                                               + "Otherwise, the original (replaced) chromosome is lost.");
        }

        newVariant.addSourceEntries(sourceEntries);
        return newVariant;
    }
}

/*
 * Copyright 2021 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.remapping.ingest.batch.processors;

import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.contig.ContigSynonyms;
import uk.ac.ebi.eva.commons.core.models.IVariant;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.util.Collection;
import java.util.stream.Collectors;

public class ContigToGenbankReplacerProcessor implements ItemProcessor<IVariant, IVariant> {

    public static final String ORIGINAL_CHROMOSOME = "CHR";

    private ContigMapping contigMapping;

    public ContigToGenbankReplacerProcessor(ContigMapping contigMapping) {
        this.contigMapping = contigMapping;
    }

    @Override
    public IVariant process(IVariant variant) throws Exception {
        String contigName = variant.getChromosome();
        ContigSynonyms contigSynonyms = contigMapping.getContigSynonyms(contigName);

        StringBuilder message = new StringBuilder();
        if (contigMapping.isGenbankReplacementPossible(contigName, contigSynonyms, message)) {
            return replaceContigWithGenbankAccession(variant, contigSynonyms);
        }
        throw new IllegalArgumentException(message.toString() );
    }

    private IVariant replaceContigWithGenbankAccession(IVariant variant, ContigSynonyms contigSynonyms) {
        Variant newVariant = new Variant(contigSynonyms.getGenBank(), variant.getStart(), variant.getEnd(),
                                         variant.getReference(), variant.getAlternate());
        newVariant.setMainId(variant.getMainId());
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

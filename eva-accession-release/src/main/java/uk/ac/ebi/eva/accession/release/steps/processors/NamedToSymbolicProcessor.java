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
package uk.ac.ebi.eva.accession.release.steps.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.commons.core.models.VariantType;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

public class NamedToSymbolicProcessor implements ItemProcessor<Variant, Variant> {

    private static final Logger logger = LoggerFactory.getLogger(NamedToSymbolicProcessor.class);

    @Override
    public Variant process(Variant variant) throws Exception {
        if (variant.getType() == VariantType.SEQUENCE_ALTERATION && variant.getAlternate().contains("(")) {
            String alternate = variant.getAlternate().replace("(", "<").replace(")", ">").replace(" ", "_");
            logger.warn("Alternate converted to {}", alternate);
            Variant newVariant = new Variant(variant.getChromosome(), variant.getStart(), variant.getEnd(),
                                             variant.getReference(), alternate);
            newVariant.addSourceEntries(variant.getSourceEntries());
            newVariant.setMainId(variant.getMainId());
            return newVariant;
        }
        return variant;
    }
}

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
package uk.ac.ebi.eva.remapping.source.batch.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;

/**
 * If a variant is multimap (mapWeight > 1) it should be excluded
 */
public class ExcludeMultimapVariantsProcessor implements ItemProcessor<SubmittedVariantEntity, SubmittedVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(ExcludeMultimapVariantsProcessor.class);

    @Override
    public SubmittedVariantEntity process(SubmittedVariantEntity submittedVariantEntity) throws Exception {
        if (!isMultimap(submittedVariantEntity)) {
            return submittedVariantEntity;
        }
        logger.warn("Skipped multimap variant {}", submittedVariantEntity);
        return null;
    }

    public boolean isMultimap(SubmittedVariantEntity submittedVariantEntity) {
        return submittedVariantEntity.getMapWeight() != null && submittedVariantEntity.getMapWeight() > 1;
    }
}

/*
 *
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
 *
 */
package uk.ac.ebi.eva.accession.release.steps.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.commons.core.models.IVariant;

public class ExcludeClusteredVariantsWithoutSubmittedVariantsProcessor implements ItemProcessor<IVariant, IVariant> {

    private static final Logger logger = LoggerFactory.getLogger(
            ExcludeClusteredVariantsWithoutSubmittedVariantsProcessor.class);

    @Override
    public IVariant process(IVariant variant) {
        boolean areSourceEntriesEmpty = (variant.getSourceEntries() == null || variant.getSourceEntries().isEmpty());
        if (areSourceEntriesEmpty) {
            logger.debug("Skipped variant " + variant.getMainId() + " because it has no submitted variants.");
            return null;
        }
        return variant;
    }
}

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
package uk.ac.ebi.eva.ingest.remapped.configuration.batch.processors;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.ingest.remapped.batch.processors.VariantToSubmittedVariantEntityRemappedProcessor;
import uk.ac.ebi.eva.ingest.remapped.parameters.InputParameters;

@Configuration
public class VariantToSubmittedVariantEntityRemappedProcessorConfiguration {

    @Bean("VARIANT_TO_SUBMITTED_VARIANT_ENTITY_REMAPPED_PROCESSOR")
    public VariantToSubmittedVariantEntityRemappedProcessor variantToSubmittedVariantEntityRemappedProcessor(
            InputParameters inputParameters) {
        return new VariantToSubmittedVariantEntityRemappedProcessor(inputParameters.getAssemblyAccession(),
                                                                    inputParameters.getRemappedFrom());
    }

}
